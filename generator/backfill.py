"""
Backfill generator: ~2 years of synthetic history for the fleet analytics project.

All data is simulated. Nothing here describes a real company, vehicle or person.

Usage
    python generator/backfill.py
    python generator/backfill.py --seed 7 --start 2024-10-01 --end 2026-09-30

Output: one folder per table, mirroring the stage layout @LANDING/<table>/
    data/backfill/clients/clients.csv
    data/backfill/vehicles/vehicles.csv
    data/backfill/lease_contracts/lease_contracts.csv
    data/backfill/dtc_codes/dtc_codes.csv
    data/backfill/resale_values/resale_values.csv
    data/backfill/maintenance_events/maintenance_events.csv
    data/backfill/fuel_transactions/fuel_transactions_YYYY_MM.csv
    data/backfill/telematics_events/telematics_events_YYYY_MM.json.gz
    data/backfill/_dq_manifest.csv   ground truth of every injected data quality issue
    data/backfill/_sim_state.json    end state of active vehicles, picked up by stream.py

Files starting with "_" are for us, not for Snowflake: they are never uploaded.

The same seed always produces byte-identical files, so re-running is safe.

How the simulation works
    Each vehicle is simulated day by day. On a working day it drives, burns fuel,
    refuels when low, and its telematics device pings once an hour. Wear builds up
    with distance and engine hours; faults (diagnostic trouble codes) appear more
    often on old, high-distance or overdue vehicles and raise fuel use until
    repaired. Services happen when due, adjusted by each client's discipline.
    Owned vehicles are sold when they hit the client's (hidden) replacement policy;
    leased vehicles are returned at lease end. Either way a replacement arrives.

    Hidden per-client policies deliberately differ (one replaces too early, one
    too late) so the Gold backtest has something real to find.

Conventions (CLAUDE.md): distance km, time hours, money CAD, timestamps UTC,
column names carry their unit, classifications use the fixed lists below.
"""

from __future__ import annotations

import argparse
import calendar
import csv
import gzip
import io
import json
import math
import random
import shutil
import uuid
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Optional
from zoneinfo import ZoneInfo

from faker import Faker

UTC = timezone.utc
ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUT = ROOT / "data" / "backfill"
DEFAULT_SEED = 42
DEFAULT_START = date(2024, 10, 1)
DEFAULT_END = date(2026, 9, 30)  # exclusive; the live stream takes over from here

# ---------------------------------------------------------------------------
# Canonical classification values. Silver maps every variant onto these.
# ---------------------------------------------------------------------------
INDUSTRIES = ("CONSTRUCTION", "HVAC", "ELECTRICAL")
VEHICLE_TYPES = ("PICKUP", "VAN", "BOX_TRUCK")
FUEL_TYPES = ("GASOLINE", "DIESEL")
OWNERSHIP = ("OWNED", "LEASED")
STATUSES = ("ACTIVE", "RETIRED")
DISPOSAL_REASONS = ("SOLD", "LEASE_RETURN")
SERVICE_TYPES = ("PREVENTIVE_SERVICE", "TIRE_SERVICE", "BRAKE_SERVICE", "UNPLANNED_REPAIR")
SEVERITIES = ("CRITICAL", "MEDIUM", "LOW")

# Monthly activity multiplier, January first.
SEASONALITY = {
    "CONSTRUCTION": (0.45, 0.50, 0.70, 0.90, 1.10, 1.20, 1.20, 1.20, 1.10, 1.00, 0.80, 0.55),
    "HVAC":         (1.15, 1.10, 0.90, 0.85, 0.95, 1.20, 1.30, 1.25, 0.95, 0.90, 1.00, 1.15),
    "ELECTRICAL":   (0.90, 0.90, 0.95, 1.00, 1.05, 1.05, 1.05, 1.05, 1.00, 1.00, 0.95, 0.90),
}
SEASONAL_PATTERN = {"CONSTRUCTION": "SUMMER_PEAK", "HVAC": "SUMMER_WINTER_PEAK", "ELECTRICAL": "FLAT"}


@dataclass(frozen=True)
class ModelSpec:
    make: str
    model: str
    vehicle_type: str
    fuel_type: str
    msrp_2020_cad: float
    l_per_100km: float
    tank_litres: float
    depreciation_rate: float  # annual exponential decay of market value
    service_km: int
    service_hours: int
    wmi: str  # first three VIN characters


CATALOG = (
    ModelSpec("Ford", "F-150", "PICKUP", "GASOLINE", 52_000, 13.5, 98, 0.14, 12_000, 400, "1FT"),
    ModelSpec("Chevrolet", "Silverado 1500", "PICKUP", "GASOLINE", 50_000, 14.0, 91, 0.15, 12_000, 400, "1GC"),
    ModelSpec("Ram", "1500", "PICKUP", "GASOLINE", 51_000, 13.8, 98, 0.15, 12_000, 400, "3C6"),
    ModelSpec("Ford", "Transit 250", "VAN", "GASOLINE", 55_000, 15.5, 95, 0.16, 12_000, 400, "1FT"),
    ModelSpec("Ram", "ProMaster 2500", "VAN", "GASOLINE", 52_000, 15.0, 91, 0.17, 12_000, 400, "3C6"),
    ModelSpec("Mercedes-Benz", "Sprinter 2500", "VAN", "DIESEL", 68_000, 12.5, 94, 0.15, 16_000, 500, "W1Y"),
    ModelSpec("Isuzu", "NPR-HD", "BOX_TRUCK", "GASOLINE", 85_000, 22.0, 150, 0.13, 12_000, 400, "JAL"),
    ModelSpec("Ford", "F-550", "BOX_TRUCK", "DIESEL", 95_000, 24.0, 151, 0.12, 16_000, 500, "1FD"),
)
MODELS_BY_TYPE = {t: [m for m in CATALOG if m.vehicle_type == t] for t in VEHICLE_TYPES}

FLEET_MIX = {
    "CONSTRUCTION": {"PICKUP": 0.60, "BOX_TRUCK": 0.25, "VAN": 0.15},
    "HVAC": {"VAN": 0.70, "PICKUP": 0.30},
    "ELECTRICAL": {"VAN": 0.60, "PICKUP": 0.35, "BOX_TRUCK": 0.05},
}
ANNUAL_KM = {"PICKUP": 32_000, "VAN": 36_000, "BOX_TRUCK": 42_000}
IDLE_RATIO = {"CONSTRUCTION": 0.35, "HVAC": 0.30, "ELECTRICAL": 0.28}
IDLE_BURN_L_PER_H = {"PICKUP": 2.0, "VAN": 2.0, "BOX_TRUCK": 3.0}
TIRE_INTERVAL_KM = {"PICKUP": 45_000, "VAN": 45_000, "BOX_TRUCK": 40_000}
BRAKE_INTERVAL_KM = {"PICKUP": 70_000, "VAN": 65_000, "BOX_TRUCK": 50_000}
DIESEL_PREMIUM_CAD = 0.18
LEASE_TERMS = {36: 0.30, 48: 0.45, 60: 0.25}  # months: probability

# code, description, severity, extra fuel use while active, relative frequency
DTC_CODES = (
    ("P0217", "Engine coolant over-temperature condition", "CRITICAL", 0.00, 1),
    ("P0300", "Random or multiple cylinder misfire detected", "CRITICAL", 0.12, 2),
    ("P0524", "Engine oil pressure too low", "CRITICAL", 0.00, 1),
    ("P0171", "System too lean, bank 1", "MEDIUM", 0.08, 4),
    ("P0101", "Mass air flow sensor range/performance", "MEDIUM", 0.06, 3),
    ("P0562", "System voltage low", "MEDIUM", 0.00, 3),
    ("C0035", "Left front wheel speed sensor circuit", "MEDIUM", 0.00, 2),
    ("P0420", "Catalyst system efficiency below threshold, bank 1", "LOW", 0.03, 5),
    ("P0128", "Coolant temperature below thermostat regulating temperature", "LOW", 0.05, 4),
    ("P0442", "Evaporative emission system small leak detected", "LOW", 0.00, 5),
)
BREAKDOWN_REPAIRS = (
    "Alternator replacement", "Starter motor replacement", "Coolant leak repair",
    "Suspension repair", "Electrical fault diagnosis and repair", "Transmission service",
    "Battery replacement", "Exhaust repair",
)

# (min_km, max_km or None, label, value factor)
DISTANCE_BANDS = (
    (0, 50_000, "000-050K", 1.00),
    (50_000, 100_000, "050-100K", 0.92),
    (100_000, 150_000, "100-150K", 0.84),
    (150_000, 200_000, "150-200K", 0.76),
    (200_000, 300_000, "200-300K", 0.66),
    (300_000, None, "300K+", 0.55),
)

FUEL_BRANDS = ("Northline Fuels", "Maple Petroleum", "Prairie Gas Bar", "Coastal Energy", "TrueNorth Fuel")

# Dirty variants the source systems send. Silver must map them back.
VARIANTS = {
    "vehicle_type": {
        "PICKUP": ("Pickup", "pickup", "PICK-UP", "Pick Up"),
        "VAN": ("Van", "van ", "Cargo Van"),
        "BOX_TRUCK": ("Box Truck", "box truck", "BOX-TRUCK", "Cube Van"),
    },
    "fuel_type": {
        "GASOLINE": ("Gas", "gasoline", "Petrol", "GAS"),
        "DIESEL": ("diesel", "Diesel ", "DSL"),
    },
    "ownership": {
        "OWNED": ("Owned", "owned", "OWN"),
        "LEASED": ("Leased", "lease", "LEASE"),
    },
    "service_type": {
        "PREVENTIVE_SERVICE": ("Preventive Service", "preventive service", "Oil Change", "PM Service"),
        "TIRE_SERVICE": ("Tire Service", "tires", "Tyre Service"),
        "BRAKE_SERVICE": ("Brake Service", "brakes", "BRAKE SERVICE"),
        "UNPLANNED_REPAIR": ("Repair", "unplanned repair", "Breakdown Repair"),
    },
}
INVALID_SERVICE_TYPES = ("MISC", "", "N/A", "TBD")

# Injection rates for the deliberate data quality issues.
DQ = {
    "ping_drop": 0.015,          # single missing telematics pings
    "outage_per_day": 0.01,      # device offline for 3-6 hours
    "odometer_glitch": 0.001,    # odometer reading lower than the previous one
    "fuel_exact_dup": 0.015,     # same transaction sent twice, same id
    "fuel_resent": 0.005,        # same transaction re-sent with a new id
    "fuel_orphans": 12,          # fuel rows for a vehicle that does not exist
    "vehicle_variant": 0.10,     # vehicles with a non-canonical classification
    "service_variant": 0.05,     # maintenance rows with a non-canonical service type
    "service_invalid": 4,        # maintenance rows with an unmappable service type
    "maintenance_orphans": 3,
}


@dataclass
class ClientSpec:
    client_id: str
    industry: str
    city: str
    province: str
    tz: str
    initial_fleet: int
    lease_share: float
    # Hidden behaviour: never written to the output files.
    replace_age_years: float
    replace_km: float
    service_compliance: float  # 1.0 = on time, 1.3 = services 30% late
    client_since: date
    company_name: str = ""
    stations: tuple = ()
    vendors: dict = field(default_factory=dict)


def build_clients() -> list:
    return [
        #          id      industry        city           prov  tz                   fleet lease age   km       compliance since
        ClientSpec("C001", "CONSTRUCTION", "Calgary",     "AB", "America/Edmonton",  40, 0.40, 5.0, 200_000, 1.05, date(2016, 3, 1)),
        ClientSpec("C002", "HVAC",         "Mississauga", "ON", "America/Toronto",   30, 0.60, 3.5, 150_000, 0.95, date(2018, 6, 1)),
        ClientSpec("C003", "ELECTRICAL",   "Vancouver",   "BC", "America/Vancouver", 35, 0.20, 10.0, 350_000, 1.30, date(2014, 9, 1)),
        ClientSpec("C004", "CONSTRUCTION", "Edmonton",    "AB", "America/Edmonton",  25, 0.30, 7.0, 260_000, 1.10, date(2017, 4, 1)),
        ClientSpec("C005", "HVAC",         "Ottawa",      "ON", "America/Toronto",   20, 0.50, 6.0, 230_000, 1.00, date(2019, 1, 1)),
    ]


@dataclass
class Vehicle:
    num: int
    client: ClientSpec
    spec: ModelSpec
    model_year: int
    vin: str
    ownership: str
    acquisition_date: date
    acquisition_cost_cad: float
    # Hidden behaviour
    annual_km: float
    idle_ratio: float
    econ_factor: float
    retire_jitter: float
    refuel_threshold: float
    firmware: str
    pre_history: bool = False
    odometer_km: float = 0.0  # at history start (pre_history) or 0 for new
    lease: Optional[dict] = None
    status: str = "ACTIVE"
    disposal_date: Optional[date] = None
    disposal_reason: Optional[str] = None
    disposal_price_cad: Optional[float] = None

    @property
    def vehicle_id(self) -> str:
        return f"V{self.num:04d}"


@dataclass
class State:
    odo: float
    eh: float
    fuel: float
    km_svc: float = 0.0
    h_svc: float = 0.0
    km_tires: float = 0.0
    km_brakes: float = 0.0
    next_svc_factor: float = 1.0
    fault: Optional[tuple] = None
    fault_since: Optional[date] = None
    fault_delay: int = 0
    downtime_days: int = 0


# ---------------------------------------------------------------------------
# Small helpers
# ---------------------------------------------------------------------------
VIN_CHARS = "ABCDEFGHJKLMNPRSTUVWXYZ0123456789"
VIN_LETTERS = "ABCDEFGHJKLMNPRSTUVWXYZ"
VIN_YEAR_CODES = "ABCDEFGHJKLMNPRSTVWXY"  # 2010 = A ... 2030 = Y
VIN_WEIGHTS = (8, 7, 6, 5, 4, 3, 2, 10, 0, 9, 8, 7, 6, 5, 4, 3, 2)
VIN_TRANSLIT = {
    **{str(d): d for d in range(10)},
    "A": 1, "B": 2, "C": 3, "D": 4, "E": 5, "F": 6, "G": 7, "H": 8,
    "J": 1, "K": 2, "L": 3, "M": 4, "N": 5, "P": 7, "R": 9,
    "S": 2, "T": 3, "U": 4, "V": 5, "W": 6, "X": 7, "Y": 8, "Z": 9,
}


def make_vin(r: random.Random, wmi: str, model_year: int) -> str:
    """17-character VIN with a valid North American check digit."""
    body = (
        wmi
        + "".join(r.choice(VIN_CHARS) for _ in range(5))
        + "0"
        + VIN_YEAR_CODES[model_year - 2010]
        + r.choice(VIN_LETTERS)
        + f"{r.randrange(10**6):06d}"
    )
    check = sum(VIN_TRANSLIT[c] * w for c, w in zip(body, VIN_WEIGHTS)) % 11
    return body[:8] + ("X" if check == 10 else str(check)) + body[9:]


def add_months(d: date, months: int) -> date:
    y, m = divmod(d.month - 1 + months, 12)
    y, m = d.year + y, m + 1
    return date(y, m, min(d.day, calendar.monthrange(y, m)[1]))


def month_starts(start: date, end: date) -> list:
    out, d = [], date(start.year, start.month, 1)
    while d < end:
        out.append(d)
        d = add_months(d, 1)
    return out


def daterange(a: date, b: date):
    for n in range((b - a).days):
        yield a + timedelta(days=n)


def iso(ts: datetime) -> str:
    return ts.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def new_uuid(r: random.Random) -> str:
    return str(uuid.UUID(int=r.getrandbits(128), version=4))


def pick(r: random.Random, weights: dict):
    keys = list(weights)
    return r.choices(keys, weights=[weights[k] for k in keys])[0]


def model_year_for(acquired: date) -> int:
    return acquired.year + (1 if acquired.month >= 9 else 0)


def msrp(spec: ModelSpec, model_year: int) -> float:
    return spec.msrp_2020_cad * 1.035 ** (model_year - 2020)


def band_for(odometer_km: float) -> tuple:
    for band in DISTANCE_BANDS:
        if band[1] is None or odometer_km < band[1]:
            return band
    return DISTANCE_BANDS[-1]


# ---------------------------------------------------------------------------
# Generator
# ---------------------------------------------------------------------------
class Backfill:
    def __init__(self, seed: int, start: date, end: date, out: Path):
        self.seed, self.start, self.end, self.out = seed, start, end, out
        self.fake = Faker("en_CA")
        self.fake.seed_instance(seed)
        self.clients = build_clients()
        self.vehicles: list = []
        self.next_num = 1
        self.next_contract = 1
        self.fuel_rows: list = []
        self.maint_rows: list = []
        self.manifest: list = []
        self.states: list = []
        self.tele_handles: dict = {}
        self.tele_counts = Counter()
        self.months = month_starts(start, end)
        self._build_markets()
        self._name_clients()

    # --- reference data ------------------------------------------------------
    def _build_markets(self):
        """Monthly used-vehicle market index and regular gasoline price (CAD/L)."""
        r = random.Random(f"{self.seed}-market")
        self.market_index, self.gas_price = {}, {}
        level, gas = 1.0, 1.52
        for m in self.months:
            level *= 1 - 0.004 + r.gauss(0, 0.008)  # slow decline in used prices
            spring = 1.02 if m.month in (3, 4, 5) else 1.0
            self.market_index[(m.year, m.month)] = level * spring
            gas = 1.52 + 0.7 * (gas - 1.52) + r.gauss(0, 0.04)  # mean-reverting
            seasonal = 0.08 * math.sin(2 * math.pi * (m.month - 4) / 12)
            self.gas_price[(m.year, m.month)] = round(gas + seasonal, 3)

    def _name_clients(self):
        suffixes = {
            "CONSTRUCTION": ("Construction Ltd.", "Builders Inc."),
            "HVAC": ("Heating & Cooling Inc.", "Mechanical Ltd."),
            "ELECTRICAL": ("Electric Ltd.", "Electrical Contractors Inc."),
        }
        used = set()
        for i, c in enumerate(self.clients):
            while True:
                name = f"{self.fake.last_name()} {suffixes[c.industry][i % 2]}"
                if name not in used:
                    used.add(name)
                    break
            c.company_name = name
            c.stations = tuple(
                f"{FUEL_BRANDS[j % len(FUEL_BRANDS)]} - {self.fake.street_name()}" for j in range(6)
            )
            c.vendors = {
                "PREVENTIVE_SERVICE": (f"{c.city} Quick Lube", f"{self.fake.last_name()} Auto Service"),
                "TIRE_SERVICE": (f"{self.fake.last_name()} Tire & Wheel",),
                "BRAKE_SERVICE": (f"{self.fake.last_name()} Brake & Muffler",),
                "UNPLANNED_REPAIR": (f"{c.city} Truck Centre", f"{self.fake.last_name()} Auto Service"),
            }

    def market_value(self, spec: ModelSpec, model_year: int, band: tuple, on: date) -> float:
        age = max(0.0, (on - date(model_year - 1, 9, 1)).days / 365.25)
        idx = self.market_index[(on.year, on.month)]
        return msrp(spec, model_year) * 0.88 * math.exp(-spec.depreciation_rate * age) * band[3] * idx

    # --- fleet ---------------------------------------------------------------
    def new_vehicle(self, r: random.Random, client: ClientSpec, spec: ModelSpec, acquired: date,
                    ownership: str, pre_history_age_years: float = 0.0,
                    term: Optional[int] = None) -> Vehicle:
        model_year = model_year_for(acquired)
        v = Vehicle(
            num=self.next_num,
            client=client,
            spec=spec,
            model_year=model_year,
            vin=make_vin(r, spec.wmi, model_year),
            ownership=ownership,
            acquisition_date=acquired,
            acquisition_cost_cad=round(msrp(spec, model_year) * r.uniform(0.93, 1.02), 2),
            annual_km=ANNUAL_KM[spec.vehicle_type] * r.lognormvariate(0, 0.22),
            idle_ratio=min(0.55, max(0.10, r.gauss(IDLE_RATIO[client.industry], 0.06))),
            econ_factor=r.uniform(0.93, 1.08),
            retire_jitter=r.uniform(0.90, 1.15),
            refuel_threshold=r.uniform(0.18, 0.35),
            firmware=r.choice(("3.8.2", "4.0.1", "4.1.0")),
            pre_history=pre_history_age_years > 0,
        )
        self.next_num += 1
        if pre_history_age_years > 0:
            v.odometer_km = v.annual_km * pre_history_age_years * r.uniform(0.9, 1.1)
        if ownership == "LEASED":
            v.lease = self.new_lease(r, v, term or pick(r, LEASE_TERMS))
        self.vehicles.append(v)
        return v

    def new_lease(self, r: random.Random, v: Vehicle, term: int) -> dict:
        # Sized to the vehicle type's typical use; high-use vehicles will still run over.
        headroom = pick(r, {1.0: 0.30, 1.15: 0.45, 1.3: 0.25})
        annual_allowance = round(ANNUAL_KM[v.spec.vehicle_type] * headroom / 5_000) * 5_000
        residual_pct = {36: 0.55, 48: 0.45, 60: 0.35}[term]
        cost = v.acquisition_cost_cad
        residual = cost * residual_pct
        payment = (cost - residual) / term + (cost + residual) * 0.0028  # money factor ~6.7% APR
        lease = {
            "contract_id": f"L{self.next_contract:04d}",
            "vehicle_id": v.vehicle_id,
            "start_date": v.acquisition_date,
            "end_date": add_months(v.acquisition_date, term),
            "term_months": term,
            "monthly_payment_cad": round(payment, 2),
            "distance_allowance_km": annual_allowance * term // 12,
            "excess_km_rate_cad": 0.20 if v.spec.vehicle_type != "BOX_TRUCK" else 0.28,
            "residual_value_cad": round(residual, 2),
        }
        self.next_contract += 1
        return lease

    def build_initial_fleet(self):
        r = random.Random(f"{self.seed}-fleet")
        for c in self.clients:
            for _ in range(c.initial_fleet):
                vtype = pick(r, FLEET_MIX[c.industry])
                spec = r.choice(MODELS_BY_TYPE[vtype])
                ownership = "LEASED" if r.random() < c.lease_share else "OWNED"
                term = None
                if ownership == "LEASED":
                    # Somewhere inside the lease, at least a month from either end.
                    term = pick(r, LEASE_TERMS)
                    age_years = r.uniform(1 / 12, term / 12 - 1 / 12)
                else:
                    km_limit_years = c.replace_km / ANNUAL_KM[vtype]
                    age_years = r.uniform(0.2, min(c.replace_age_years, km_limit_years) * 0.87)
                acquired = self.start - timedelta(days=int(age_years * 365.25))
                self.new_vehicle(r, c, spec, acquired, ownership, pre_history_age_years=age_years, term=term)

    # --- simulation ----------------------------------------------------------
    def hazard(self, v: Vehicle, s: State, day: date, base: float) -> float:
        age = max(0.0, (day - v.acquisition_date).days / 365.25)
        overdue = max(0.0, s.km_svc / v.spec.service_km - 1)
        return base * (1 + 0.15 * age) * (1 + 3 * overdue) * (1 + 0.5 * s.odo / 200_000)

    def should_retire(self, v: Vehicle, s: State, day: date) -> bool:
        if v.ownership == "LEASED":
            return day >= v.lease["end_date"]
        c = v.client
        age = (day - v.acquisition_date).days / 365.25
        return age >= c.replace_age_years * v.retire_jitter or s.odo >= c.replace_km * v.retire_jitter

    def retire(self, r: random.Random, v: Vehicle, s: State, day: date):
        v.status = "RETIRED"
        v.disposal_date = day
        v.disposal_reason = "LEASE_RETURN" if v.ownership == "LEASED" else "SOLD"
        value = self.market_value(v.spec, v.model_year, band_for(s.odo), day)
        v.disposal_price_cad = round(value * r.uniform(0.90, 1.05), 2)
        # Replacement: usually the same model, arriving within a week.
        arrival = day + timedelta(days=r.randint(0, 7))
        if arrival < self.end:
            spec = v.spec if r.random() < 0.7 else r.choice(MODELS_BY_TYPE[v.spec.vehicle_type])
            ownership = "LEASED" if r.random() < v.client.lease_share else "OWNED"
            self.new_vehicle(r, v.client, spec, arrival, ownership)

    def simulate(self, v: Vehicle):
        r = random.Random(f"{self.seed}-veh-{v.num}")    # vehicle behaviour
        q = random.Random(f"{self.seed}-dq-{v.num}")     # data quality injection
        c, spec = v.client, v.spec
        tz = ZoneInfo(c.tz)
        tank = spec.tank_litres
        eh_per_km = 1 / 40 / (1 - v.idle_ratio)

        s = State(odo=v.odometer_km, eh=v.odometer_km * eh_per_km, fuel=tank * 0.95)
        s.next_svc_factor = c.service_compliance * r.uniform(0.92, 1.10)
        if v.pre_history:
            s.km_svc = min(s.odo, r.uniform(0, spec.service_km * s.next_svc_factor * 0.95))
            s.h_svc = s.km_svc * eh_per_km
            s.km_tires = min(s.odo, r.uniform(0, TIRE_INTERVAL_KM[spec.vehicle_type] * 0.95))
            s.km_brakes = min(s.odo, r.uniform(0, BRAKE_INTERVAL_KM[spec.vehicle_type] * 0.95))
            s.fuel = tank * r.uniform(0.35, 0.95)
            self._prior_service(r, v, s, tz)

        first_day = max(self.start, v.acquisition_date)
        for day in daterange(first_day, self.end):
            if self.should_retire(v, s, day):
                self.retire(r, v, s, day)
                break
            if s.downtime_days > 0:
                s.downtime_days -= 1
                continue
            mult = SEASONALITY[c.industry][day.month - 1]
            weekday = day.weekday()
            if weekday < 5:
                p_work = min(0.97, 0.88 * mult)
            elif weekday == 5 and c.industry == "CONSTRUCTION" and mult > 1.0:
                p_work = 0.25
            else:
                p_work = 0.0
            if r.random() >= p_work:
                continue
            shift_end = self._drive_day(r, q, v, s, day, tz, mult)
            self._maintenance(r, v, s, day, shift_end)

        if v.status == "ACTIVE":
            self.states.append({
                "vehicle_id": v.vehicle_id,
                "client_id": c.client_id,
                "device_id": f"TLM-{v.num:05d}",
                "firmware_version": v.firmware,
                "time_zone": c.tz,
                "industry": c.industry,
                "vehicle_type": spec.vehicle_type,
                "fuel_type": spec.fuel_type,
                "tank_litres": tank,
                "odometer_km": round(s.odo, 1),
                "engine_hours": round(s.eh, 1),
                "fuel_litres": round(s.fuel, 1),
                "km_since_service": round(s.km_svc, 1),
                "active_dtc": s.fault[0] if s.fault else None,
                "annual_km": round(v.annual_km),
                "idle_ratio": round(v.idle_ratio, 3),
                "l_per_100km": round(spec.l_per_100km * v.econ_factor, 2),
            })

    def _prior_service(self, r, v, s, tz):
        """Last service before history starts, so every vehicle has a baseline."""
        if s.odo - s.km_svc < 1_000:
            return
        days_ago = int(s.km_svc / (v.annual_km / 365)) + 1
        day = self.start - timedelta(days=days_ago)
        if day <= v.acquisition_date:
            return
        ts = datetime.combine(day, time(16, 30), tzinfo=tz)
        self._add_maintenance(r, v, "PREVENTIVE_SERVICE", ts, s.odo - s.km_svc, s.eh - s.h_svc,
                              day, None, "Oil and filter change, multi-point inspection")

    def _drive_day(self, r, q, v, s, day, tz, mult) -> datetime:
        c, spec = v.client, v.spec
        tank = spec.tank_litres
        n = r.randint(8, 10)  # hourly intervals in this shift
        weights = [r.gammavariate(2, 1) for _ in range(n)]
        total_w = sum(weights)
        day_km = v.annual_km / 230 * math.sqrt(mult) * r.lognormvariate(0, 0.25)
        t0 = (datetime.combine(day, time(7, 0), tzinfo=tz) + timedelta(minutes=r.randrange(60))).astimezone(UTC)

        fault_idx = 0
        if s.fault is None and r.random() < self.hazard(v, s, day, 0.0012):
            code = r.choices(DTC_CODES, weights=[d[4] for d in DTC_CODES])[0]
            s.fault, s.fault_since = code, day
            s.fault_delay = {"CRITICAL": r.randint(0, 2), "MEDIUM": r.randint(3, 21), "LOW": 10_000}[code[2]]
            fault_idx = r.randint(0, n)

        age_years = max(0.0, (day - date(v.model_year - 1, 9, 1)).days / 365.25)
        winter = 1.12 if day.month in (12, 1, 2) else 1.05 if day.month in (3, 11) else 1.0
        base_econ = spec.l_per_100km * v.econ_factor * winter * (1 + 0.006 * age_years)

        outage = range(0)
        if q.random() < DQ["outage_per_day"]:
            o_start = q.randint(0, n)
            outage = range(o_start, o_start + q.randint(3, 6))

        readings = []

        def ping(i: int, ts: datetime, speed: float, idle_h: float):
            event_id = new_uuid(r)  # drawn from r so clean data never depends on DQ draws
            dtc = [s.fault[0]] if s.fault and i >= fault_idx else []
            if i in outage or q.random() < DQ["ping_drop"]:
                self.manifest.append(("MISSING_PING", "telematics_events", event_id, v.vehicle_id,
                                      f"expected ping at {iso(ts)}" + (" (device outage)" if i in outage else "")))
                return
            odo = s.odo
            if q.random() < DQ["odometer_glitch"]:
                odo = odo / 10 if q.random() < 0.5 else odo - q.uniform(50, 3_000)
                self.manifest.append(("ODOMETER_BACKWARD", "telematics_events", event_id, v.vehicle_id,
                                      f"reported {odo:.1f} km, true {s.odo:.1f} km"))
            readings.append({
                "event_id": event_id,
                "event_ts": iso(ts),
                "odometer_km": round(max(odo, 0.0), 1),
                "engine_hours": round(s.eh, 2),
                "fuel_level_pct": round(100 * s.fuel / tank, 1),
                "speed_kph": round(speed, 1),
                "idle_hours": round(idle_h, 3),
                "dtc_codes": dtc,
            })

        ping(0, t0, 0.0, 0.0)  # ignition on
        for i in range(n):
            km = day_km * weights[i] / total_w
            avg_speed = r.uniform(32, 58)
            moving_h = km / avg_speed
            if moving_h > 0.92:
                moving_h, km = 0.92, 0.92 * avg_speed
            idle_h = min(1 - moving_h, moving_h * v.idle_ratio / (1 - v.idle_ratio) * r.uniform(0.6, 1.4))
            wear = 1 + 0.06 * min(1.5, s.km_svc / spec.service_km)
            fault_pen = 1 + s.fault[3] if s.fault and i >= fault_idx else 1.0
            litres = km * base_econ * wear * fault_pen / 100 + idle_h * IDLE_BURN_L_PER_H[spec.vehicle_type]

            s.odo += km
            s.eh += moving_h + idle_h
            s.km_svc += km
            s.h_svc += moving_h + idle_h
            s.km_tires += km
            s.km_brakes += km
            s.fuel = max(tank * 0.03, s.fuel - litres)
            if s.fuel < tank * v.refuel_threshold:
                self._refuel(r, v, s, t0 + timedelta(hours=i, minutes=r.randint(10, 50)), day)

            ts = t0 + timedelta(hours=i + 1, seconds=r.randint(-40, 40))
            speed = 0.0 if r.random() < 0.3 else max(0.0, r.gauss(avg_speed, 12))
            ping(i + 1, ts, speed, idle_h)

        if readings:
            record = {
                "device_id": f"TLM-{v.num:05d}",
                "vehicle_id": v.vehicle_id,
                "firmware_version": v.firmware,
                "upload_ts": iso(t0 + timedelta(hours=n, minutes=r.randint(2, 15))),
                "readings": readings,
            }
            self._write_telematics((t0.year, t0.month), record)
            self.tele_counts["readings"] += len(readings)
            self.tele_counts["records"] += 1
        return t0 + timedelta(hours=n, minutes=30)

    def _refuel(self, r, v, s, ts, day):
        c, spec = v.client, v.spec
        litres = spec.tank_litres * r.uniform(0.90, 1.0) - s.fuel
        if litres < 15:
            return
        price = self.gas_price[(day.year, day.month)]
        if spec.fuel_type == "DIESEL":
            price += DIESEL_PREMIUM_CAD
        price = round(price * r.uniform(0.97, 1.04), 3)
        station = r.choices(c.stations, weights=(5, 4, 3, 2, 1, 1))[0]
        self.fuel_rows.append({
            "transaction_id": new_uuid(r),
            "vehicle_id": v.vehicle_id,
            "fuel_card_id": f"FC-{c.client_id}-{v.num:04d}",
            "transaction_ts": iso(ts),
            "station_name": station,
            "station_city": c.city,
            "station_province": c.province,
            "fuel_type": spec.fuel_type,
            "litres": round(litres, 2),
            "price_per_litre_cad": price,
            "amount_cad": round(litres * price, 2),
        })
        s.fuel += litres

    def _maintenance(self, r, v, s, day, shift_end):
        spec, vtype = v.spec, v.spec.vehicle_type
        ts = shift_end
        downtime = 0.0

        # Faults that can't wait get fixed on their own visit.
        if s.fault and s.fault[2] in ("CRITICAL", "MEDIUM") and (day - s.fault_since).days >= s.fault_delay:
            downtime += self._repair(r, v, s, ts, day, s.fault)
            s.fault = None

        # Scheduled service: km or engine hours, whichever comes first.
        due = (s.km_svc >= spec.service_km * s.next_svc_factor
               or s.h_svc >= spec.service_hours * s.next_svc_factor)
        if due:
            resolved = None
            if s.fault and s.fault[2] == "LOW":
                resolved, s.fault = s.fault[0], None
            elif s.fault:
                downtime += self._repair(r, v, s, ts, day, s.fault)
                s.fault = None
            age = (day - v.acquisition_date).days / 365.25
            lo, hi = (350, 600) if vtype == "BOX_TRUCK" else (180, 330)
            downtime += self._add_maintenance(
                r, v, "PREVENTIVE_SERVICE", ts, s.odo, s.eh, day, resolved,
                "Oil and filter change, multi-point inspection",
                cost=r.uniform(lo, hi) * (1 + 0.03 * age), downtime=r.uniform(1.5, 4))
            s.km_svc = s.h_svc = 0.0
            s.next_svc_factor = v.client.service_compliance * r.uniform(0.92, 1.10)
            if s.km_tires >= TIRE_INTERVAL_KM[vtype]:
                lo, hi = (1_200, 2_400) if vtype == "BOX_TRUCK" else (600, 1_400)
                downtime += self._add_maintenance(r, v, "TIRE_SERVICE", ts, s.odo, s.eh, day, None,
                                                  "Replaced tires, alignment", cost=r.uniform(lo, hi),
                                                  downtime=r.uniform(1, 3))
                s.km_tires = 0.0
            if s.km_brakes >= BRAKE_INTERVAL_KM[vtype]:
                lo, hi = (900, 1_800) if vtype == "BOX_TRUCK" else (450, 1_100)
                downtime += self._add_maintenance(r, v, "BRAKE_SERVICE", ts, s.odo, s.eh, day, None,
                                                  "Brake pads and rotors", cost=r.uniform(lo, hi),
                                                  downtime=r.uniform(2, 5))
                s.km_brakes = 0.0

        # Breakdowns with no warning code.
        if r.random() < self.hazard(v, s, day, 0.0006):
            downtime += self._repair(r, v, s, ts, day, None)

        s.downtime_days = int(downtime // 10)

    def _repair(self, r, v, s, ts, day, fault) -> float:
        age = (day - v.acquisition_date).days / 365.25
        severity = fault[2] if fault else None
        sev_factor = {"CRITICAL": 2.5, "MEDIUM": 1.2, "LOW": 0.6, None: 1.0}[severity]
        type_factor = 1.6 if v.spec.vehicle_type == "BOX_TRUCK" else 1.0
        cost = min(15_000, max(150, 700 * r.lognormvariate(0, 0.6) * (1 + 0.12 * age) * type_factor * sev_factor))
        downtime = {"CRITICAL": (24, 96), "MEDIUM": (3, 16), "LOW": (1, 4), None: (6, 60)}[severity]
        desc = f"Diagnosed and repaired {fault[0]}: {fault[1]}" if fault else r.choice(BREAKDOWN_REPAIRS)
        return self._add_maintenance(r, v, "UNPLANNED_REPAIR", ts, s.odo, s.eh, day,
                                     fault[0] if fault else None, desc,
                                     cost=cost, downtime=r.uniform(*downtime))

    def _add_maintenance(self, r, v, service_type, ts, odo, eh, day, resolved_dtc, description,
                         cost: Optional[float] = None, downtime: Optional[float] = None) -> float:
        if cost is None:
            lo, hi = (350, 600) if v.spec.vehicle_type == "BOX_TRUCK" else (180, 330)
            cost = r.uniform(lo, hi)
        if downtime is None:
            downtime = r.uniform(1.5, 4)
        self.maint_rows.append({
            "maintenance_id": None,  # assigned after sorting
            "vehicle_id": v.vehicle_id,
            "service_ts": iso(ts),
            "service_type": service_type,
            "odometer_km": round(odo, 1),
            "engine_hours": round(eh, 1),
            "vendor": r.choice(v.client.vendors[service_type]),
            "cost_cad": round(cost, 2),
            "downtime_hours": round(downtime, 1),
            "resolved_dtc": resolved_dtc,
            "description": description,
        })
        return downtime

    # --- output --------------------------------------------------------------
    def _write_telematics(self, month_key, record):
        fh = self.tele_handles.get(month_key)
        if fh is None:
            path = self.out / "telematics_events" / f"telematics_events_{month_key[0]}_{month_key[1]:02d}.json.gz"
            path.parent.mkdir(parents=True, exist_ok=True)
            # mtime=0 keeps the gzip header, and so the file, identical across runs.
            fh = io.TextIOWrapper(gzip.GzipFile(path, "wb", mtime=0), encoding="utf-8", newline="\n")
            self.tele_handles[month_key] = fh
        fh.write(json.dumps(record, separators=(",", ":")) + "\n")

    def _write_csv(self, rel_path: str, rows: list, columns: list):
        path = self.out / rel_path
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=columns, lineterminator="\n")
            w.writeheader()
            for row in rows:
                w.writerow({k: ("" if row.get(k) is None else row.get(k)) for k in columns})

    def resale_rows(self) -> list:
        combos = sorted({(v.spec, v.model_year) for v in self.vehicles},
                        key=lambda x: (x[0].make, x[0].model, x[1]))
        rows = []
        for spec, model_year in combos:
            for m in self.months:
                if date(model_year - 1, 9, 1) > m:
                    continue
                for band in DISTANCE_BANDS:
                    rows.append({
                        "make": spec.make,
                        "model": spec.model,
                        "model_year": model_year,
                        "distance_band": band[2],
                        "band_min_km": band[0],
                        "band_max_km": band[1],
                        "valuation_month": m.isoformat(),
                        "market_value_cad": round(self.market_value(spec, model_year, band, m), 2),
                    })
        return rows

    def inject_table_issues(self, vehicle_rows, fuel_rows, maint_rows):
        r = random.Random(f"{self.seed}-dq-tables")

        # Vehicles: inconsistent classification spellings.
        for row in vehicle_rows:
            if r.random() < DQ["vehicle_variant"]:
                col = r.choice(("vehicle_type", "fuel_type", "ownership"))
                bad = r.choice(VARIANTS[col][row[col]])
                self.manifest.append(("CLASSIFICATION_VARIANT", "vehicles", row["vehicle_id"], row["vehicle_id"],
                                      f"{col}: '{bad}' should be {row[col]}"))
                row[col] = bad

        # Fuel: exact duplicates, re-sent duplicates with a new id, orphans.
        extra = []
        for row in fuel_rows:
            x = r.random()
            if x < DQ["fuel_exact_dup"]:
                extra.append(dict(row))
                self.manifest.append(("DUPLICATE_FUEL_EXACT", "fuel_transactions", row["transaction_id"],
                                      row["vehicle_id"], "same transaction_id sent twice"))
            elif x < DQ["fuel_exact_dup"] + DQ["fuel_resent"]:
                dup = dict(row, transaction_id=new_uuid(r))
                extra.append(dup)
                self.manifest.append(("DUPLICATE_FUEL_RESENT", "fuel_transactions", dup["transaction_id"],
                                      row["vehicle_id"], f"re-sent copy of {row['transaction_id']}"))
        for _ in range(DQ["fuel_orphans"]):
            ghost = f"V9{r.randrange(1000):03d}"
            orphan = dict(r.choice(fuel_rows), transaction_id=new_uuid(r), vehicle_id=ghost,
                          fuel_card_id=f"FC-UNKNOWN-{ghost}")
            extra.append(orphan)
            self.manifest.append(("ORPHAN_ROW", "fuel_transactions", orphan["transaction_id"], ghost,
                                  "vehicle_id does not exist"))
        fuel_rows.extend(extra)

        # Maintenance: service type variants, unmappable values, orphans.
        for row in maint_rows:
            if r.random() < DQ["service_variant"]:
                bad = r.choice(VARIANTS["service_type"][row["service_type"]])
                self.manifest.append(("CLASSIFICATION_VARIANT", "maintenance_events", row["maintenance_id"],
                                      row["vehicle_id"], f"service_type: '{bad}' should be {row['service_type']}"))
                row["service_type"] = bad
        still_clean = [row for row in maint_rows if row["service_type"] in SERVICE_TYPES]
        for row in r.sample(still_clean, DQ["service_invalid"]):
            bad = r.choice(INVALID_SERVICE_TYPES)
            self.manifest.append(("INVALID_CLASSIFICATION", "maintenance_events", row["maintenance_id"],
                                  row["vehicle_id"], f"service_type: '{bad}' cannot be mapped"))
            row["service_type"] = bad
        next_id = len(maint_rows) + 1
        for _ in range(DQ["maintenance_orphans"]):
            ghost = f"V9{r.randrange(1000):03d}"
            orphan = dict(r.choice(maint_rows), maintenance_id=f"M{next_id:06d}", vehicle_id=ghost)
            next_id += 1
            maint_rows.append(orphan)
            self.manifest.append(("ORPHAN_ROW", "maintenance_events", orphan["maintenance_id"], ghost,
                                  "vehicle_id does not exist"))

    def run(self):
        if self.out.exists():
            shutil.rmtree(self.out)  # always rebuild from scratch: same seed, same files
        self.out.mkdir(parents=True)

        self.build_initial_fleet()
        i = 0
        while i < len(self.vehicles):  # replacements are appended as vehicles retire
            self.simulate(self.vehicles[i])
            i += 1
        for fh in self.tele_handles.values():
            fh.close()

        # Maintenance ids in time order.
        self.maint_rows.sort(key=lambda m: (m["service_ts"], m["vehicle_id"], m["service_type"]))
        for n, row in enumerate(self.maint_rows, start=1):
            row["maintenance_id"] = f"M{n:06d}"

        clients = [{
            "client_id": c.client_id, "company_name": c.company_name, "industry": c.industry,
            "city": c.city, "province": c.province, "seasonal_pattern": SEASONAL_PATTERN[c.industry],
            "client_since": c.client_since.isoformat(),
        } for c in self.clients]
        vehicles = [{
            "vehicle_id": v.vehicle_id, "client_id": v.client.client_id, "vin": v.vin,
            "make": v.spec.make, "model": v.spec.model, "model_year": v.model_year,
            "vehicle_type": v.spec.vehicle_type, "fuel_type": v.spec.fuel_type, "ownership": v.ownership,
            "acquisition_date": v.acquisition_date.isoformat(), "acquisition_cost_cad": v.acquisition_cost_cad,
            "status": v.status,
            "disposal_date": v.disposal_date.isoformat() if v.disposal_date else None,
            "disposal_reason": v.disposal_reason, "disposal_price_cad": v.disposal_price_cad,
        } for v in sorted(self.vehicles, key=lambda v: v.num)]
        leases = [dict(v.lease, start_date=v.lease["start_date"].isoformat(),
                       end_date=v.lease["end_date"].isoformat())
                  for v in sorted(self.vehicles, key=lambda v: v.num) if v.lease]
        leases.sort(key=lambda row: row["contract_id"])
        dtc = [{"dtc_code": d[0], "description": d[1], "severity": d[2]} for d in DTC_CODES]

        self.inject_table_issues(vehicles, self.fuel_rows, self.maint_rows)

        self._write_csv("clients/clients.csv", clients, list(clients[0]))
        self._write_csv("vehicles/vehicles.csv", vehicles, list(vehicles[0]))
        self._write_csv("lease_contracts/lease_contracts.csv", leases, list(leases[0]))
        self._write_csv("dtc_codes/dtc_codes.csv", dtc, list(dtc[0]))
        resale = self.resale_rows()
        self._write_csv("resale_values/resale_values.csv", resale, list(resale[0]))
        maint_cols = list(self.maint_rows[0])
        self._write_csv("maintenance_events/maintenance_events.csv", self.maint_rows, maint_cols)

        fuel_cols = list(self.fuel_rows[0])
        by_month = defaultdict(list)
        for row in sorted(self.fuel_rows, key=lambda x: (x["transaction_ts"], x["vehicle_id"])):
            by_month[row["transaction_ts"][:7].replace("-", "_")].append(row)
        for month, rows in sorted(by_month.items()):
            self._write_csv(f"fuel_transactions/fuel_transactions_{month}.csv", rows, fuel_cols)

        manifest = [dict(zip(("issue_type", "table_name", "record_key", "vehicle_id", "detail"), m))
                    for m in self.manifest]
        self._write_csv("_dq_manifest.csv", manifest,
                        ["issue_type", "table_name", "record_key", "vehicle_id", "detail"])
        with open(self.out / "_sim_state.json", "w", encoding="utf-8") as f:
            json.dump({"seed": self.seed, "history_start": self.start.isoformat(),
                       "history_end_exclusive": self.end.isoformat(),
                       "vehicles": self.states}, f, indent=2)

        self.print_summary(clients, vehicles, leases, resale)

    def print_summary(self, clients, vehicles, leases, resale):
        retired = Counter((v.client.client_id, v.disposal_reason) for v in self.vehicles if v.status == "RETIRED")
        print(f"\nBackfill {self.start} to {self.end} (exclusive), seed {self.seed}")
        print(f"Output: {self.out}\n")
        print(f"{'table':<22}{'rows':>12}")
        for name, n in (("clients", len(clients)), ("vehicles", len(vehicles)), ("lease_contracts", len(leases)),
                        ("dtc_codes", len(DTC_CODES)), ("resale_values", len(resale)),
                        ("maintenance_events", len(self.maint_rows)), ("fuel_transactions", len(self.fuel_rows)),
                        ("telematics records", self.tele_counts["records"]),
                        ("telematics readings", self.tele_counts["readings"])):
            print(f"{name:<22}{n:>12,}")
        print("\nRetirements per client")
        for c in self.clients:
            active = sum(1 for v in self.vehicles if v.client is c and v.status == "ACTIVE")
            print(f"  {c.client_id}  active {active:>3}   sold {retired[(c.client_id, 'SOLD')]:>3}"
                  f"   lease returns {retired[(c.client_id, 'LEASE_RETURN')]:>3}")
        print("\nInjected data quality issues")
        for issue, n in sorted(Counter((m[0], m[1]) for m in self.manifest).items()):
            print(f"  {issue[0]:<24}{issue[1]:<20}{n:>8,}")


def main():
    ap = argparse.ArgumentParser(description="Generate synthetic fleet history (all data is simulated).")
    ap.add_argument("--seed", type=int, default=DEFAULT_SEED)
    ap.add_argument("--start", type=date.fromisoformat, default=DEFAULT_START)
    ap.add_argument("--end", type=date.fromisoformat, default=DEFAULT_END, help="exclusive")
    ap.add_argument("--out", type=Path, default=DEFAULT_OUT)
    args = ap.parse_args()
    Backfill(args.seed, args.start, args.end, args.out).run()


if __name__ == "__main__":
    main()
