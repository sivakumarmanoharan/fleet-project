"""
Live telematics stream: continues every active vehicle from where the backfill stopped.

All data is simulated.

How it works
    The stream runs on a simulated clock that starts where the backfill ended and
    moves faster than real time (--speed 60 means one simulated hour per real minute).
    Vehicles keep the backfill's behaviour: hourly pings during a 07:00-17:00 local
    shift on working days, fuel burn and refuels, fault codes, and the same injected
    data quality problems (missing pings, odometer glitches), logged to a manifest.

    Nights and weekends are skipped by default, so something is always happening.

    Every --flush-every real seconds, pending readings are written to a small file
    and uploaded (PUT) to @LANDING. PUT needs no warehouse, so this is free.
    Every --copy-every real minutes, COPY INTO loads the new files into Bronze.
    That is the only step that wakes the warehouse.

    The session stops by itself after --minutes, then loads what is left and
    suspends the warehouse. Ctrl+C does the same.

    State is saved after every upload, so the next session continues the timeline.

Usage
    python generator/stream.py                      # 30 minutes, upload and load
    python generator/stream.py --minutes 10
    python generator/stream.py --no-upload          # local files only, no Snowflake
    python generator/stream.py --fresh              # restart from the backfill end state

Output
    data/stream/telematics_events/*.json.gz   same record shape as the backfill
    data/stream/fuel_transactions/*.csv       refuels during the stream
    data/stream/_dq_manifest.csv              injected problems (answer key), appended
    data/stream/_stream_state.json            simulated clock and vehicle state
"""

from __future__ import annotations

import argparse
import csv
import gzip
import io
import json
import math
import random
import sys
import time
from collections import Counter, defaultdict
from datetime import date, datetime, time as dtime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from backfill import DQ, DTC_CODES, IDLE_BURN_L_PER_H, SEASONALITY, UTC, iso, new_uuid

ROOT = Path(__file__).resolve().parents[1]
BACKFILL_DIR = ROOT / "data" / "backfill"
STREAM_DIR = ROOT / "data" / "stream"
STATE_FILE = STREAM_DIR / "_stream_state.json"
MANIFEST_FILE = STREAM_DIR / "_dq_manifest.csv"
BRONZE_SQL = ROOT / "sql" / "02_bronze.sql"

DTC_BY_CODE = {d[0]: d for d in DTC_CODES}
SERVICE_KM = {"GASOLINE": 12_000, "DIESEL": 16_000}
FUEL_COLUMNS = ["transaction_id", "vehicle_id", "fuel_card_id", "transaction_ts", "station_name",
                "station_city", "station_province", "fuel_type", "litres", "price_per_litre_cad", "amount_cad"]
MANIFEST_COLUMNS = ["issue_type", "table_name", "record_key", "vehicle_id", "detail"]

# Guardrails: the stream is for active development sessions only.
MAX_MINUTES = 180
MIN_COPY_EVERY = 5


# ---------------------------------------------------------------------------
# Reference data taken from the backfill, so the stream stays consistent with it
# ---------------------------------------------------------------------------
def load_fuel_reference():
    """Each client's fuel stations (by how often they were used) and the latest prices."""
    stations = defaultdict(Counter)
    prices = defaultdict(list)
    files = sorted((BACKFILL_DIR / "fuel_transactions").glob("*.csv"))
    if not files:
        sys.exit("No backfill fuel files found. Run generator/backfill.py first.")
    for path in files:
        latest = path == files[-1]
        with open(path, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                if row["fuel_card_id"].startswith("FC-UNKNOWN"):
                    continue
                client_id = row["fuel_card_id"].split("-")[1]
                stations[client_id][(row["station_name"], row["station_city"], row["station_province"])] += 1
                if latest:
                    prices[row["fuel_type"]].append(float(row["price_per_litre_cad"]))
    top_stations = {c: counter.most_common(6) for c, counter in stations.items()}
    median_price = {ft: sorted(p)[len(p) // 2] for ft, p in prices.items()}
    return top_stations, median_price


# ---------------------------------------------------------------------------
# One vehicle on the road
# ---------------------------------------------------------------------------
class LiveVehicle:
    def __init__(self, state: dict, stations, prices, r: random.Random, q: random.Random):
        self.s = state                      # persisted between sessions
        self.tz = ZoneInfo(state["time_zone"])
        self.r, self.q = r, q                # behaviour and data quality random streams
        self.stations = stations
        self.prices = prices
        self.refuel_threshold = r.uniform(0.18, 0.35)
        self.plan_date: date | None = None
        self.pings: list = []
        self.idx = 0
        self.pending: list = []

    # --- planning ---------------------------------------------------------
    def local_date(self, ts: datetime) -> date:
        return ts.astimezone(self.tz).date()

    def start_at(self, sim_now: datetime):
        """Plan the day containing sim_now and skip pings that are already in the past."""
        self.plan(self.local_date(sim_now))
        while self.idx < len(self.pings) and self.pings[self.idx] <= sim_now:
            self.idx += 1

    def plan(self, d: date):
        s, r, q = self.s, self.r, self.q
        self.plan_date, self.pings, self.idx = d, [], 0
        mult = SEASONALITY[s["industry"]][d.month - 1]
        weekday = d.weekday()
        if weekday < 5:
            p_work = min(0.97, 0.88 * mult)
        elif weekday == 5 and s["industry"] == "CONSTRUCTION" and mult > 1.0:
            p_work = 0.25
        else:
            p_work = 0.0
        if r.random() >= p_work:
            return
        n = r.randint(8, 10)
        weights = [r.gammavariate(2, 1) for _ in range(n)]
        total = sum(weights)
        self.weights = [w / total for w in weights]
        self.day_km = s["annual_km"] / 230 * math.sqrt(mult) * r.lognormvariate(0, 0.25)
        t0 = (datetime.combine(d, dtime(7, 0), tzinfo=self.tz) + timedelta(minutes=r.randrange(60))).astimezone(UTC)
        self.pings = [t0] + [t0 + timedelta(hours=i + 1, seconds=r.randint(-40, 40)) for i in range(n)]
        self.winter = 1.12 if d.month in (12, 1, 2) else 1.05 if d.month in (3, 11) else 1.0

        # No maintenance happens during the stream, so overdue vehicles fault more and more.
        self.fault_idx = 0
        overdue = max(0.0, s["km_since_service"] / SERVICE_KM[s["fuel_type"]] - 1)
        hazard = 0.0012 * 1.5 * (1 + 3 * overdue) * (1 + 0.5 * s["odometer_km"] / 200_000)
        if not s["active_dtc"] and r.random() < hazard:
            s["active_dtc"] = r.choices(DTC_CODES, weights=[c[4] for c in DTC_CODES])[0][0]
            self.fault_idx = r.randint(0, n)

        self.outage = range(0)
        if q.random() < DQ["outage_per_day"]:
            o_start = q.randint(0, n)
            self.outage = range(o_start, o_start + q.randint(3, 6))

    def next_event(self) -> datetime:
        """When this vehicle next needs attention: its next ping, or the next local midnight."""
        if self.idx < len(self.pings):
            return self.pings[self.idx]
        return datetime.combine(self.plan_date + timedelta(days=1), dtime(0, 0), tzinfo=self.tz).astimezone(UTC)

    # --- driving ------------------------------------------------------------
    def advance(self, sim_now: datetime, out: "Session"):
        while True:
            if self.idx < len(self.pings):
                ts = self.pings[self.idx]
                if ts > sim_now:
                    return
                self.emit(self.idx, ts, out)
                self.idx += 1
            else:
                nxt = self.plan_date + timedelta(days=1)
                if datetime.combine(nxt, dtime(0, 0), tzinfo=self.tz) > sim_now:
                    return
                self.plan(nxt)

    def emit(self, i: int, ts: datetime, out: "Session"):
        s, r, q = self.s, self.r, self.q
        speed, idle_h = 0.0, 0.0
        if i > 0:  # interval i runs from ping i-1 to ping i
            km = self.day_km * self.weights[i - 1]
            avg_speed = r.uniform(32, 58)
            moving_h = km / avg_speed
            if moving_h > 0.92:
                moving_h, km = 0.92, 0.92 * avg_speed
            idle_h = min(1 - moving_h, moving_h * s["idle_ratio"] / (1 - s["idle_ratio"]) * r.uniform(0.6, 1.4))
            wear = 1 + 0.06 * min(1.5, s["km_since_service"] / SERVICE_KM[s["fuel_type"]])
            fault = DTC_BY_CODE.get(s["active_dtc"]) if i >= self.fault_idx else None
            fault_pen = 1 + fault[3] if fault else 1.0
            litres = (km * s["l_per_100km"] * self.winter * wear * fault_pen / 100
                      + idle_h * IDLE_BURN_L_PER_H[s["vehicle_type"]])
            s["odometer_km"] += km
            s["engine_hours"] += moving_h + idle_h
            s["km_since_service"] += km
            s["fuel_litres"] = max(s["tank_litres"] * 0.03, s["fuel_litres"] - litres)
            if s["fuel_litres"] < s["tank_litres"] * self.refuel_threshold:
                self.refuel(ts - timedelta(minutes=r.randint(10, 50)), out)
            speed = 0.0 if r.random() < 0.3 else max(0.0, r.gauss(avg_speed, 12))

        event_id = new_uuid(r)
        dtc = [s["active_dtc"]] if s["active_dtc"] and i >= self.fault_idx else []
        if i in self.outage or q.random() < DQ["ping_drop"]:
            out.log_issue("MISSING_PING", "telematics_events", event_id, s["vehicle_id"],
                          f"expected ping at {iso(ts)}" + (" (device outage)" if i in self.outage else ""))
            return
        odo = s["odometer_km"]
        if q.random() < DQ["odometer_glitch"]:
            odo = odo / 10 if q.random() < 0.5 else odo - q.uniform(50, 3_000)
            out.log_issue("ODOMETER_BACKWARD", "telematics_events", event_id, s["vehicle_id"],
                          f"reported {odo:.1f} km, true {s['odometer_km']:.1f} km")
        self.pending.append({
            "event_id": event_id,
            "event_ts": iso(ts),
            "odometer_km": round(max(odo, 0.0), 1),
            "engine_hours": round(s["engine_hours"], 2),
            "fuel_level_pct": round(100 * s["fuel_litres"] / s["tank_litres"], 1),
            "speed_kph": round(speed, 1),
            "idle_hours": round(idle_h, 3),
            "dtc_codes": dtc,
        })
        out.readings += 1

    def refuel(self, ts: datetime, out: "Session"):
        s, r = self.s, self.r
        litres = s["tank_litres"] * r.uniform(0.90, 1.0) - s["fuel_litres"]
        if litres < 15 or not self.stations:
            return
        (name, city, province), _ = r.choices(self.stations, weights=[n for _, n in self.stations])[0]
        price = round(self.prices[s["fuel_type"]] * r.uniform(0.97, 1.04), 3)
        out.fuel_rows.append({
            "transaction_id": new_uuid(r),
            "vehicle_id": s["vehicle_id"],
            "fuel_card_id": f"FC-{s['client_id']}-{s['vehicle_id'][1:]}",
            "transaction_ts": iso(ts),
            "station_name": name,
            "station_city": city,
            "station_province": province,
            "fuel_type": s["fuel_type"],
            "litres": round(litres, 2),
            "price_per_litre_cad": price,
            "amount_cad": round(litres * price, 2),
        })
        s["fuel_litres"] += litres


# ---------------------------------------------------------------------------
# A streaming session
# ---------------------------------------------------------------------------
class Session:
    def __init__(self, args):
        self.args = args
        self.readings = 0
        self.fuel_rows: list = []
        self.issues: list = []
        self.totals = Counter()
        self.conn = None
        self.copy_sql: dict = {}
        self._load_state()

    # --- state ----------------------------------------------------------------
    def _load_state(self):
        backfill_state_path = BACKFILL_DIR / "_sim_state.json"
        if not backfill_state_path.exists():
            sys.exit("No backfill state found. Run generator/backfill.py first.")
        backfill = json.loads(backfill_state_path.read_text(encoding="utf-8"))
        fingerprint = f"{backfill['seed']}|{backfill['history_end_exclusive']}"

        saved = None
        if STATE_FILE.exists() and not self.args.fresh:
            saved = json.loads(STATE_FILE.read_text(encoding="utf-8"))
            if saved.get("backfill_fingerprint") != fingerprint:
                print("The backfill was regenerated since the last stream session. Starting fresh.")
                saved = None

        if saved:
            self.session_no = saved["session"] + 1
            self.file_seq = saved["file_seq"]
            self.sim_now = datetime.fromisoformat(saved["sim_clock"])
            vehicle_states = saved["vehicles"]
        else:
            self.session_no = 1
            self.file_seq = 0
            self.sim_now = datetime.combine(date.fromisoformat(backfill["history_end_exclusive"]), dtime(0, 0), UTC)
            vehicle_states = backfill["vehicles"]
        self.fingerprint = fingerprint
        self.seed = backfill["seed"]

        stations, prices = load_fuel_reference()
        self.vehicles = []
        for vs in vehicle_states:
            r = random.Random(f"{self.seed}-stream-{self.session_no}-{vs['vehicle_id']}")
            q = random.Random(f"{self.seed}-stream-dq-{self.session_no}-{vs['vehicle_id']}")
            v = LiveVehicle(vs, stations.get(vs["client_id"], []), prices, r, q)
            v.start_at(self.sim_now)
            self.vehicles.append(v)

    def save_state(self):
        STREAM_DIR.mkdir(parents=True, exist_ok=True)
        state = {
            "backfill_fingerprint": self.fingerprint,
            "session": self.session_no,
            "file_seq": self.file_seq,
            "sim_clock": self.sim_now.isoformat(),
            "vehicles": [v.s for v in self.vehicles],
        }
        tmp = STATE_FILE.with_suffix(".tmp")
        tmp.write_text(json.dumps(state, indent=1), encoding="utf-8")
        tmp.replace(STATE_FILE)

    def log_issue(self, issue_type, table, key, vehicle_id, detail):
        self.issues.append((issue_type, table, key, vehicle_id, detail))
        self.totals[issue_type] += 1

    # --- Snowflake ----------------------------------------------------------
    def connect(self):
        if self.args.no_upload:
            return
        from snowflake.connector.util_text import split_statements
        from snowflake_conn import connect

        self.conn = connect()
        sql = BRONZE_SQL.read_text(encoding="utf-8")
        # Reuse the exact COPY statements from 02_bronze.sql: one definition, two callers.
        for stmt, _ in split_statements(io.StringIO(sql), remove_comments=True):
            flat = " ".join(stmt.split())
            for table in ("TELEMATICS_EVENTS", "FUEL_TRANSACTIONS"):
                if flat.upper().startswith(f"COPY INTO {table} "):
                    self.copy_sql[table] = stmt
        if len(self.copy_sql) != 2:
            sys.exit(f"Could not find both COPY statements in {BRONZE_SQL.name}.")

    def put(self, path: Path, folder: str):
        if self.conn is None:
            return
        self.conn.cursor().execute(
            f"PUT 'file://{path.resolve().as_posix()}' @LANDING/{folder}/ "
            "AUTO_COMPRESS = TRUE OVERWRITE = FALSE"
        )

    def copy(self):
        if self.conn is None:
            return
        cur = self.conn.cursor()
        parts = []
        for table, stmt in self.copy_sql.items():
            cur.execute(stmt)
            cols = [d[0].lower() for d in cur.description]
            rows = cur.fetchall()
            if cols == ["status"]:
                parts.append(f"{table.lower()} 0 files")
                continue
            loaded = sum(r[cols.index("rows_loaded")] for r in rows)
            errors = sum(r[cols.index("errors_seen")] for r in rows)
            self.totals[f"copied_{table}"] += loaded
            parts.append(f"{table.lower()} {len(rows)} files, {loaded:,} rows, {errors} errors")
        print(f"  COPY INTO Bronze: {'; '.join(parts)}")

    def suspend_warehouse(self):
        if self.conn is None:
            return
        try:
            self.conn.cursor().execute("ALTER WAREHOUSE FLEET_WH SUSPEND")
            print("Warehouse FLEET_WH suspended.")
        except Exception:
            print("Warehouse FLEET_WH already suspended.")

    # --- files --------------------------------------------------------------
    def flush(self):
        """Write pending readings and refuels to files and upload them."""
        records = []
        for v in self.vehicles:
            if v.pending:
                records.append({
                    "device_id": v.s["device_id"],
                    "vehicle_id": v.s["vehicle_id"],
                    "firmware_version": v.s["firmware_version"],
                    "upload_ts": iso(self.sim_now),
                    "readings": v.pending,
                })
                v.pending = []
        uploaded = []
        if records:
            self.file_seq += 1
            path = STREAM_DIR / "telematics_events" / f"telematics_stream_s{self.session_no:03d}_{self.file_seq:05d}.json.gz"
            path.parent.mkdir(parents=True, exist_ok=True)
            with gzip.open(path, "wt", encoding="utf-8", newline="\n") as f:
                for rec in records:
                    f.write(json.dumps(rec, separators=(",", ":")) + "\n")
            self.put(path, "telematics_events")
            uploaded.append(path.name)
            self.totals["records"] += len(records)
        if self.fuel_rows:
            self.file_seq += 1
            path = STREAM_DIR / "fuel_transactions" / f"fuel_stream_s{self.session_no:03d}_{self.file_seq:05d}.csv"
            path.parent.mkdir(parents=True, exist_ok=True)
            with open(path, "w", newline="", encoding="utf-8") as f:
                w = csv.DictWriter(f, fieldnames=FUEL_COLUMNS, lineterminator="\n")
                w.writeheader()
                w.writerows(self.fuel_rows)
            self.put(path, "fuel_transactions")
            uploaded.append(path.name)
            self.totals["fuel"] += len(self.fuel_rows)
            self.fuel_rows = []
        if self.issues:
            new_file = not MANIFEST_FILE.exists()
            with open(MANIFEST_FILE, "a", newline="", encoding="utf-8") as f:
                w = csv.writer(f, lineterminator="\n")
                if new_file:
                    w.writerow(MANIFEST_COLUMNS)
                w.writerows(self.issues)
            self.issues = []
        self.totals["readings"] += self.readings
        on_shift = sum(1 for v in self.vehicles if v.idx > 0 and v.idx < len(v.pings))
        action = "uploaded" if self.conn else "written"
        print(f"  sim {self.sim_now:%a %Y-%m-%d %H:%M} UTC | {self.readings:>4} readings, "
              f"{on_shift:>3} vehicles on shift | {action}: {', '.join(uploaded) or 'nothing new'}")
        self.readings = 0
        self.save_state()

    # --- main loop ------------------------------------------------------------
    def run(self):
        a = self.args
        cycles = a.minutes / a.copy_every + 1
        est_credits = cycles * 2 / 60  # each COPY: about 1 min billed + 1 min idle before auto-suspend
        print(f"Stream session {self.session_no}: {len(self.vehicles)} vehicles, "
              f"simulated clock starts {self.sim_now:%a %Y-%m-%d %H:%M} UTC, speed x{a.speed:g}")
        print(f"Stops after {a.minutes} min. Upload every {a.flush_every} s (free), "
              f"load every {a.copy_every} min.")
        if not a.no_upload:
            print(f"Estimated warehouse use: about {est_credits:.2f} credits (${est_credits * 2:.2f}).")
        print("Press Ctrl+C to stop early.\n")

        self.connect()
        started = last_tick = last_flush = last_copy = time.monotonic()
        try:
            while True:
                now = time.monotonic()
                self.sim_now += timedelta(seconds=(now - last_tick) * a.speed)
                last_tick = now

                if not a.no_skip_idle:
                    next_event = min(v.next_event() for v in self.vehicles)
                    if next_event > self.sim_now + timedelta(minutes=10):
                        self.sim_now = next_event  # jump over nights and weekends

                for v in self.vehicles:
                    v.advance(self.sim_now, self)

                if now - last_flush >= a.flush_every:
                    self.flush()
                    last_flush = now
                if now - last_copy >= a.copy_every * 60:
                    self.copy()
                    last_copy = now
                if now - started >= a.minutes * 60:
                    print("\nTime limit reached.")
                    break
                time.sleep(1)
        except KeyboardInterrupt:
            print("\nStopping.")
        finally:
            self.flush()
            self.copy()
            self.suspend_warehouse()
            if self.conn:
                self.conn.close()
            print(f"\nSession {self.session_no} totals: {self.totals['readings']:,} readings in "
                  f"{self.totals['records']:,} records, {self.totals['fuel']:,} refuels, "
                  f"{self.totals['MISSING_PING']:,} missing pings, "
                  f"{self.totals['ODOMETER_BACKWARD']:,} odometer glitches.")
            print(f"Simulated clock stopped at {self.sim_now:%a %Y-%m-%d %H:%M} UTC. Next session continues from here.")


def main():
    ap = argparse.ArgumentParser(description="Stream simulated telematics into Bronze (all data is simulated).")
    ap.add_argument("--minutes", type=float, default=30, help=f"session length in real minutes (max {MAX_MINUTES})")
    ap.add_argument("--speed", type=float, default=60, help="simulated seconds per real second")
    ap.add_argument("--flush-every", type=int, default=60, help="real seconds between uploads (PUT)")
    ap.add_argument("--copy-every", type=float, default=10,
                    help=f"real minutes between loads (COPY INTO), minimum {MIN_COPY_EVERY}")
    ap.add_argument("--no-upload", action="store_true", help="write local files only, no Snowflake")
    ap.add_argument("--no-skip-idle", action="store_true", help="don't jump over nights and weekends")
    ap.add_argument("--fresh", action="store_true", help="ignore saved stream state and restart from the backfill")
    args = ap.parse_args()

    if not 0 < args.minutes <= MAX_MINUTES:
        sys.exit(f"--minutes must be between 0 and {MAX_MINUTES}. The stream is for active sessions, never overnight.")
    if args.copy_every < MIN_COPY_EVERY and not args.no_upload:
        sys.exit(f"--copy-every must be at least {MIN_COPY_EVERY} minutes. Each load wakes the warehouse.")
    Session(args).run()


if __name__ == "__main__":
    main()
