# Fleet Analytics on Snowflake — Project Brief

## Goal
Portfolio project simulating a fleet management + fleet financing company's data on Snowflake,
answering three business questions:

1. **Maintenance priority** — which vehicles need maintenance first?
2. **Financing opportunities** — which vehicles are candidates for a purchase leaseback or lease restructuring?
3. **Replacement timing** — are vehicles being replaced at the right time?

All data is synthetic. Describe it as a portfolio project with simulated data, never as real company data.

## Hard constraints (Snowflake trial account)
- 30-day trial, **Standard edition**, about $2.00 per credit.
- Trial started with $360 of free usage; $355.90 left on 2026-09-30.
- **A credit card is on file**, so usage past the free balance may be billed. The guardrails below are mandatory.
- Trial accounts have **no external network access**: Snowflake cannot call outside APIs.
  All data is generated locally in Python and pushed in (PUT to internal stage, then COPY INTO).
- What costs money: running warehouses (credits), serverless features and Cortex AI (credits, not covered by resource monitors),
  cloud services above 10% of daily compute, and storage. Warehouses are by far the biggest item.
- Standard edition limits: no materialized views, no masking or row access policies, Time Travel max 1 day.

### Cost guardrails (set up before anything else)
- One X-SMALL warehouse `FLEET_WH`: `AUTO_SUSPEND = 60`, `AUTO_RESUME = TRUE`, statement timeout 15 minutes.
- One **account-level** resource monitor `FLEET_RM` covering all warehouses (details below).
- A Budget with email alerts for serverless and AI spend, which the resource monitor cannot stop.
- Daily work runs as role `FLEET_ENGINEER`, not ACCOUNTADMIN. It cannot resize the warehouse or run serverless tasks.
- Tasks run on `FLEET_WH` (never serverless), every 10–15 minutes, not every minute. Suspend tasks when not working.
- Every task checks its stream for new data before it runs (`WHEN SYSTEM$STREAM_HAS_DATA(...)`), so it never wakes the warehouse for nothing.
- Run the live stream only while actively developing, never overnight.
- Keep data small: ~5 clients, ~200 vehicles, ~2 years of history.
- No Cortex AI functions in this project: they bill per token and no resource monitor can stop them.
- Use Snowsight dashboards for visuals (no extra tools).

### Resource monitor (decided 2026-09-30)
- `FLEET_RM`: CREDIT_QUOTA = 40 (about $80 max), FREQUENCY = NEVER (one total cap that never resets).
- Triggers: notify at 50% and 75%, SUSPEND at 90%, SUSPEND_IMMEDIATE at 100%.
- Created with `IF NOT EXISTS`, never `OR REPLACE`: replacing it resets the used-credit counter to zero.
- Raising the quota is a deliberate decision, recorded here with a date.

## Snowflake objects
| Object | Name | Owner | Notes |
|---|---|---|---|
| Resource monitor | `FLEET_RM` | ACCOUNTADMIN | Account level |
| Warehouse | `FLEET_WH` | SYSADMIN | `FLEET_ENGINEER` has USAGE + OPERATE only |
| Role | `FLEET_ENGINEER` | — | Rolls up to SYSADMIN |
| Database | `FLEET_DB` | `FLEET_ENGINEER` | Data retention 1 day |
| Schemas | `BRONZE`, `SILVER`, `GOLD` | `FLEET_ENGINEER` | Quarantine tables live in `SILVER` |
| Stage | `FLEET_DB.BRONZE.LANDING` | `FLEET_ENGINEER` | One folder per table, e.g. `@LANDING/telematics_events/` |

## Architecture
Python simulator (laptop) → internal stage → BRONZE (raw) → SILVER (clean, validated) → GOLD (business answers) → Snowsight dashboard

- **Backfill** (`generator/backfill.py`): ~2 years of history for all tables, written as CSV/JSON.
- **Live stream** (`generator/stream.py`): telematics events every few seconds, micro-batched into small files, PUT + COPY INTO.
- **Incremental processing**: Snowflake Streams + Tasks move only new rows Bronze → Silver → Gold.
- **Idempotency**: Re-running any load or transformation must not create duplicates. Only new or changed data lands;
  a file or row that was already processed is skipped.
- **Handling bad data**: Rows that fail a quality check go to a quarantine table in SILVER with a reason column.
  Once corrected, they can be reprocessed into the right Silver table.
- **Consistency**: Units, classifications and constants are the same everywhere in the pipeline.
  Distance in kilometres, time in hours, money in CAD, timestamps in UTC. Classifications use one fixed list of allowed values.
- Telematics lands as JSON in a VARIANT column; Silver uses FLATTEN.

## Data model
| Table | Key | Links to | Columns |
|---|---|---|---|
| clients | client_id | — | company_name, industry, city, province, seasonal_pattern, client_since |
| vehicles | vehicle_id | client_id | vin, make, model, model_year, vehicle_type, fuel_type, ownership, acquisition_date, acquisition_cost_cad, **status, disposal_date, disposal_reason, disposal_price_cad** (disposal columns NULL while active) |
| lease_contracts | contract_id | vehicle_id | start_date, end_date, term_months, monthly_payment_cad, distance_allowance_km, excess_km_rate_cad, residual_value_cad |
| telematics_events (streaming, JSON) | event_id | vehicle_id | One JSON record per vehicle per day: device_id, firmware_version, upload_ts and a `readings` array. Each reading: event_id, event_ts, odometer_km, engine_hours, fuel_level_pct, speed_kph, idle_hours, `dtc_codes` array |
| fuel_transactions | transaction_id | vehicle_id | fuel_card_id, transaction_ts, station_name, station_city, station_province, fuel_type, litres, price_per_litre_cad, amount_cad |
| maintenance_events | maintenance_id | vehicle_id | service_ts, service_type, **odometer_km, engine_hours** (at service), vendor, cost_cad, downtime_hours, resolved_dtc, description |
| resale_values | make + model + model_year + distance_band + valuation_month | — | band_min_km, band_max_km, market_value_cad |
| dtc_codes (reference) | dtc_code | — | description, severity |

Allowed values (Silver maps every variant onto these):
- industry: CONSTRUCTION, HVAC, ELECTRICAL
- vehicle_type: PICKUP, VAN, BOX_TRUCK · fuel_type: GASOLINE, DIESEL · ownership: OWNED, LEASED
- status: ACTIVE, RETIRED · disposal_reason: SOLD, LEASE_RETURN
- service_type: PREVENTIVE_SERVICE, TIRE_SERVICE, BRAKE_SERVICE, UNPLANNED_REPAIR
- severity: CRITICAL, MEDIUM, LOW

- Retired vehicles keep their full history so the replacement backtest can compare the actual disposal date against the modelled best date.
- Maintenance rows carry the odometer and engine hours at service, so "distance since last service" is a subtraction, not a time-based join to telematics.
- Snowflake does not enforce primary or foreign keys (only NOT NULL), just like informational constraints in Databricks. Silver must check them itself.
- Telematics pings arrive hourly during a shift (about 07:00–17:00 local) on working days only. No pings on days off is normal, not missing data.
  A missing ping is a gap of more than about 1.5 hours between consecutive readings of the same vehicle on the same local day.
  **Detect gaps per vehicle and day, never per JSON record**: a backfill record holds a whole day, but a stream record
  holds only the few readings since the last upload.

### Deliberate data quality issues (Silver must catch them)
- Duplicate fuel transactions
- Missing telematics pings
- Odometer readings that go backward
- Inconsistent classification values: the same category spelled or cased differently (e.g. `Truck`, `truck`, `TRUCK`).
  Silver maps them to one fixed list of allowed values; anything not on the list goes to quarantine.
- Orphan rows: a vehicle_id or client_id with no matching parent row goes to quarantine.
- Every injected issue is logged in `data/backfill/_dq_manifest.csv` (issue type, table, record key).
  This is the answer key: Silver's quarantine counts are checked against it.

### Backfill generator (`generator/backfill.py`)
- Simulates each vehicle day by day, so odometer, fuel, faults and maintenance stay consistent.
- Deterministic: the same `--seed` gives byte-identical files. It deletes and rebuilds `data/backfill/` on every run.
- Default window 2024-10-01 to 2026-09-30 (exclusive). About 200 vehicles, 640k telematics readings, 35 MB.
- Hidden per-client behaviour, never written to the output: replacement policy (C002 replaces too early, C003 too late),
  service discipline (C003 services late), annual km per vehicle. Gold must discover these from the data.
- Writes `data/backfill/_sim_state.json` with each active vehicle's end state, so `stream.py` continues where the backfill stopped.
- Files starting with `_` are never uploaded to Snowflake.

### Live stream (`generator/stream.py`)
- Continues every active vehicle from `_sim_state.json`: same odometer, fuel, faults and behaviour as the backfill.
- Runs on a **simulated clock** that starts at the backfill end and moves faster than real time
  (`--speed 60` = one simulated hour per real minute). Nights and weekends are skipped, so there is always traffic.
  Stream timestamps can therefore be ahead of the real date; Gold must use the latest data date, not `CURRENT_DATE`.
- Every 60 real seconds: pending readings go to a small file and are uploaded with PUT. **PUT needs no warehouse.**
- Every 10 real minutes (minimum 5): COPY INTO loads the new files. This is the only step that wakes the warehouse.
  The COPY statements are read from `sql/02_bronze.sql`, so there is one definition for backfill and stream.
- Guardrails: stops itself after `--minutes` (default 30, max 180), then loads what is left and suspends the warehouse.
  Ctrl+C does the same. About 0.2 credits per streaming hour.
- Refuels during the stream produce fuel transactions. No maintenance happens during the stream, so faults and overdue services build up.
- Same injected problems as the backfill (missing pings, odometer glitches), appended to `data/stream/_dq_manifest.csv`.
- State is saved after every upload in `data/stream/_stream_state.json`; the next session continues the timeline.
  Regenerating the backfill makes the stream start fresh. `--no-upload` writes local files only.

## Business logic (Gold)
1. **Maintenance priority score** per vehicle: distance and engine hours since last service,
   active diagnostic codes, fuel efficiency drifting worse than the vehicle's own baseline. Ranked per client.
2. **Financing candidates**:
   - Client-owned vehicles in good condition with strong resale value → purchase leaseback candidates
   - Leased vehicles on track to exceed their distance allowance → restructure before overage
   - Leases near end date → renewal or replacement decision
3. **Replacement timing**: rolling cost per km (fuel + maintenance + depreciation) versus falling resale value;
   flag vehicles past the point where running costs rise faster than resale value falls.
   **Backtest** retired vehicles: replaced too early, too late, or about right?

## Project layout
```
fleet-project/
├── CLAUDE.md
├── generator/
│   ├── backfill.py
│   ├── load_bronze.py    # PUT files to @LANDING, run 02_bronze.sql, suspend warehouse
│   ├── snowflake_conn.py # shared connection from .env; run it to test the connection
│   └── stream.py
├── sql/
│   ├── 01_setup.sql      # resource monitor, role, warehouse, database, schemas, stage
│   ├── 02_bronze.sql     # file formats, raw TRANSIENT tables (all VARCHAR + source metadata), COPY INTO
│   ├── 03_silver.sql     # dedup, validation, quarantine, FLATTEN, streams + tasks
│   └── 04_gold.sql       # the three business questions
├── data/                 # generated files, gitignored
│   ├── backfill/<table>/ # one folder per table, same layout as @LANDING/<table>/
│   └── stream/<table>/   # live stream files, uploaded to the same @LANDING/<table>/ folders
├── requirements.txt
├── .venv/                # Python virtual environment, gitignored
├── .env                  # Snowflake settings, gitignored
├── .env.example          # template for .env, committed
├── .gitignore
└── README.md
```

## Build order
1. Setup SQL with cost guardrails — written (`sql/01_setup.sql`), run in Snowsight
2. Backfill generator — written and validated (`generator/backfill.py`)
   - Bronze load — done (`generator/load_bronze.py` + `sql/02_bronze.sql`); row counts match the generator, re-runs load nothing
3. Live stream simulator — done (`generator/stream.py`); tested end to end, stream rows load into the same Bronze tables
4. Silver layer (data quality, quarantine, FLATTEN, streams + tasks)
5. Gold layer (three business questions)
6. Dashboard + README (problem, architecture, findings)

## Conventions
- Python 3.9+ in `.venv` (`pip install -r requirements.txt`): faker, numpy, pandas, snowflake-connector-python, python-dotenv, tzdata
- Column names carry their unit: `_km`, `_hours`, `_cad`, `_pct`, `_litres` or `litres`, `_ts` for UTC timestamps, `_date` for dates.
- Credentials only in `.env`, never committed. `data/` and `.env` in `.gitignore`.
- Python connects with **key-pair authentication** as role `FLEET_ENGINEER`: no password stored, no MFA prompt.
  The private key lives outside the project (`~/.snowflake/`); `*.p8` and `*.pem` are gitignored as a backstop.
- Every SQL script is safe to re-run: `CREATE ... IF NOT EXISTS`, then `ALTER` to re-apply settings.
- Commit to GitHub after each step so work survives the trial ending.
- Owner's background: Databricks, Azure Fabric, dbt, Spark. Explain Snowflake concepts by mapping them to Databricks equivalents.
