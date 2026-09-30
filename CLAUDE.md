# Fleet Analytics on Snowflake — Project Brief

## Goal
Portfolio project simulating a fleet management + fleet financing company's data on Snowflake,
answering three business questions:

1. **Maintenance priority** — which vehicles need maintenance first?
2. **Financing opportunities** — which vehicles are candidates for a purchase leaseback or lease restructuring?
3. **Replacement timing** — are vehicles being replaced at the right time?

All data is synthetic. Describe it as a portfolio project with simulated data, never as real company data.

## Hard constraints (Snowflake free trial)
- 30-day trial with a limited free credit balance. Never add a credit card.
- Trial accounts have **no external network access**: Snowflake cannot call outside APIs.
  All data is generated locally in Python and pushed in (PUT to internal stage, then COPY INTO).
- Only running warehouses consume credits.

### Cost guardrails (set up before anything else)
- One X-SMALL warehouse, `AUTO_SUSPEND = 60`, `AUTO_RESUME = TRUE`.
- A resource monitor that suspends the warehouse at a fixed credit quota.
- Tasks scheduled every 10–15 minutes, not every minute. Suspend tasks when not working.
- Run the live stream only while actively developing, never overnight.
- Keep data small: ~5 clients, ~200 vehicles, ~2 years of history.
- Use Snowsight dashboards for visuals (no extra tools).
- For every task, check its stream for new data before it runs. 

## Architecture
Python simulator (laptop) → internal stage → BRONZE (raw) → SILVER (clean, validated) → GOLD (business answers) → Snowsight dashboard

- **Backfill** (`generator/backfill.py`): ~2 years of history for all tables, written as CSV/JSON.
- **Live stream** (`generator/stream.py`): telematics events every few seconds, micro-batched into small files, PUT + COPY INTO.
- **Incremental processing**: Snowflake Streams + Tasks move only new rows Bronze → Silver → Gold.
- **Idempotency**: While ingestion, make sure that the pipeline is landing only the data that has changes and not the data that is duplicated.
-**Handling Bad Data**: For each issue or bad data arriving based on quality checks, quarantine that data with a reason column and make sure after checking that corrected data should be able to add in the right table.
-**Consistency**: The units, classifications and the constant values if used should be consistent everywhere across the pipeline. You can use Kilometers for covering distance and hours for covering time
- Telematics lands as JSON in a VARIANT column; Silver uses FLATTEN.

## Data model
| Table | Key | Links to | Contents |
|---|---|---|---|
| clients | client_id | — | company, industry (construction, HVAC, electrical), seasonal revenue pattern |
| vehicles | vehicle_id | client_id | VIN, make, model, year, acquisition cost/date, ownership (owned/leased), **status (active/retired), disposal_date, disposal_price** (both NULL while active) |
| lease_contracts | contract_id | vehicle_id | term, start date, monthly payment, mileage allowance, residual value, end date |
| telematics_events (streaming, JSON) | event_id | vehicle_id | event timestamp, odometer, engine hours, fuel level, speed, idle time, diagnostic trouble codes |
| fuel_transactions | transaction_id | vehicle_id | **transaction timestamp**, fuel card purchases: litres, amount, station |
| maintenance_events | maintenance_id | vehicle_id | **service timestamp, odometer at service, engine hours at service**, service type, cost, vendor, downtime hours |
| resale_values | make + model + year + mileage band + month | — | monthly market value by make/model/year/mileage band |

- Retired vehicles keep their full history so the replacement backtest can compare the actual disposal date against the modelled best date.
- Maintenance rows carry the odometer and engine hours at service, so "distance since last service" is a subtraction, not a time-based join to telematics.
- Snowflake does not enforce primary or foreign keys (only NOT NULL), just like informational constraints in Databricks. Silver must check them itself: rows whose vehicle_id or client_id has no match go to quarantine as orphans.

### Deliberate data quality issues (Silver must catch them)
- Duplicate fuel transactions
- Missing telematics pings
- Odometer readings that go backward
- Values mismatch if there is any classification. For example, if the type is classified as car, truck etc, it should be the same across all classifications. Using an enum to classify would help that.

## Business logic (Gold)
1. **Maintenance priority score** per vehicle: distance and engine hours since last service,
   active diagnostic codes, fuel efficiency drifting worse than the vehicle's own baseline. Ranked per client.
2. **Financing candidates**:
   - Client-owned vehicles in good condition with strong resale value → purchase leaseback candidates
   - Leased vehicles on track to exceed mileage allowance → restructure before overage
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
│   └── stream.py
├── sql/
│   ├── 01_setup.sql      # warehouse, resource monitor, database, schemas, stage
│   ├── 02_bronze.sql
│   ├── 03_silver.sql     # dedup, validation, FLATTEN, streams + tasks
│   └── 04_gold.sql       # the three business questions
├── data/                 # generated files, gitignored
├── .env                  # Snowflake credentials, gitignored
└── README.md
```

## Build order
1. Setup SQL with cost guardrails
2. Backfill generator
3. Live stream simulator
4. Silver layer (data quality, FLATTEN, streams + tasks)
5. Gold layer (three business questions)
6. Dashboard + README (problem, architecture, findings)

## Conventions
- Python 3.9+, libraries: faker, numpy, pandas, snowflake-connector-python, python-dotenv
- Credentials only in `.env`, never committed. `data/` and `.env` in `.gitignore`.
- Commit to GitHub after each step so work survives the trial ending.
- Owner's background: Databricks, Azure Fabric, dbt, Spark. Explain Snowflake concepts by mapping them to Databricks equivalents.

### Resource monitor (decided 2026-09-30)
- Edition: Standard, about $2.00 per credit.
- Trial started with $360 of free usage; $355.90 left on 2026-09-30.
- A credit card is on file, so overage may be billed.
- One account-level resource monitor covering all warehouses.
- CREDIT_QUOTA = 40 (about $80 max), FREQUENCY = NEVER.
- Triggers: notify at 50% and 75%, SUSPEND at 90%, SUSPEND_IMMEDIATE at 100%.
- A Budget with email alerts for serverless and AI spend, which the monitor can't stop.
- Raising the quota is a deliberate decision, recorded here with a date.
- Standard edition: no materialized views, no masking policies, Time Travel max 1 day.
