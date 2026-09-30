/* =============================================================================
   02_bronze.sql  -  Raw tables and loads from the landing stage
   -----------------------------------------------------------------------------
   Normally run by generator/load_bronze.py, which first uploads (PUT) the files
   to @LANDING/<table>/ and then executes this whole file.
   It can also be run in a Snowsight worksheet once the files are in the stage.

   Safe to re-run:
     - Tables use IF NOT EXISTS.
     - COPY INTO remembers which files it already loaded into each table
       (for 64 days) and skips them. Re-running loads only new files.
       Databricks equivalent: the file tracking in COPY INTO / Auto Loader.

   Bronze design rules
     1. Store exactly what arrived. No cleaning, no dedup, no filtering:
        bad rows must survive so Silver can quarantine them with a reason.
     2. Every CSV column is VARCHAR. A bad date or number can never make a
        load fail; Silver casts with TRY_CAST and quarantines what fails.
     3. Every row records where it came from: source file, row number, load time.
     4. TRANSIENT tables: no 7-day Fail-safe storage. Bronze can always be
        rebuilt from the files in the stage, so paying for Fail-safe is waste.
   ============================================================================= */

USE ROLE FLEET_ENGINEER;
USE WAREHOUSE FLEET_WH;
USE SCHEMA FLEET_DB.BRONZE;


/* -----------------------------------------------------------------------------
   File formats
   -----------------------------------------------------------------------------
   OR REPLACE is fine here: a file format holds no data or load history.
   TRIM_SPACE = FALSE on purpose: 'van ' with a trailing space is one of the
   dirty values Silver has to catch, so Bronze must keep it.
----------------------------------------------------------------------------- */
CREATE OR REPLACE FILE FORMAT FF_CSV
  TYPE = CSV
  SKIP_HEADER = 1
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  EMPTY_FIELD_AS_NULL = TRUE
  TRIM_SPACE = FALSE
  ENCODING = 'UTF8';

-- One JSON object per line (NDJSON). Each object is one vehicle-day batch.
CREATE OR REPLACE FILE FORMAT FF_JSON
  TYPE = JSON
  STRIP_OUTER_ARRAY = FALSE;


/* -----------------------------------------------------------------------------
   Tables
----------------------------------------------------------------------------- */
CREATE TRANSIENT TABLE IF NOT EXISTS CLIENTS (
  client_id VARCHAR, company_name VARCHAR, industry VARCHAR, city VARCHAR,
  province VARCHAR, seasonal_pattern VARCHAR, client_since VARCHAR,
  _source_file VARCHAR, _source_row NUMBER, _loaded_at TIMESTAMP_NTZ
);

CREATE TRANSIENT TABLE IF NOT EXISTS VEHICLES (
  vehicle_id VARCHAR, client_id VARCHAR, vin VARCHAR, make VARCHAR, model VARCHAR,
  model_year VARCHAR, vehicle_type VARCHAR, fuel_type VARCHAR, ownership VARCHAR,
  acquisition_date VARCHAR, acquisition_cost_cad VARCHAR, status VARCHAR,
  disposal_date VARCHAR, disposal_reason VARCHAR, disposal_price_cad VARCHAR,
  _source_file VARCHAR, _source_row NUMBER, _loaded_at TIMESTAMP_NTZ
);

CREATE TRANSIENT TABLE IF NOT EXISTS LEASE_CONTRACTS (
  contract_id VARCHAR, vehicle_id VARCHAR, start_date VARCHAR, end_date VARCHAR,
  term_months VARCHAR, monthly_payment_cad VARCHAR, distance_allowance_km VARCHAR,
  excess_km_rate_cad VARCHAR, residual_value_cad VARCHAR,
  _source_file VARCHAR, _source_row NUMBER, _loaded_at TIMESTAMP_NTZ
);

CREATE TRANSIENT TABLE IF NOT EXISTS DTC_CODES (
  dtc_code VARCHAR, description VARCHAR, severity VARCHAR,
  _source_file VARCHAR, _source_row NUMBER, _loaded_at TIMESTAMP_NTZ
);

CREATE TRANSIENT TABLE IF NOT EXISTS RESALE_VALUES (
  make VARCHAR, model VARCHAR, model_year VARCHAR, distance_band VARCHAR,
  band_min_km VARCHAR, band_max_km VARCHAR, valuation_month VARCHAR, market_value_cad VARCHAR,
  _source_file VARCHAR, _source_row NUMBER, _loaded_at TIMESTAMP_NTZ
);

CREATE TRANSIENT TABLE IF NOT EXISTS MAINTENANCE_EVENTS (
  maintenance_id VARCHAR, vehicle_id VARCHAR, service_ts VARCHAR, service_type VARCHAR,
  odometer_km VARCHAR, engine_hours VARCHAR, vendor VARCHAR, cost_cad VARCHAR,
  downtime_hours VARCHAR, resolved_dtc VARCHAR, description VARCHAR,
  _source_file VARCHAR, _source_row NUMBER, _loaded_at TIMESTAMP_NTZ
);

CREATE TRANSIENT TABLE IF NOT EXISTS FUEL_TRANSACTIONS (
  transaction_id VARCHAR, vehicle_id VARCHAR, fuel_card_id VARCHAR, transaction_ts VARCHAR,
  station_name VARCHAR, station_city VARCHAR, station_province VARCHAR, fuel_type VARCHAR,
  litres VARCHAR, price_per_litre_cad VARCHAR, amount_cad VARCHAR,
  _source_file VARCHAR, _source_row NUMBER, _loaded_at TIMESTAMP_NTZ
);

-- One row per vehicle-day batch, kept as raw JSON. Silver FLATTENs the readings.
CREATE TRANSIENT TABLE IF NOT EXISTS TELEMATICS_EVENTS (
  record VARIANT,
  _source_file VARCHAR, _source_row NUMBER, _loaded_at TIMESTAMP_NTZ
);


/* -----------------------------------------------------------------------------
   Loads
   -----------------------------------------------------------------------------
   $1, $2, ... are the columns of the file, in order.
   METADATA$FILENAME and METADATA$FILE_ROW_NUMBER come free with every COPY.
   SYSDATE() is the load time in UTC.
----------------------------------------------------------------------------- */
COPY INTO CLIENTS FROM (
  SELECT $1, $2, $3, $4, $5, $6, $7,
         METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, SYSDATE()
  FROM @LANDING/clients/
) FILE_FORMAT = (FORMAT_NAME = 'FF_CSV');

COPY INTO VEHICLES FROM (
  SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15,
         METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, SYSDATE()
  FROM @LANDING/vehicles/
) FILE_FORMAT = (FORMAT_NAME = 'FF_CSV');

COPY INTO LEASE_CONTRACTS FROM (
  SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9,
         METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, SYSDATE()
  FROM @LANDING/lease_contracts/
) FILE_FORMAT = (FORMAT_NAME = 'FF_CSV');

COPY INTO DTC_CODES FROM (
  SELECT $1, $2, $3,
         METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, SYSDATE()
  FROM @LANDING/dtc_codes/
) FILE_FORMAT = (FORMAT_NAME = 'FF_CSV');

COPY INTO RESALE_VALUES FROM (
  SELECT $1, $2, $3, $4, $5, $6, $7, $8,
         METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, SYSDATE()
  FROM @LANDING/resale_values/
) FILE_FORMAT = (FORMAT_NAME = 'FF_CSV');

COPY INTO MAINTENANCE_EVENTS FROM (
  SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11,
         METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, SYSDATE()
  FROM @LANDING/maintenance_events/
) FILE_FORMAT = (FORMAT_NAME = 'FF_CSV');

COPY INTO FUEL_TRANSACTIONS FROM (
  SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11,
         METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, SYSDATE()
  FROM @LANDING/fuel_transactions/
) FILE_FORMAT = (FORMAT_NAME = 'FF_CSV');

COPY INTO TELEMATICS_EVENTS FROM (
  SELECT $1,
         METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, SYSDATE()
  FROM @LANDING/telematics_events/
) FILE_FORMAT = (FORMAT_NAME = 'FF_JSON');


/* -----------------------------------------------------------------------------
   Verify: row counts should match the backfill summary
   (telematics counts vehicle-day batches, not individual readings)
----------------------------------------------------------------------------- */
SELECT 'CLIENTS' AS table_name, COUNT(*) AS row_count FROM CLIENTS
UNION ALL SELECT 'VEHICLES', COUNT(*) FROM VEHICLES
UNION ALL SELECT 'LEASE_CONTRACTS', COUNT(*) FROM LEASE_CONTRACTS
UNION ALL SELECT 'DTC_CODES', COUNT(*) FROM DTC_CODES
UNION ALL SELECT 'RESALE_VALUES', COUNT(*) FROM RESALE_VALUES
UNION ALL SELECT 'MAINTENANCE_EVENTS', COUNT(*) FROM MAINTENANCE_EVENTS
UNION ALL SELECT 'FUEL_TRANSACTIONS', COUNT(*) FROM FUEL_TRANSACTIONS
UNION ALL SELECT 'TELEMATICS_EVENTS', COUNT(*) FROM TELEMATICS_EVENTS
ORDER BY table_name;
