/* =============================================================================
   01_setup.sql  -  Cost guardrails, security and containers
   -----------------------------------------------------------------------------
   Run in a Snowsight worksheet, top to bottom, with "Run All".
   Safe to re-run: every object uses IF NOT EXISTS, and settings are
   re-applied with ALTER so a re-run always converges to the same state.

   Nothing in this file needs a running warehouse. It is all metadata work,
   which Snowflake handles in its cloud services layer, so it costs ~0 credits.

   Databricks mapping
     Resource monitor   ~ budget policy that can actually STOP compute
     Warehouse          ~ SQL warehouse (AUTO_SUSPEND ~ auto-termination)
     Role + grants      ~ Unity Catalog groups + GRANTs
     Database.Schema    ~ Catalog.Schema
     Internal stage     ~ Unity Catalog Volume used as a landing zone
   ============================================================================= */


/* -----------------------------------------------------------------------------
   1. Resource monitor: one hard credit cap for the whole account
   -----------------------------------------------------------------------------
   IF NOT EXISTS, never OR REPLACE: replacing a monitor creates a new one
   whose used-credit counter starts at zero, silently resetting the cap.

   FREQUENCY = NEVER: one total cap for the life of the trial. MONTHLY would
   hand out a fresh 40 credits on the 1st of each month.

   NOTIFY emails go to account admins who have turned on notifications in
   Snowsight: your profile menu > Settings > Notifications.
   Resource monitors only watch warehouses. Serverless features and Cortex AI
   are not covered; the Budget (set up in the UI) alerts on those.
----------------------------------------------------------------------------- */
USE ROLE ACCOUNTADMIN;

CREATE RESOURCE MONITOR IF NOT EXISTS FLEET_RM
  WITH CREDIT_QUOTA    = 40            -- ~$80 at $2/credit (Standard edition)
       FREQUENCY       = NEVER
       START_TIMESTAMP = IMMEDIATELY
  TRIGGERS
    ON 50  PERCENT DO NOTIFY
    ON 75  PERCENT DO NOTIFY
    ON 90  PERCENT DO SUSPEND            -- let running queries finish
    ON 100 PERCENT DO SUSPEND_IMMEDIATE; -- kill running queries

-- Attach at account level so it covers every warehouse, including COMPUTE_WH.
ALTER ACCOUNT SET RESOURCE_MONITOR = FLEET_RM;


/* -----------------------------------------------------------------------------
   2. Tighten the trial's default warehouse
   -----------------------------------------------------------------------------
   COMPUTE_WH came with the trial and already spent $4.10. We won't use it,
   but if something wakes it up, it should go back to sleep after 60 seconds.
----------------------------------------------------------------------------- */
ALTER WAREHOUSE IF EXISTS COMPUTE_WH SET
  AUTO_SUSPEND = 60
  AUTO_RESUME  = TRUE;


/* -----------------------------------------------------------------------------
   3. A working role for the project
   -----------------------------------------------------------------------------
   Daily work should not happen as ACCOUNTADMIN, the same way you would not
   run jobs as a Databricks account admin.

   FLEET_ENGINEER gets:
     - CREATE DATABASE : it will own FLEET_DB and everything inside it
     - EXECUTE TASK    : it can run tasks on a warehouse
   It deliberately does NOT get:
     - CREATE WAREHOUSE / MODIFY on the warehouse : it cannot resize compute
     - EXECUTE MANAGED TASK : no serverless tasks, which the monitor can't stop
----------------------------------------------------------------------------- */
CREATE ROLE IF NOT EXISTS FLEET_ENGINEER
  COMMENT = 'Working role for the fleet analytics portfolio project';

-- Role hierarchy best practice: custom roles roll up to SYSADMIN.
GRANT ROLE FLEET_ENGINEER TO ROLE SYSADMIN;

GRANT CREATE DATABASE ON ACCOUNT TO ROLE FLEET_ENGINEER;
GRANT EXECUTE TASK    ON ACCOUNT TO ROLE FLEET_ENGINEER;

-- Grant the role to whoever is running this script.
-- EXECUTE IMMEDIATE lets us build the statement from CURRENT_USER().
SET grant_role_sql = 'GRANT ROLE FLEET_ENGINEER TO USER "' || CURRENT_USER() || '"';
EXECUTE IMMEDIATE $grant_role_sql;


/* -----------------------------------------------------------------------------
   4. The project warehouse
   -----------------------------------------------------------------------------
   Owned by SYSADMIN; FLEET_ENGINEER may only use and start/stop it.
   CREATE ... IF NOT EXISTS does not update an existing warehouse, so the
   ALTER below re-applies the settings on every run.

   STATEMENT_TIMEOUT_IN_SECONDS kills any single query running over 15 minutes.
   Nothing in this project should take more than a minute on X-Small.
----------------------------------------------------------------------------- */
USE ROLE SYSADMIN;

CREATE WAREHOUSE IF NOT EXISTS FLEET_WH
  WAREHOUSE_SIZE      = XSMALL
  AUTO_SUSPEND        = 60
  AUTO_RESUME         = TRUE
  INITIALLY_SUSPENDED = TRUE
  COMMENT = 'Fleet analytics project. X-Small only. Do not resize.';

ALTER WAREHOUSE FLEET_WH SET
  WAREHOUSE_SIZE               = XSMALL
  AUTO_SUSPEND                 = 60
  AUTO_RESUME                  = TRUE
  STATEMENT_TIMEOUT_IN_SECONDS = 900;

GRANT USAGE, OPERATE ON WAREHOUSE FLEET_WH TO ROLE FLEET_ENGINEER;


/* -----------------------------------------------------------------------------
   5. Database, medallion schemas and landing stage
   -----------------------------------------------------------------------------
   Created as FLEET_ENGINEER so the role owns them outright.

   DATA_RETENTION_TIME_IN_DAYS = 1 is the Standard edition maximum for
   Time Travel (like Delta time travel with a 1-day retention).

   The stage is where PUT uploads files from the laptop. Files are organised
   by folder per table, e.g. @LANDING/telematics_events/.
----------------------------------------------------------------------------- */
USE ROLE FLEET_ENGINEER;

CREATE DATABASE IF NOT EXISTS FLEET_DB
  DATA_RETENTION_TIME_IN_DAYS = 1
  COMMENT = 'Fleet analytics portfolio project. All data is synthetic.';

CREATE SCHEMA IF NOT EXISTS FLEET_DB.BRONZE COMMENT = 'Raw data exactly as landed';
CREATE SCHEMA IF NOT EXISTS FLEET_DB.SILVER COMMENT = 'Cleaned, validated, deduplicated; plus quarantine tables';
CREATE SCHEMA IF NOT EXISTS FLEET_DB.GOLD   COMMENT = 'Business answers and scoring config';

CREATE STAGE IF NOT EXISTS FLEET_DB.BRONZE.LANDING
  COMMENT = 'Internal stage. Python generators PUT files here, COPY INTO loads them.';

USE WAREHOUSE FLEET_WH;
USE SCHEMA FLEET_DB.BRONZE;


/* -----------------------------------------------------------------------------
   6. Verify
   -----------------------------------------------------------------------------
   Check each result:
     - FLEET_RM: credit_quota 40, frequency NEVER, level ACCOUNT
     - FLEET_WH: size X-Small, auto_suspend 60, state SUSPENDED
     - Schemas BRONZE, SILVER, GOLD exist; stage LANDING exists
----------------------------------------------------------------------------- */
USE ROLE ACCOUNTADMIN;
SHOW RESOURCE MONITORS LIKE 'FLEET_RM';
SHOW WAREHOUSES;

USE ROLE FLEET_ENGINEER;
SHOW SCHEMAS IN DATABASE FLEET_DB;
SHOW STAGES  IN SCHEMA   FLEET_DB.BRONZE;
SHOW GRANTS  TO ROLE     FLEET_ENGINEER;


/* -----------------------------------------------------------------------------
   Emergency stop (run manually if spend looks wrong)
----------------------------------------------------------------------------- */
-- ALTER WAREHOUSE FLEET_WH SUSPEND;
-- ALTER WAREHOUSE COMPUTE_WH SUSPEND;
-- Tasks get their own SUSPEND lines once 03_silver.sql creates them.
