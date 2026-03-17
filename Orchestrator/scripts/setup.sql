-- This is the readme file for the Hello Snowflake app.

CREATE APPLICATION ROLE IF NOT EXISTS Unified_Snowspace_Orchestrator;
CREATE SCHEMA IF NOT EXISTS core;
GRANT USAGE ON SCHEMA core TO APPLICATION ROLE Unified_Snowspace_Orchestrator;

CREATE OR ALTER VERSIONED SCHEMA code_schema;
GRANT USAGE ON SCHEMA code_schema TO APPLICATION ROLE Unified_Snowspace_Orchestrator;

CREATE STREAMLIT IF NOT EXISTS code_schema.orchestrator_streamlit
  FROM '/streamlit'
  MAIN_FILE = '/streamlit_app.py'
;

GRANT USAGE ON STREAMLIT code_schema.orchestrator_streamlit TO APPLICATION ROLE Unified_Snowspace_Orchestrator;
