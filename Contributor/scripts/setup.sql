-- This is the readme file for the Contributor app.

CREATE APPLICATION ROLE IF NOT EXISTS Unified_Snowspace_Contributor;
CREATE SCHEMA IF NOT EXISTS core;
GRANT USAGE ON SCHEMA core TO APPLICATION ROLE Unified_Snowspace_Contributor;

CREATE OR ALTER VERSIONED SCHEMA code_schema;
GRANT USAGE ON SCHEMA code_schema TO APPLICATION ROLE Unified_Snowspace_Contributor;

CREATE STREAMLIT IF NOT EXISTS code_schema.contributor_streamlit
  FROM '/streamlit'
  MAIN_FILE = '/streamlit_app.py'
;

GRANT USAGE ON STREAMLIT code_schema.contributor_streamlit TO APPLICATION ROLE Unified_Snowspace_Contributor;