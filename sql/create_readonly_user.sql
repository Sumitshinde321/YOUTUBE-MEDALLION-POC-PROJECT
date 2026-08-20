-- Create read-only role chatbot_readonly
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'chatbot_readonly') THEN
        CREATE ROLE chatbot_readonly WITH LOGIN PASSWORD 'chatbot_readonly_pass';
    END IF;
END
$$;

-- Grant usage on schemas
GRANT USAGE ON SCHEMA bronze, silver, gold, metadata TO chatbot_readonly;

-- Grant select on all existing tables in schemas
GRANT SELECT ON ALL TABLES IN SCHEMA bronze, silver, gold, metadata TO chatbot_readonly;

-- Ensure future tables created in these schemas are also readable
ALTER DEFAULT PRIVILEGES IN SCHEMA bronze, silver, gold, metadata GRANT SELECT ON TABLES TO chatbot_readonly;
