-- Create a dedicated user for Metabase
CREATE USER metabase WITH PASSWORD 'mysecretpassword';

-- Create a database for Metabase and assign ownership
CREATE DATABASE metabaseappdb OWNER metabase;

-- Grant privileges on the Metabase database
GRANT ALL PRIVILEGES ON DATABASE metabaseappdb TO metabase;

-- Connect to the Metabase database to set schema permissions
\c metabaseappdb;

-- Give Metabase user full access on the public schema
GRANT ALL PRIVILEGES ON SCHEMA public TO metabase;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO metabase;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO metabase;
GRANT CREATE ON SCHEMA public TO metabase;

-- Ensure future tables and sequences also have privileges
ALTER DEFAULT PRIVILEGES IN SCHEMA public
GRANT ALL ON TABLES TO metabase;
ALTER DEFAULT PRIVILEGES IN SCHEMA public
GRANT ALL ON SEQUENCES TO metabase;
