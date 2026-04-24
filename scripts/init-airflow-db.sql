-- Creates the Airflow metadata database on first postgres container start.
-- Our pipeline data lives in POSTGRES_DB (set via env); Airflow gets its own DB.
SELECT 'CREATE DATABASE airflow'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'airflow')\gexec
