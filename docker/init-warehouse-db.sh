#!/bin/bash
set -e

# The default postgres container only creates POSTGRES_DB (the "airflow"
# metadata database). This adds a second database + role for our own
# warehouse tables, kept separate from Airflow's internal bookkeeping.
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE USER warehouse WITH PASSWORD 'warehouse';
    CREATE DATABASE warehouse OWNER warehouse;
EOSQL
