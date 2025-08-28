#!/bin/bash
set -e

clickhouse-client -n <<-EOSQL
    CREATE DATABASE IF NOT EXISTS coffee_bean_sales;
    CREATE DATABASE IF NOT EXISTS raw;
    CREATE DATABASE IF NOT EXISTS intermediate;
    CREATE DATABASE IF NOT EXISTS marts;
    CREATE DATABASE IF NOT EXISTS staging;
EOSQL
