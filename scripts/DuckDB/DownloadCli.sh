#!/bin/bash

rm -f ./duckdb
rm -f ./duckdb_cli-*

wget -q https://github.com/duckdb/duckdb/releases/download/v0.6.1/duckdb_cli-linux-amd64.zip
unzip -qq duckdb_cli-*.zip
rm -f duckdb_cli-*.zip