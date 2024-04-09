#!/bin/bash

SCRIPT=$(readlink -f $0)
SCRIPT_PATH=$(dirname "${SCRIPT}")
PARENT_PATH=$(dirname "${SCRIPT_PATH}")

source "${PARENT_PATH}/common.sh"
source "${PARENT_PATH}/env.sh"

config_files=("${PARENT_PATH}/config.properties")

log_file="${PARENT_PATH}/log/load_data.log"

mkdir -p "${PARENT_PATH}/log"
rm -f ${log_file}
touch ${log_file}

postgresql_home=$(prop ${config_files} 'postgresql.home')
psql="${postgresql_home}/bin/psql"
database=$(prop ${config_files} 'postgresql.database')
username=$(prop ${config_files} 'postgresql.username')
port=$(prop ${config_files} 'postgresql.port')
host=$(prop ${config_files} 'postgresql.host')

create_tables_sql="${SCRIPT_PATH}/Utils/create_tables.sql"
${psql} "-d" "${database}" "-U" "${username}" "-p" "${port}" "-h" "${host}" "-f" "${create_tables_sql}" >> ${log_file} 2>&1

tmp_path=$(prop ${config_files} 'common.tmp.path')
load_data_sql="${tmp_path}/load_data.sql"
rm -f "${load_data_sql}"
touch "${load_data_sql}"

graph_tables=('epinions' 'google' 'wiki' 'dblp')
for table in ${graph_tables[@]}; do
    echo "DELETE FROM ${table};" >> ${load_data_sql}
    echo "COPY ${table} FROM '${PARENT_PATH}/Data/${table}.txt' DELIMITER ' ' CSV;" >> ${load_data_sql}
done

tpcds_tables=('customer' 'customer_address' 'customer_demographics' 'store_sales' 'web_sales' 'catalog_sales' 'date_dim')
for table in ${tpcds_tables[@]}; do
    echo "DELETE FROM ${table};" >> ${load_data_sql}
    echo "COPY ${table} FROM '${PARENT_PATH}/Data/${table}.csv' DELIMITER '|' CSV;" >> ${load_data_sql}
done

tpch_tables=('nation' 'part' 'supplier' 'partsupp')
for table in ${tpch_tables[@]}; do
    echo "DELETE FROM ${table};" >> ${load_data_sql}
    echo "COPY ${table} FROM '${PARENT_PATH}/Data/${table}.csv' DELIMITER '|' CSV;" >> ${load_data_sql}
done

${psql} "-d" "${database}" "-U" "${username}" "-p" "${port}" "-h" "${host}" "-f" "${load_data_sql}" >> ${log_file} 2>&1