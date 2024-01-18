#!/bin/bash

SCRIPT=$(readlink -f $0)
SCRIPT_PATH=$(dirname "${SCRIPT}")
PARENT_PATH=$(dirname "${SCRIPT_PATH}")

source "${PARENT_PATH}/common.sh"
source "${PARENT_PATH}/env.sh"

config_files=("${PARENT_PATH}/config.properties")

function execute_postgresql {
    postgresql_home=$(prop ${config_files} 'postgresql.home')
	  #psql="${postgresql_home}/bin/psql"
	  psql="psql"
    database=$(prop ${config_files} 'postgresql.database')
    username=$(prop ${config_files} 'postgresql.username')
    port=$(prop ${config_files} 'postgresql.port')
    host=$(prop ${config_files} 'postgresql.host')
    datapath=$(prop ${config_files} 'postgresql.datapath')

    input_graph=$3
    input_query=$1
    input_k=$2
    query_template="${SCRIPT_PATH}/QueryTemplate/${input_query}.sql"
    parallelism=$4
	  #ran=$RANDOM

	  log_path="${PARENT_PATH}/log"
	  mkdir -p ${log_path}
  
	  log_file="${log_path}/postgres_${input_query}_${input_k}_${input_graph}_${parallelism}.log"
	  result_file="${log_path}/postgres_${input_query}_${input_k}_${input_graph}_${parallelism}.result"
    # create query file under tmp path
    tmp_path=$(prop ${config_files} 'common.tmp.path')
    timeout_time=$(prop ${config_files} 'postgresql.statement.timeout')

    submit_query="${tmp_path}/postgres_query_${ran}.sql"
    rm -f "${submit_query}"
    touch "${submit_query}"

    echo "SET max_parallel_workers_per_gather=${parallelism};" >> ${submit_query}
    echo "SET max_parallel_workers=${parallelism};" >> ${submit_query}
	  echo "SET statement_timeout=${timeout_time};" >> ${submit_query}
	  echo "create extension pg_prewarm;" >> ${submit_query}
	  echo "drop table if exists graph cascade;" >> ${submit_query}
	  echo "create table graph (src bigint, dst bigint, rating bigint);" >> ${submit_query}
	  echo "copy graph(src, dst, rating) FROM '${datapath}/${input_graph}.csv' ( DELIMITER '|');" >> ${submit_query}
	  echo "select pg_prewarm('graph');" >> ${submit_query}
	  echo "\timing on" >> ${submit_query}
	  #echo "COPY (" >> ${submit_query}
    sed 's/#K/'"${input_k}"'/g' ${query_template} >> ${submit_query}
    echo ";" >> ${submit_query}
#	  cat ${query_template} >> ${submit_query}
#    echo ") TO '/dev/null' DELIMITER ',' CSV;" >> ${submit_query}

#	  sed -i "s/_graph_/${input_graph}/g"	${submit_query}

    rm -f "${log_file}"
    touch "${log_file}"
    rm -f "${result_file}"
    touch "${result_file}"

    print "execute sql: ${submit_query}"

    repeat_count=$(prop ${config_files} "common.experiment.repeat")
    echo "Start Task!"
    current_task=1
    while [[ ${current_task} -le ${repeat_count} ]]
    do
        ${psql} "-h" "${host}" "-d" "${database}" "-U" "${username}"\
        "-f" "${submit_query}" >> ${log_file} 2>&1

        status_code=$?
        if [[ ${status_code} -eq 1 ]]; then
            print "postgresql task timed out."
        elif [[ ${status_code} -ne 0 ]]; then
            print "postgresql task failed."
        else
            extracted_time=$(tail -n 20 ${log_file} | grep -A20 "COPY" | grep "Time: " | tail -n 1 | sed -rn 's/^.*Time:\s*(\S+).*$/\1/p')
            if [[ -n ${extracted_time} ]]; then
                execution_time=${extracted_time}
                print "${execution_time}"
            else
                print "extract execution_time failed."
            fi
        fi
        current_task=$(($current_task+1))
    done
}

function print {
    content=$1
    echo ${content} >> ${result_file}
}

input_graph=$3
input_query=$1
input_k=$2
parallelism=$4
if [[ -n $4 ]]; then
	  execute_postgresql ${input_query} ${input_k} ${input_graph} $4
else
	  execute_postgresql ${input_query} ${input_k} ${input_graph} 0
fi
