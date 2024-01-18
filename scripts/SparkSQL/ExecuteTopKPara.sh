#!/bin/bash

SCRIPT=$(readlink -f $0)
SCRIPT_PATH=$(dirname "${SCRIPT}")
PARENT_PATH=$(dirname "${SCRIPT_PATH}")

source "${PARENT_PATH}/common.sh"
source "${PARENT_PATH}/env.sh"

config_files=("${PARENT_PATH}/config.properties")

function execute_sparksql {
    spark_home=$(prop ${config_files} 'spark.home')
    sparksql="spark-sql"
    datapath=$(prop ${config_files} 'postgresql.datapath')

    input_graph=$3
    input_query=$1
    input_k=$2
    query_template="${SCRIPT_PATH}/QueryTemplate/${input_query}.sql"
    core=$4
    parallelism=$((core * 2))
	  #ran=$RANDOM

	  log_path="${PARENT_PATH}/log"
	  mkdir -p ${log_path}
  
	  log_file="${log_path}/sparksql_${input_query}_${input_k}_${input_graph}.log"
	  result_file="${log_path}/sparksql_${input_query}_${input_k}_${input_graph}.result"
    # create query file under tmp path
    tmp_path=$(prop ${config_files} 'common.tmp.path')
    timeout_time="8h"

    submit_query="${tmp_path}/sparksql_query_${ran}.sql"
    rm -f "${submit_query}"
    touch "${submit_query}"
    echo "create table graph (src bigint, dst bigint, rating bigint) USING CSV LOCATION '${datapath}/${input_graph}.csv' OPTIONS (DELIMITER '|');" >> ${submit_query}
    sed 's/#K/'"${input_k}"'/g' ${query_template} >> ${submit_query}
    echo ';' >> ${submit_query}
#	  cat ${query_template} >> ${submit_query}

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
        rm -rf ./metastore_db
	COMMAND="timeout -s SIGKILL ${timeout_time} ${sparksql} --master local[${core}] --driver-memory 256G --executor-memory 128G --conf spark.shuffle.service.removeShuffle=true --conf spark.executor.cores=${core} --conf spark.default.parallelism=${parallelism} --conf spark.cores.max=${core} -f ${submit_query}" 	
	$COMMAND >> ${log_file} 2>&1

        status_code=$?
        if [[ ${status_code} -eq 1 ]]; then
            print "sparksql task timed out."
        elif [[ ${status_code} -ne 0 ]]; then
            print "sparksql task failed."
        else
            extracted_time=$(tail -n 20 ${log_file} | grep "Time taken: " | tail -n 1 | sed -rn 's/^.*Time taken:\s*(\S+).*$/\1/p')
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
	  execute_sparksql ${input_query} ${input_k} ${input_graph} $4
else
	  execute_sparksql ${input_query} ${input_k} ${input_graph} 1
fi
