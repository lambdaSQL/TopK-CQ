#!/bin/bash

SCRIPT=$(readlink -f $0)
SCRIPT_PATH=$(dirname "${SCRIPT}")
PARENT_PATH=$(dirname "${SCRIPT_PATH}")

source "${PARENT_PATH}/common.sh"
source "${PARENT_PATH}/env.sh"

config_files=("${PARENT_PATH}/config.properties")

function execute_duckdb {
    input_query=$1
    schema="topk"
    input_k=$2
    input_graph=$3
    if_null=$4
    num_parallel=$5
    #ran=$RANDOM
    query_template="${SCRIPT_PATH}/QueryTemplate/${input_query}.sql"
    schema_file="${SCRIPT_PATH}/QueryTemplate/load_${schema}.sql"

    # create query file under tmp path
    tmp_path=$(prop ${config_files} 'common.tmp.path')

    load_query="${tmp_path}/duckdb_load_${ran}.sql"
    submit_query="${tmp_path}/duckdb_query_${ran}.sql"
    rm -f "${load_query}"
    touch "${load_query}"
    rm -f "${submit_query}"
    touch "${submit_query}"

    #cat ${schema_file} >> ${load_query}
    sed 's/#GRAPH/'"${input_graph}"'/g' ${schema_file} >> ${load_query}
    if [[ ${if_null} -eq 0 ]] ; then 
	    echo "COPY (" >> ${submit_query} 
    fi
    sed 's/#K/'"${input_k}"'/g' ${query_template} >> ${submit_query}
    #cat ${query_template} >> ${submit_query}
    if [[ ${if_null} -eq 0 ]] ; then 
	    echo ") TO '/dev/null' (DELIMITER ',')" >> ${submit_query} 
    fi
    echo ";" >> ${submit_query}

    # NOTE: if the default glibc version ls lower than 2.23 in your environment,
    # uncomment the following commands to run duckdb with specified glibc
    #export LD_LIBRARY_PATH="/usr/local/GNU/glibc-2.34/:$LD_LIBRARY_PATH"
#	export LD_PRELOAD="/usr/local/GNU/glibc-2.34/lib/libc.so.6"
	#/usr/local/GNU/glibc-2.34/lib/ld-linux-x86-64.so.2 --library-path /usr/local/GNU/glibc-2.34/ 
	"${SCRIPT_PATH}/duckdb" -c ".mode csv" -c ".read ${load_query}" -c "SET threads TO ${num_parallel};" -c ".timer on" -c ".read ${submit_query}"
}

input_query=$1
input_k=$2
input_graph=$3
num_parallel=1
if [[ -n $5 ]]; then
	num_parallel=$5
fi
if [[ -n $4 ]]; then
	execute_duckdb ${input_query} ${input_k} ${input_graph} $4 ${num_parallel}
else
	execute_duckdb ${input_query} ${input_k} ${input_graph} 0 ${num_parallel}
fi
