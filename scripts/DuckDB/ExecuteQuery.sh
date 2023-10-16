#!/bin/bash

SCRIPT=$(readlink -f $0)
SCRIPT_PATH=$(dirname "${SCRIPT}")
PARENT_PATH=$(dirname "${SCRIPT_PATH}")

source "${PARENT_PATH}/common.sh"
source "${PARENT_PATH}/env.sh"

config_files=("${PARENT_PATH}/config.properties")

input_query=$1
schema=$2

log_path="${PARENT_PATH}/log"
mkdir -p ${log_path}

log_file="${log_path}/duckdb_${schema}_${input_query}.log"
result_file="${log_path}/duckdb_${schema}_${input_query}.result"

touch $log_file
touch $result_file

function print {
    content=$1
    echo ${content} >> ${result_file}
}

repeat_count=$(prop ${config_files} "common.experiment.repeat")

echo "Start DuckDB Task!"
current_task=1
while [[ ${current_task} -le ${repeat_count} ]]
do
    timeout -s SIGKILL 8h bash "${SCRIPT_PATH}/Wrapper.sh" "${input_query}" "${schema}" >> "${log_file}"

    status_code=$?
    if [[ ${status_code} -eq 137 ]]; then
        print "duckdb task timed out."
    elif [[ ${status_code} -ne 0 ]]; then
        print "duckdb task failed."
    else
        time_taken_in_second=$(tail -n 10 ${log_file} | grep "Time " | tail -n 1 | sed -rn 's/^.*Time \(s\): real\s*(\S+).*$/\1/p')
        time_taken=$(echo "${time_taken_in_second}*1000" | bc)
        print "${time_taken}"
    fi
    current_task=$(($current_task+1))
done