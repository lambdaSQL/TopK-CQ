#!/bin/bash

SCRIPT=$(readlink -f $0)
SCRIPT_PATH=$(dirname "${SCRIPT}")
PARENT_PATH=$(dirname "${SCRIPT_PATH}")

source "${PARENT_PATH}/common.sh"
source "${PARENT_PATH}/env.sh"

mkdir -p "${SCRIPT_PATH}/Data/"
cp -f ${PARENT_PATH}/SparkSQLTest/Data/*.csv "${SCRIPT_PATH}/Data/"