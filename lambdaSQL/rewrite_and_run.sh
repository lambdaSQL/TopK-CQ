#!/bin/bash

python="/path/to/python3"
duckdb="/path/to/duckdb"

result_file="rewrite_and_run.result"
touch $result_file

query=$1
b=$2
m=$3
g="D"
rewrite="query/$query/rewrite0.txt"

$python main.py "$query" -b "$b" -m "$m" -g "$g" >> $result_file

out_file="rewrite_and_run.out"
rm -f $out_file
touch $out_file
$duckdb -c ".mode csv" -c ".read query/load_graph.sql" -c ".timer on" -c ".read ${rewrite}" | grep "Run Time (s): real" >> $out_file

awk 'BEGIN{sum=0;}{sum+=$5;} END{printf "Exec time(s): %f\n", sum;}' $out_file >> $result_file

echo "======================" >> $result_file
