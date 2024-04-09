#!/bin/bash

for i in "dblp_2" "dblp_8" "dblp_32" "dblp_128"
do
	query=${i}
	echo $query
	(time ./TopKQuery.sh L332 1024 $query 1) 2>&1
	(time ./TopKQuery.sh L432 1024 $query 1) 2>&1
	(time ./TopKQuery.sh L3ProductK 1024 $query) 2>&1
	(time ./TopKQuery.sh L4ProductK 1024 $query) 2>&1
	(time ./TopKQuery.sh L3Original 1024 $query) 2>&1
	(time ./TopKQuery.sh L4Original 1024 $query) 2>&1
done
