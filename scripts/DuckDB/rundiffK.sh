#!/bin/bash

for i in "8" "64" "256" "4096" "16384" "32768"
do
	query=${i}
	echo $query
	(time ./TopKQuery.sh L332_$query $query twitter 1) 2>&1
	(time ./TopKQuery.sh L3ProductK $query twitter) 2>&1
done
