#!/bin/bash

for i in "8" "64" "256" "4096" "16384" "32768"
do
	query=${i}
	echo $query
	(time ./ExecuteTopK.sh L3ProductK $query twitter ) 2>&1
	(time ./ExecuteTopK.sh L3Original $query twitter ) 2>&1
done
