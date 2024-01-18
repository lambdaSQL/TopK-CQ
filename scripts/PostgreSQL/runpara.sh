#!/bin/bash

for i in "8" "4" "2"
do
	query=${i}
	echo $query
	(time ./ExecuteTopK.sh L4ProductK 1024 twitter $query) 2>&1
#	(time ./ExecuteTopK.sh L4Original 1024 twitter $query) 2>&1
done
