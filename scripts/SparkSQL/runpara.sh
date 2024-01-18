#!/bin/bash

for i in "40" "32" "16" "8" "4" "2" "1"
do
	query=${i}
	echo $query
#	(time ./ExecuteTopK.sh L4ProductK 1024 twitter ${i}) 2>&1
	(time ./ExecuteTopK.sh L4Original 1024 twitter $i) 2>&1
done
