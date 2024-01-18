#!/bin/bash

for i in "40" "32" "16" "8" "4" "2" "1"
do
	echo $i
	#(time ./TopKQuery.sh L4ProductK 1024 twitter 0 $i) 2>&1
	(time ./TopKQuery.sh L4Original 1024 twitter 0 $i) 2>&1
done
