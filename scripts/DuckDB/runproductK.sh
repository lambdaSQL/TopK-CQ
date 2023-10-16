#!/bin/bash

for i in "bitcoin" "dblp" "epinions" "wiki_vote" "twitter"
do
	query=${i}
	echo $query
	(time ./TopKQuery.sh L2ProductK 1024 $query) 2>&1
	(time ./TopKQuery.sh L3ProductK 1024 $query) 2>&1
	(time ./TopKQuery.sh L4ProductK 1024 $query) 2>&1
	(time ./TopKQuery.sh StarProductK 1024 $query) 2>&1
	(time ./TopKQuery.sh TreeProductK 1024 $query) 2>&1
done
