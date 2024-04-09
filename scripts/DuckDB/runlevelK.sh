#!/bin/bash

for i in "dblp" "epinions" "wiki_vote" "twitter" "bitcoin"
do
	query=${i}
	echo $query
	(time ./TopKQuery.sh L232 1024 $query 1) 2>&1
	(time ./TopKQuery.sh L332 1024 $query 1) 2>&1
	(time ./TopKQuery.sh L432 1024 $query 1) 2>&1
done
