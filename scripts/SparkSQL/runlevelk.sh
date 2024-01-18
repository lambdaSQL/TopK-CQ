#!/bin/bash

for i in "bitcoin" "twitter" "dblp" "epinions" "wiki_vote"
do
	query=${i}
	echo $query
	(time ./ExecuteTopK.sh L232 1024 $query 40) 2>&1
	rm -r spark-warehouse
	rm -r metastore_db
	(time ./ExecuteTopK.sh L332 1024 $query 40) 2>&1
	rm -r spark-warehouse
	rm -r metastore_db
	(time ./ExecuteTopK.sh L432 1024 $query 40) 2>&1
	rm -r spark-warehouse
	rm -r metastore_db
done
