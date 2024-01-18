#!/bin/bash

for i in "bitcoin" "dblp" "epinions" "wiki_vote" "twitter"
do
	query=${i}
	echo $query
	(time ./ExecuteLevelK.sh L232 1024 $query) 2>&1
	(time ./ExecuteLevelK.sh L332 1024 $query) 2>&1
	(time ./ExecuteLevelK.sh L432 1024 $query) 2>&1
done
