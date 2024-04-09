#!/bin/bash

for i in "bitcoin" "dblp" "epinions" "wiki_vote" "twitter"
do
	query=${i}
	echo $query
	(time ./ExecuteTopK.sh L2Original 1024 $query) 2>&1
	(time ./ExecuteTopK.sh L2ProductK 1024 $query) 2>&1
	(time ./ExecuteTopK.sh L3Original 1024 $query) 2>&1
	(time ./ExecuteTopK.sh L3ProductK 1024 $query) 2>&1
	(time ./ExecuteTopK.sh L4Original 1024 $query) 2>&1
	(time ./ExecuteTopK.sh L4ProductK 1024 $query) 2>&1
	(time ./ExecuteTopK.sh StarOriginal 1024 $query) 2>&1
	(time ./ExecuteTopK.sh StarProductK 1024 $query) 2>&1
	(time ./ExecuteTopK.sh TreeOriginal 1024 $query) 2>&1
	(time ./ExecuteTopK.sh TreeProductK 1024 $query) 2>&1
done
