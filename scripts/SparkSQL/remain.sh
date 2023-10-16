#!/bin/bash

	(time ./ExecuteTopK.sh StarOriginal 1024 epinions) 2>&1
	(time ./ExecuteTopK.sh StarOriginal 1024 twitter) 2>&1
	(time ./ExecuteTopK.sh TreeOriginal 1024 dblp) 2>&1
	(time ./ExecuteTopK.sh TreeOriginal 1024 wiki_vote) 2>&1
	(time ./ExecuteTopK.sh L4Original 1024 epinions) 2>&1
