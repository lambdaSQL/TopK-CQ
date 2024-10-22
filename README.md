# TopK

This repo provides the code of paper "Relational Algorithms for Top-k Query Evaluation", and also contains the scripts for reproducing the experiment results in the paper.

The file structure is as below
```
TopK  
└─── SecTopK
│    └─── SecTopK     // Source codes of secure implementation
│    └─── some aby3 dependencies
└─── python  // Legency version of query rewriter
└─── C++  // Source codes of C++ version for Product-K and Level-K
└─── RankEnum  // Source codes of rank enumeration algorithms from paper "Ranked enumeration of Join Queries with Projection"
└─── lambdaSQL // Source codes of lambdaSQL system, with end-to-end parser, rewriter and DuckDB support
└─── scripts
     └─── DuckDB     // Test scripts for DuckDB
     |      └─── QueryTemplate  // All SQL Queries used in the experiment.  *Original.sql represents the original SQL queries,
     |                          // *ProductK.sql represents the rewrite SQL using the product-k algorithm, *LevelK.sql represents the rewrite SQL using the level-K algorithm.  
     └─── SparkSQL   // Test scripts for SparkSQL
     └─── PostgreSQL // Test scripts for PostgreSQL
```

## SecTopK
We provide a semi-honest implementation of secure level-k and product-k algorithms under the three-server honest majority setting.

Our work is built on [ABY3](https://github.com/ladnir/aby3) framework. Please refer to `ABY3-README.md` for building dependencies and installation guidelines.

We also provide a simple demo (See `SecTopK/demo.cpp` for details), it supports 3Line and Tree queries; and `LevelK` and `ProductK` algorithms.
```
python3 build.py --setup
python3 build.py 
cd bin 
./demo [0/1/2]
```

The parameter [0/1/2] stands for the identifier of the three servers.

## Plaintext Experiments
We provide a series of bash scripts for our experiments on plaintext.  Users should install PostgreSQL and Spark in advance and advise the executable file in configuration or under $PATH.

For the experiment on DuckDB, users should put the executable file under the script folder, or use DownloadCLI.sh to download the CLI executable under the folder.  Users can run 'runproductK.sh', 'runlevelK.sh', 'rundiffK.sh', 'rundiffDist.sh' under ./scripts/DuckDB to obtain the experiment results.  New queries can be added under the QueryTemplate and run with 
```
TopKWrapper.sh ${input_query} ${input_k} ${input_graph} 1(only for level-K)
```
For example, 
```
TopKWrapper.sh L3ProductK 1024 twitter 0
```
executes L3ProductK.sql in DuckDB with k = 1024 and using graph twitter.

## Other Experiments

You can refer to the README file under each directory to get the usage of each platform.