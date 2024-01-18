# lambdaSQL

This project provides the experiment rewriter code of the paper: Relational Algorithms for Top-k Query Evaluation.

## Queries

The experiment queries, including the original queries and queries after rewriting, are provided in the directory `line2/*`, `line3/*`, `line4/*`, `star/*`, `tree/*`. 

## Graph Data

All graph data can be downloaded from SNAP (https://snap.stanford.edu).


## Experiments
### Requirements
- Java JDK or JRE(Java Runtime Environment). This program use one `jar` file to parse the query and generate the related information.
- Python version >= 3.9
- Python package requirements: docopt, requests

### Steps
 1. Modify path for `python3` & `duckdb` in `rewrite_and_run.sh`.
 2. Modify the path to graph data in `query/load_graph.sql`, and change the table schema if needed. 
 3. To run a given query, put the schema file `graph.ddl` and the SQL query `query.sql` under the execute query path.  The default schema is 
 ```SQL
 CREATE TABLE Graph (
    src INT,
    dst INT,
    rating INT
 );
 ```
 
 4. Execute the following command and get the required output. 
 ```
 $ bash start_parser.sh
 $ Parser started.
 $ bash rewrite_and_run.sh line2/ 32 0
    or 
 $ bash rewrite_and_run.sh line2/
 ```
 5. The results is generated in `rewrite_and_run.result`. 
 6. Use `jps` command to get the parser pid which name is `jar`, and then kill it when finished. 

### Notes
- For parameters in step3, the details information is shown below. 
```
Options:
  -h --help           Show help.
  <query>             Set execute query path, like line2/, remember to include / at the end.
  -b, --base base     Set level-k log base [default: 32]
  -m, --mode mode     Set topK algorithm mode. 0: level-k, 1: product-k [default: 0]
  -g, --genType type  Set generate code mode D(DuckDB)/M(MySql) [default: D]
```