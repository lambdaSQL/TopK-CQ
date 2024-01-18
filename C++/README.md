# Relational Algorithms for Top-k Query Evaluation

## Description
The directory contains a C++ implementation of product-k and level-k Protocols.

## Dependency
cmake >= 3.22.1

gcc >= 11.3.0

## Compile
``` sh
cmake -B build
cmake --build build -j 8
```

## Run
### Usage
```sh
./build/test/TopK
Usage: TopK <input_file> <method> <query> <options>
Method: productK/levelK/all
Query: line2/line3/line4
Options: [-K int] [-b int] [-max] [-mul]
```

### Example
```
./build/test/TopK ./data/bitcoin_dense.txt all line4
Data file = data/bitcoin_dense.txt, Query = line4
K=1024,base=2,oplus=MIN,otimes=ADD

--- Method productK ---
Preprocessing: 34ms
Binary joins: 47ms
Total: 81ms
Result digest: MIN=11, MAX=30, SUM=25206

--- Method levelK ---
Preprocessing: 31ms
Binary joins: 27ms
Total: 59ms
Result digest: MIN=11, MAX=30, SUM=25206
```

## Data Format
Line 1 starts with column names. Currently, the only accepted schema is Graph(Src, Dst, Rating). 
Other lines are data. Separator is a **white space**. 
``` sh
head ./data/bitcoin_dense.txt
src dst rating
20 1 202
14 1 203
30 1 204
18 1 205
2 1 206
4 1 207
0 1 208
31 1 199
25 1 180
```