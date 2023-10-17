# TopK

This repo provides the code of paper "Relational Algorithms for Top-k Query Evaluation", and also contains the scripts for reproducing the experiment results in the paper.

The file structure is as below
```
TopK  
└─── SecTopK
│    └─── SecTopK     // Source codes of secure implementation
│    └─── some aby3 dependencies
└─── python
└─── scripts
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