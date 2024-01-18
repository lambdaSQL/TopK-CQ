"""
Usage:
  main.py <query> [options]
  
Options:
  -h --help     Show help.
  <query>       Set execute query path, like topk1/
  -b, --base base   Set level-k log base [default: 32]
  -m, --mode mode   Set topK algorithm mode. 0: level-k, 1: product-k [default: 0]
  -g, --genType type    Set generate code mode D(DuckDB)/M(MySql) [default: D]
"""

from docopt import docopt
from treenode import *
from comparison import Comparison
from jointree import Edge, JoinTree
from aggregation import *
from generateIR import *
from generateAggIR import *
from generateTopKIR import *
from codegen import *
from codegenTopK import *
from topk import *

from random import randint
import os
import re
import time
import traceback
import requests

BASE_PATH = 'query/'
DDL_NAME = 'graph.ddl'
QUERY_NAME = 'query.sql'
OUT_NAME = 'rewrite.txt'

AddiRelationNames = set(['TableAggRelation', 'AuxiliaryRelation', 'BagRelation']) #5, 5, 6


''' Formatt
RelationName;id;source/inalias(bag);cols;tableDisplayName;[AggList(tableagg)|internalRelations(bag)|supportingRelation(aux)|group+func(agg)]
Only AuxiliaryRelation source is [Bag(Graph,Graph)|Graph|...]
'''

def connect(base: int, mode: int, type: GenType):
    start = time.time()
    headers = {'Content-Type': 'application/json'}
    body = dict()
    ddl_file = open(BASE_PATH + DDL_NAME)
    body['ddl'] = ddl_file.read()
    ddl_file.close()
    query_file = open(BASE_PATH + QUERY_NAME)
    body['query'] = query_file.read()
    query_file.close()
    '''
    body = {
        'ddl': "CREATE TABLE Graph(src    INT,dst    INT,rating DECIMAL) WITH ('path' = 'examples/data/graph.dat')",
        'query': "SELECT R.src+R.dst as c FROM graph R,graph S,graph T,graph U WHERE R.dst = S.src AND S.dst = T.src AND T.dst = U.src AND S.src < T.dst order by R.rating DESC limit 10"
    }
    '''
    response = requests.post(url="http://localhost:8848/api/v1/parse?orderBy=fanout&desc=false&limit=1", headers=headers, json=body).json()['data']
    end = time.time()
    print('Parse time(s): ' + str(end-start) + '\n')
    # print(response)
    # 1. 
    table2vars = dict([(t['name'], t['columns']) for t in response['tables']])
    # 2. parse jointree
    joinTrees = response['joinTrees']
    isFreeConnex = response['freeConnex']
    isFull = response['full']
    optJT: JoinTree = None
    optCOMP: dict[int, Comparison] = None
    allRes, aggFunc = [], []
    for index, jt in enumerate(joinTrees):
        allNodes = dict()
        supId = set()
        
        nodes, edges, root, subset, comparisons = jt['nodes'], jt['edges'], jt['root'], jt['subset'], jt['comparisons']
        
        # a. parse relations
        for node in nodes:
            id, name, source, cols = node['id'], node['type'], node['source'], node['columns']
            alias = node['alias']
            if name == 'BagRelation':
                # FIXME: 
                internal = node['internal']
                inAlias = None
                inId = None
                bagNode = BagTreeNode(id, source, cols, [], alias, inId, inAlias)
                allNodes[id] = bagNode
                
            elif name == 'AuxiliaryRelation':
                supportId = node['support']
                auxNode = AuxTreeNode(id, source, cols, [], alias, supportId)
                supId.add(supportId)
                allNodes[id] = auxNode
            
            elif name == 'TableScanRelation':
                tsNode = TableTreeNode(id, source, cols, [], alias)
                allNodes[id] = tsNode
            
            elif name == 'TableAggRelation':
                aggList = node['aggList']
                taNode = TableAggTreeNode(id, source, cols, [], alias, aggList)
                allNodes[id] = taNode
            
            elif name == 'AggregatedRelation':
                group = node['group']
                func = node['func']
                aNode = AggTreeNode(id, source, cols, [], alias, group, func)
                allNodes[id] = aNode
            
            else:
                raise NotImplementedError("Error Realtion type! ")
        # b. parse edge
        allNodes = parse_col2var(allNodes, table2vars)
        JT = JoinTree(allNodes, isFull, isFreeConnex, supId, subset)
        JT.setRootById(root)
        CompareMap: dict[int, Comparison] = dict()
        
        for edge_data in edges:
            edge = Edge(JT.getNode(edge_data['src']), JT.getNode(edge_data['dst']))
            JT.addEdge(edge)
        # c. parse comparison
        for compId, comp in enumerate(comparisons):
            op, path, left, right = comparisons['op'], comparisons['path'], comparisons['left'], comparisons['right']
            # FIXME: split from op
            opName = None
            Compare = Comparison()
            Compare.setAttr(compId, opName, left, right, path, left+right, op)
            CompareMap[Compare.id] = Compare
        # d. final
        if optJT is not None and JT.root.depth < optJT.root.depth:
            optJT, optCOMP = JT, CompareMap
        elif optJT is None:
            optJT, optCOMP = JT, CompareMap
        allRes.append([JT, CompareMap, index])  

    # 3. parse outputVariables
    outputVariables = response['outputVariables']
    groupBy = response['groupByVariables']
    # 4. aggregation
    aggregations = response['aggregations']
    Agg = None
    for aggregation in aggregations:
        func, result, formular = aggregation['func'], aggregation['result'], aggregation['args']
        pattern = re.compile('v[0-9]+')
        inVars = list(set(pattern.findall(formular)))
        agg = AggFunc(func, inVars, result, formular)
        aggFunc.append(agg)
    if len(aggFunc):
        Agg = Aggregation(groupBy, aggFunc)
    # 5. topk
    topK_data = response['topK']
    topK = None
    if topK_data:
        topK = TopK(topK_data['orderByVariable'], topK_data['desc'], topK_data['limit'], mode=mode, base=base, genType=type)
    # 6. computations
    computations = response['computations']
    tempComp = []
    for com in computations:
        comp = Comp(com['result'], com['expr'])
        tempComp.append(comp)
    computationList = CompList(tempComp)
    return optJT, optCOMP, allRes, outputVariables, Agg, topK, computationList


def parse_col2var(allNodes: dict[int, TreeNode], table2vars: dict[str, str]) -> dict[int, TreeNode]:
    sortedNodes = sorted(allNodes.items())
    ret = {k: v for k, v in sortedNodes}
    for id, treeNode in ret.items():
        # k: id, v: TreeNode
        vars = table2vars.get(treeNode.source, None) # Aux/bag can't get the corresponding
        if treeNode.relationType == RelationType.TableScanRelation:
            treeNode.setcol2vars([treeNode.cols, vars])
            
        elif treeNode.relationType == RelationType.AggregatedRelation:
            aggVars = [vars[treeNode.group], treeNode.func.name+'(*)']
            treeNode.setcol2vars([treeNode.cols, aggVars])
        
        elif treeNode.relationType == RelationType.TableAggRelation:    # tablescan+agg: source must in table2vars
            aggIds = treeNode.aggRelation
            aggAllVars = set()
            for id in aggIds:
                # NOTE: Only one aggregation function
                aggAllVars.add(allNodes[id].cols[-1])
            
            i = 0
            col2vars = [[], []]
            # 1. push original (not from aggList) first
            for col in treeNode.cols:
                if col not in aggAllVars:
                    col2vars[0].append(col)
                    col2vars[1].append(vars[i])
                    i += 1
            # 2. push agg values (alias tackle in aggNode)
            for var in aggAllVars:
                col2vars[0].append(var)
                col2vars[1].append('')
                
            treeNode.setcol2vars(col2vars)   
            
        elif treeNode.relationType == RelationType.BagRelation:
            allBagVars = set()
            allBagVarMap = dict()
            for eachId in treeNode.insideId:
                eachCols, eachVars = allNodes[eachId].col2vars
                eachAlias = allNodes[eachId].alias
                for index, eachCol in enumerate(eachCols):
                    if eachCol not in allBagVars:
                        allBagVars.add(eachCol)
                        allBagVarMap[eachCol] =  eachAlias + '.' + (eachVars[index] if allNodes[eachId].relationType == RelationType.TableScanRelation else eachCol)

            vars = [allBagVarMap[col] for col in treeNode.cols]
            treeNode.setcol2vars([treeNode.cols, vars])
            
        elif treeNode.relationType == RelationType.AuxiliaryRelation:
            supCols, supVars = allNodes[treeNode.supRelationId].col2vars
            auxCols, auxVars = [], []
            for index, col in enumerate(supCols):
                if col in treeNode.cols:
                    auxCols.append(col)
                    auxVars.append(supVars[index])
            treeNode.setcol2vars([auxCols, auxVars])
            
    return ret


if __name__ == '__main__':
    arguments = docopt(__doc__)
    BASE_PATH += arguments['<query>']
    base = int(arguments['--base'])
    mode=int(arguments['--mode']) 
    # print(arguments)
    type=GenType.Mysql if arguments['--genType'] == 'M' else GenType.DuckDB
    optJT, optCOMP, allRes, outputVariables, Agg, topK, computationList = connect(base=base, mode=mode, type=type)
    start = time.time()
    IRmode = IRType.Report if not Agg else IRType.Aggregation
    IRmode = IRType.Level_K if topK.mode == 0 else IRmode
    IRmode = IRType.Product_K if topK.mode == 1 else IRmode
    # sign for whether process all JT
    optFlag = False
    if optFlag:
        if IRmode == IRType.Report:
            reduceList, enumerateList, finalResult = generateIR(optJT, optCOMP, outputVariables)
            codeGen(reduceList, enumerateList, outputVariables, BASE_PATH + 'opt' +OUT_NAME, isFull=optJT.isFull)
        elif IRmode == IRType.Aggregation:
            aggList, reduceList, enumerateList, finalResult = generateAggIR(optJT, optCOMP, outputVariables, Agg)
            codeGen(reduceList, enumerateList, finalResult, outputVariables, BASE_PATH + 'opt' +OUT_NAME, aggGroupBy=Agg.groupByVars, aggList=aggList, isFull=optJT.isFull, isAgg=True)
        # NOTE: No comparison for TopK yet
        elif IRmode == IRType.Level_K:
            reduceList, enumerateList, finalResult = generateTopKIR(optJT, outputVariables, IRmode=IRType.Level_K, base=topK.base, DESC=topK.DESC, limit=topK.limit)
            codeGenTopK(reduceList, enumerateList, finalResult,  BASE_PATH + 'opt' +OUT_NAME, IRmode=IRType.Level_K, genType=topK.genType)
        elif IRmode == IRType.Product_K:
            reduceList, enumerateList, finalResult = generateTopKIR(optJT, outputVariables, IRmode=IRType.Product_K, base=topK.base, DESC=topK.DESC, limit=topK.limit)
            codeGenTopK(reduceList, enumerateList, finalResult,  BASE_PATH + 'opt' +OUT_NAME, IRmode=IRType.Product_K, genType=topK.genType)  
        
    else:
        for jt, comp, index in allRes:
            outName = OUT_NAME.split('.')[0] + str(index) + '.' + OUT_NAME.split('.')[1]
            try:
                if IRmode == IRType.Report:
                    reduceList, enumerateList, finalResult = generateIR(jt, comp, outputVariables)
                    codeGen(reduceList, enumerateList, finalResult, outputVariables, BASE_PATH + outName, isFull=jt.isFull)
                elif IRmode == IRType.Aggregation:
                    Agg.initDoneFlag()
                    aggList, reduceList, enumerateList, finalResult = generateAggIR(jt, comp, outputVariables, Agg)
                    codeGen(reduceList, enumerateList, finalResult, outputVariables, BASE_PATH + outName, aggGroupBy=Agg.groupByVars, aggList=aggList, isFull=jt.isFull, isAgg=True)
                # NOTE: No comparison for TopK yet
                elif IRmode == IRType.Level_K:
                    reduceList, enumerateList, finalResult = generateTopKIR(jt, outputVariables, IRmode=IRType.Level_K, base=topK.base, DESC=topK.DESC, limit=topK.limit)
                    codeGenTopK(reduceList, enumerateList, finalResult, BASE_PATH + outName, IRmode=IRType.Level_K, genType=topK.genType)
                elif IRmode == IRType.Product_K:
                    reduceList, enumerateList, finalResult = generateTopKIR(jt, outputVariables, IRmode=IRType.Product_K, base=topK.base, DESC=topK.DESC, limit=topK.limit)
                    codeGenTopK(reduceList, enumerateList, finalResult, BASE_PATH + outName, IRmode=IRType.Product_K, genType=topK.genType)

            except Exception as e:
                traceback.print_exc()
                print("Error JT: " + index)
    end = time.time()
    print('Rewrite time(s): ' + str(end-start) + '\n')