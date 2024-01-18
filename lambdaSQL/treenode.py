from enumsType import *
from reduce import *
from enumerate import *
from aggregation import *
from levelK import *
from productK import *

from typing import Union

class TreeNode:
    def __init__(self, id: int, source: str, cols: list[str], col2vars: list[list[str], list[str]], alias: str):
        self.id = id                # relation id
        self.source = source        # Graph
        self.cols = cols            # [v7, v8]
        self.alias = alias          # table displayName, g1
        self.col2vars = col2vars    # map variable name to original variable name
                                    # zipped = zip(a,b), zip(*zipped)
        self.children: list[TreeNode] = []
        self.parent: TreeNode = None
        
        self.estimateSize = -1                      # estimate relation size
        self.relationType: RelationType = None
        self.createViewAlready: bool = False        # create view TableAgg, Aux, bag already
        self.reducePhase: Union[ReducePhase, AggReducePhase, LevelKReducePhase, ProductKReducePhase] = None        # Attach reduce information to the node
        self.JoinResView: Union[Join2tables, SemiJoin, CreateBagAuxView, WithView] = None        # record the name of previous join
        # topk
        self.enumDone: bool = False                 # mark for whether done in enumerate phase
    
    @property
    def getcol2vars(self): 
        return self.col2vars
    
    def setcol2vars(self, col2vars: list[list[str]]):
        self.col2vars = col2vars
    
    @property
    def getNodeAlias(self):
        return self.alias
    
    @property
    def isLeaf(self):
        return len(self.children) == 0
    
    @property
    def isRoot(self):
        return self.parent == None
    
    @property
    def depth(self):
        return 1 + max([0] + [c.depth for c in self.children])
    
    def removeChild(self, child):
        self.children.remove(child)
    
    def __repr__(self) -> str:
        return 'TreeNode[' + str(self.id) + ']: ' + self.alias
    
    def __str__(self) -> str:
        ret = str(self.id) + str('\n') + str(self.source) + str('\n') + str(self.cols)
        ret += str('\n') + str(self.alias) + str('\n') + str(self.col2vars)
        return ret
    
    def setReducePhase(self, reducePhase: ReducePhase):
        self.ReducePhase = reducePhase
        
    def setEnumeratePhase(self, enumeratePhase: EnumeratePhase):
        self.EnumeratePhase = enumeratePhase
        
'''only one Support Relation now, jar code only provide one support relation'''
class AuxTreeNode(TreeNode):
    def __init__(self, id: int, source: str, cols: list[str], col2vars: list[list[str], list[str]], alias: str, supRelationId: int):
        super().__init__(id, source, cols, col2vars, alias)
        self.relationType = RelationType.AuxiliaryRelation
        self.supRelationId = supRelationId # supporting TreeNode Id

        
class Func(Enum):
    COUNT = 0
    # TODO: Others Support later

class AggTreeNode(TreeNode):
    '''
    Only exist in TableAggTreeNode
    '''
    def __init__(self, id: int, source: str, cols: list[str], col2vars: list[list[str], list[str]], alias: str, group: int, func: str):
        super().__init__(id, source, cols, col2vars, alias)
        self.relationType = RelationType.AggregatedRelation
        self.group = group
        self.func = Func[func] # use enum to build

class TableAggTreeNode(TreeNode):
    '''
    mix of TableScan and Agg
    '''
    def __init__(self, id: int, source: str, cols: list[str], col2vars: list[list[str], list[str]], alias: str, aggRelation: list[int]):
        super().__init__(id, source, cols, col2vars, alias)
        self.relationType = RelationType.TableAggRelation
        self.aggRelation = aggRelation  # list of agg id


class BagTreeNode(TreeNode):
    def __init__(self, id: int, source: str, cols: list[str], col2vars: list[list[str], list[str]], alias: str, insideId: list[int], insideAlias: list[str]):
        super().__init__(id, source, cols, col2vars, alias)
        self.relationType = RelationType.BagRelation
        self.insideId = insideId
        self.inAlias = insideAlias
        self.auxId = None
        # self.auxFlag = [False * len(self.insideId)]      # mark if create aux view for axiliary view in the bag
    

class TableTreeNode(TreeNode):
    def __init__(self, id: int, source: str, cols: list[str], col2vars: list[list[str], list[str]], alias: str):
        super().__init__(id, source, cols, col2vars, alias)
        self.relationType = RelationType.TableScanRelation
