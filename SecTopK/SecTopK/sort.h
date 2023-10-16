#pragma once

#include <iostream>
#include <vector>
#include <unordered_map>

#include "PrefixSumCircuit.h"
#include "Permutation.h"
#include "SingleRelationOperator.h"

using namespace oc;
using namespace aby3;

void ShuffleQuickSort(u64 partyIdx, DBServer& srvs, SharedTable &R, u64 sortkeyid, u64 orderby = 1);
// orderby = [0/1] represents ASC/DESC;
void ShuffleQuickSort(u64 partyIdx, DBServer& srvs, SharedTable &R, u64 sortkeyid1, u64 sortkeyid2, u64 orderby);

void ShuffleSelectK(u64 partyIdx, DBServer& srvs, SharedTable &R, u64 sortkeyid, u64 k);
// Select the largest K annotations only

void FindKthElement(u64 partyIdx, DBServer& srvs, SharedTable weights, u64 k, sb64 &kth);