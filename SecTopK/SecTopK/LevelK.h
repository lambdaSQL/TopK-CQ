#pragma once

#include <iostream>
#include <vector>
#include <unordered_map>

#include "PrefixSumCircuit.h"
#include "Permutation.h"
#include "SingleRelationOperator.h"
#include "sort.h"
#include "join.h"

using namespace oc;
using namespace aby3;

void LevelK(u64 partyIdx, DBServer& srvs, SharedTable R, u64 RKeyID, u64 RValID, SharedTable S, u64 SKeyID, u64 SValID, u64 k, SharedTable &T);
