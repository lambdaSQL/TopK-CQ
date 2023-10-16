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

void CalcSHA(u64 partyIdx, DBServer& srvs, SharedTable& R, u64 RKeyID, u64 RSHAID, SharedTable S, u64 SKeyID, u64 SSHAID);

void FilterNextK(u64 partyIdx, DBServer& srvs, SharedTable R, u64 RKeyID, u64 RAnnotID, SharedTable &S, u64 SKeyID, u64 SSHAID, u64 k);

void ColumnMult(u64 partyIdx, DBServer& srvs, SharedTable& R, u64 RAID, u64 RBID, u64 RCID);