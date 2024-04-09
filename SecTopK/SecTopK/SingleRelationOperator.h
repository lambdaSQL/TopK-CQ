#pragma once

#include <iostream>
#include <vector>
#include <unordered_map>

#include "PrefixSumCircuit.h"
#include "Permutation.h"

using namespace oc;
using namespace aby3;

enum AggFunc {
    MAX,
    MIN,
    COUNT, 
    SUM,
    AVG,
    DISTINCT
};

void ColBitAnd(u64 partyIdx, DBServer& srvs, sbMatrix signal, sbMatrix &value); // value <- value * signal [0/1]
void ColBitAnd(u64 partyIdx, DBServer& srvs, SharedColumn signal, SharedColumn &value); // value <- value * signal [0/1]

void CompactionZeroEntity(u64 partyIdx, DBServer& srvs, SharedTable &table, u64 col_id, u64 is_zero = 0, i64 bound_size = -1);