#pragma once

#include <iostream>
#include <vector>
#include <map>

#include "PrefixSumCircuit.h"
#include "Permutation.h"
#include "SingleRelationOperator.h"
#include "sort.h"

using namespace oc;
using namespace aby3;

void SBMatReveal(DBServer& srvs, sbMatrix mat);

void PSIwithPayload(u64 partyIdx, DBServer& srvs, sbMatrix rkey, sbMatrix skey, sbMatrix spayload, sbMatrix& rpayload);

void Expansion(u64 partyIdx, DBServer& srvs, SharedTable R, sbMatrix deg, SharedTable &RE, i64 outsize, bool need_PI = false, u64 pk_id = 0, u64 colb_id = 0);

void BinaryJoin(u64 partyIdx, DBServer& srvs, SharedTable R, u64 RBKeyID, u64 ROrderKeyId, u64 RBPIKeyID, std::vector<u64> RCalPIID,
                SharedTable S, u64 SBKeyID, u64 SOrderKeyId, u64 SBPIKeyID, std::vector<u64> SCalPIID, SharedTable &T);

void PKJoin(u64 partyIdx, DBServer& srvs, SharedTable R, u64 RBKeyID, u64 ROrderKeyId, u64 RBPIKeyID,
    SharedTable S, u64 SBKeyID, u64 SOrderKeyId, u64 SBPIKeyID, std::vector<u64> SCalPIID, SharedTable &T);
void PKJoinBaseline(u64 partyIdx, DBServer& srvs, SharedTable R, u64 RBKeyID, u64 ROrderKeyId, u64 RBPIKeyID,
    SharedTable S, u64 SBKeyID, u64 SOrderKeyId, u64 SBPIKeyID, std::vector<u64> SCalPIID, SharedTable &T);

void PKFKwithPayload(u64 partyIdx, DBServer& srvs, sbMatrix rkey, sbMatrix skey, sbMatrix spayload, sbMatrix& rpayload);

void SemiJoin(u64 partyIdx, DBServer& srvs, SharedTable R, u64 RBKeyID, u64 ROrderKeyId, u64 RBPIKeyID,
    SharedTable S, u64 SBKeyID, u64 SOrderKeyId, u64 SBPIKeyID, u64 SAggKeyID, u64 AFunType, SharedTable &T);
void SemiJoinBaseline(u64 partyIdx, DBServer& srvs, SharedTable R, u64 RBKeyID, u64 ROrderKeyId, u64 RBPIKeyID,
    SharedTable S, u64 SBKeyID, u64 SOrderKeyId, u64 SBPIKeyID, u64 SAggKeyID, u64 AFunType, SharedTable &T);

void TakeColumns(SharedTable& table, std::vector<u64> col_lists);

void PKFKJoin(u64 partyIdx, DBServer& srvs, SharedTable R, u64 RBKeyID, u64 ROrderKeyId,
            SharedTable S, u64 SBKeyID, u64 SOrderKeyId, SharedTable &T);