#pragma once

#include <iostream>
#include <vector>

#include "Permutation.h"

void SBMatReveal(DBServer& srvs, sbMatrix mat);

void STableReveal(DBServer& srvs, SharedTable tab);
void STableReveal(DBServer& srvs, SharedTable tab, u64 limit);

void PrefixSum(u64 partyIdx, DBServer& srvs, sbMatrix& val);
void SegmentedPrefixSum(u64 partyIdx, DBServer& srvs, const sbMatrix key, sbMatrix& val);

void NeighborEquality(u64 partyIdx, DBServer& srvs, const sbMatrix key, sbMatrix& tag);

void GenDummyKey(u64 partyIdx, DBServer& srvs, const sbMatrix key, sbMatrix& key_dum);

void SegmentedCopy(u64 partyIdx, DBServer& srvs, const sbMatrix key, sbMatrix& value1);
void SegmentedCopy(u64 partyIdx, DBServer& srvs, const sbMatrix key, sbMatrix& value1, sbMatrix& value2);

BetaCircuit ZeroCheckAndMux();

void CopyDuplicate(u64 partyIdx, DBServer& srvs, SharedTable& RE);

void CalcAlignmentPI(u64 partyIdx, DBServer& srvs, SharedTable& RE, u64 pkid, u64 bid);

BetaCircuit LessThan(u64 size);
BetaCircuit LessThanAndMux(u64 size);

BetaCircuit EqualityCheckAndMuxDepthOptimized();