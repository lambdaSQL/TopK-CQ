#include "CalcSHA.h"

void ColumnMult(u64 partyIdx, DBServer& srvs, SharedTable& R, u64 RAID, u64 RBID, u64 RCID) {
    sbMatrix rout(R.rows(), 64);
    BetaLibrary lib;
    // auto circ = lib.int_int_mult(64, 64, 64, aby3::CircuitLibrary::Optimized::Depth);
    auto circ = lib.int_int_add(64, 64, 64, aby3::CircuitLibrary::Optimized::Depth);
    Sh3BinaryEvaluator eval;
    
    eval.setCir(circ, R.rows(), srvs.mEnc.mShareGen);
    eval.setInput(0, R.mColumns[RAID]);
    eval.setInput(1, R.mColumns[RBID]);
    eval.asyncEvaluate(srvs.mRt.noDependencies()).get();
    eval.getOutput(0, rout);

    R.mColumns[RCID].mShares = rout.mShares;
}

void CalcSHA(u64 partyIdx, DBServer& srvs, SharedTable& R, u64 RKeyID, u64 RSHAID, SharedTable S, u64 SKeyID, u64 SSHAID) {
    // Sort R by RKeyID in ascending order
    ShuffleQuickSort(partyIdx, srvs, R, RKeyID, 0);
    // Sort S by (SKeyID, SSHAID) in ascending order
    ShuffleQuickSort(partyIdx, srvs, S, SKeyID, SSHAID, 0);
    // std::cerr << "Sorted S" << std::endl;
    // STableReveal(srvs, S);

    u64 rnrows = R.rows();
    sbMatrix rpayload(rnrows, 64), rout(rnrows, 64);
    PKFKwithPayload(partyIdx, srvs, R.mColumns[RKeyID], S.mColumns[SKeyID], S.mColumns[SSHAID], rpayload);

    BetaLibrary lib;
    // auto circ = lib.int_int_mult(64, 64, 64, aby3::CircuitLibrary::Optimized::Depth);
    auto circ = lib.int_int_add(64, 64, 64, aby3::CircuitLibrary::Optimized::Depth);
    Sh3BinaryEvaluator eval;
    
    eval.setCir(circ, rnrows, srvs.mEnc.mShareGen);
    eval.setInput(0, R.mColumns[RSHAID]);
    eval.setInput(1, rpayload);
    eval.asyncEvaluate(srvs.mRt.noDependencies()).get();
    eval.getOutput(0, rout);

    R.mColumns[RSHAID].mShares = rout.mShares;
    // std::cerr << "R" << std::endl;
    // STableReveal(srvs, R);
}

void FilterNextK(u64 partyIdx, DBServer& srvs, SharedTable R, u64 RKeyID, u64 RAnnotID, SharedTable &S, u64 SKeyID, u64 SSHAID, u64 k) {
    u64 STHAID = S.mColumns.size();
    S.mColumns.resize(STHAID + 1);
    S.mColumns[STHAID] = S.mColumns[SSHAID];
    CalcSHA(partyIdx, srvs, S, SKeyID, STHAID, R, RKeyID, RAnnotID);
    ShuffleSelectK(partyIdx, srvs, S, STHAID, k);
    S.mColumns.pop_back();
}