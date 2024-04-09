#include "SingleRelationOperator.h"

BetaCircuit BitAnd() {
    BetaCircuit r;
    BetaBundle a(64), b(64);
    BetaBundle out(64);
    r.addInputBundle(a);
    r.addInputBundle(b);
    r.addOutputBundle(out);
    for (auto i=0; i<64; ++i) {
        r.addGate(a[0], b[i], GateType::And, out[i]);
    }
    return r;
}

void ColBitAnd(u64 partyIdx, DBServer& srvs, sbMatrix signal, sbMatrix &value) {
    u64 nrows = value.rows();
    BetaLibrary lib;
    auto circ = BitAnd();
    Sh3BinaryEvaluator eval;

    eval.setCir(&circ, nrows, srvs.mEnc.mShareGen);
    eval.setInput(0, signal);
    eval.setInput(1, value);

    eval.asyncEvaluate(srvs.mRt.noDependencies()).get();

    eval.getOutput(0, value);
}

void ColBitAnd(u64 partyIdx, DBServer& srvs, SharedColumn signal, SharedColumn &value) {
    u64 nrows = value.rows();
    sbMatrix sig(nrows, 64), val(nrows, 64);
    sig.mShares = signal.mShares;
    val.mShares = value.mShares;
    ColBitAnd(partyIdx, srvs, sig, val);
    value.mShares = val.mShares;
}

BetaCircuit ZeroCheck() {
    BetaCircuit r;
    BetaBundle a(64), t(64);
    BetaBundle out(1);
    r.addInputBundle(a);
    r.addTempWireBundle(t);
    r.addOutputBundle(out);
    for (auto l=64; l>2; l/=2) {
        // a or b = (a xor b) xor (a and b)
        for (auto i=0, j=l-1; i<j; ++i, --j) {
            r.addGate(a[i], a[j], GateType::Xor, t[i]);
            r.addGate(a[i], a[j], GateType::And, t[j]);
        }
        for (auto i=0, j=l-1; i<j; ++i, --j) {
            r.addGate(t[i], t[j], GateType::Xor, a[i]);
        }
    }
    r.addGate(a[0], a[1], GateType::Xor, t[0]);
    r.addGate(a[0], a[1], GateType::And, t[1]);
    r.addGate(t[0], t[1], GateType::Xor, out[0]);
    return r;
}

void CompactionZeroEntity(u64 partyIdx, DBServer& srvs, SharedTable &table, u64 col_id, u64 is_zero, i64 bound_size) {
    u64 nrows = table.rows();
    BetaLibrary lib;
    auto circ = ZeroCheck();
    Sh3BinaryEvaluator eval;

    eval.setCir(&circ, nrows, srvs.mEnc.mShareGen);
    eval.setInput(0, table.mColumns[col_id]);

    eval.asyncEvaluate(srvs.mRt.noDependencies()).get();

    sbMatrix tag(nrows, 1);
    eval.getOutput(0, tag);

    // SBMatReveal(srvs, tag); std::cerr << std::endl;

    sbMatrix pos1(nrows, 64), pos2(nrows, 64), pos(nrows, 64);
    for (auto i=0; i<nrows; ++i) {
        pos1.mShares[0](i) = tag.mShares[0](i) ^ is_zero;
        pos1.mShares[1](i) = tag.mShares[1](i) ^ is_zero;
        pos2.mShares[0](i) = tag.mShares[0](i) ^ 1 ^ is_zero;
        pos2.mShares[1](i) = tag.mShares[1](i) ^ 1 ^ is_zero;
    }
    PrefixSum(partyIdx, srvs, pos1);
    PrefixSum(partyIdx, srvs, pos2);
    // SBMatReveal(srvs, pos1); std::cerr << std::endl;
    // SBMatReveal(srvs, pos2); std::cerr << std::endl;

    auto circ_minus = lib.int_int_add(64, 64, 64, BetaLibrary::Optimized::Depth);
    Sh3BinaryEvaluator eval_minus;
    sbMatrix bound(nrows, 64), size(1, 64);
    for (auto i=0; i<nrows; ++i) {
        bound.mShares[0](i) = pos1.mShares[0](nrows - 1);
        bound.mShares[1](i) = pos1.mShares[1](nrows - 1);
    }
    size.mShares[0](0) = pos1.mShares[0](nrows - 1);
    size.mShares[1](0) = pos1.mShares[1](nrows - 1);

    u64 compact_size;
    if (bound_size == -1) {
        i64Matrix real_size(1, 1);
        srvs.mEnc.revealAll(srvs.mRt.mComm, size, real_size);
        compact_size = real_size(0, 0);
    } else {
        compact_size = bound_size;
    }
    

    // SBMatReveal(srvs, bound); std::cerr << std::endl;
    eval_minus.setCir(circ_minus, nrows, srvs.mEnc.mShareGen);
    eval_minus.setInput(0, bound);
    eval_minus.setInput(1, pos2);
    eval_minus.asyncEvaluate(srvs.mRt.noDependencies()).get();
    eval_minus.getOutput(0, pos2);
    // SBMatReveal(srvs, pos2); std::cerr << std::endl;

    auto circ_mux = ZeroCheckAndMux();
    Sh3BinaryEvaluator eval_mux;
    eval_mux.setCir(&circ_mux, nrows, srvs.mEnc.mShareGen);
    eval_mux.setInput(0, table.mColumns[col_id]);
    if (is_zero == 0) {
        eval_mux.setInput(1, pos2);
        eval_mux.setInput(2, pos1);
    } else {
        eval_mux.setInput(1, pos1);
        eval_mux.setInput(2, pos2);
    }
    eval_mux.asyncEvaluate(srvs.mRt.noDependencies()).get();
    eval_mux.getOutput(0, pos);
    // SBMatReveal(srvs, pos); std::cerr << std::endl;
    u64 nattrs = table.mColumns.size();
    table.mColumns.resize(nattrs + 1);
    table.mColumns[nattrs].resize(nrows, 64);
    table.mColumns[nattrs].mShares = pos.mShares;
    Permutation perm;
    perm.SSPerm(partyIdx, srvs, table, nattrs);
    table.mColumns.pop_back();
    for (auto i=0; i<table.mColumns.size(); ++i) {
        table.mColumns[i].resize(compact_size, 64);
    }
    // STableReveal(srvs, table);
}