#include "LevelK.h"

void ExpandTable(SharedTable R, u64 start_id, u64 end_id, SharedTable &RE) {
    u64 nrows = R.rows(), ncols = R.mColumns.size();
    RE.mColumns.resize(ncols + 1);
    u64 renrows = nrows * (end_id - start_id + 1);
    for (auto i=0; i<=ncols; ++i) {
        RE.mColumns[i].resize(renrows, 64);
        u64 id = 0;
        for (auto k=start_id; k<=end_id; ++k) {
            for (auto j=0; j<nrows; ++j) {
                if (i < ncols) {
                    RE.mColumns[i].mShares[0](id) = R.mColumns[i].mShares[0](j);
                    RE.mColumns[i].mShares[1](id) = R.mColumns[i].mShares[1](j);
                } else {
                    RE.mColumns[i].mShares[0](id) = k;
                    RE.mColumns[i].mShares[1](id) = k;
                }
                ++id;
            }
        }
    }
}

// We only remain the value "ID" list
void DecomposeColumn(sbMatrix &col_a) {
    u64 nrows = col_a.rows();
    for (auto i=0; i<nrows; ++i) {
        col_a.mShares[0](i) = (col_a.mShares[0](i) >> 32);
        col_a.mShares[1](i) = (col_a.mShares[1](i) >> 32);
    }
}

void ComposeColumn(sbMatrix col_a, sbMatrix &col_b) {
    u64 nrows = col_a.rows();
    for (auto i=0; i<nrows; ++i) {
        col_b.mShares[0](i) = ((col_b.mShares[0](i)) << 32) | (col_a.mShares[0](i) & ((1ULL << 32) - 1));
        col_b.mShares[1](i) = ((col_b.mShares[1](i)) << 32) | (col_a.mShares[1](i) & ((1ULL << 32) - 1));
    }
}

void AppendTuples(SharedTable add, std::vector<u64> cols, SharedTable &origin) {
    if (origin.mColumns.size() == 0) {
        origin.mColumns.resize(cols.size());
    }
    u64 nrows = origin.rows(), addrows = add.rows();
    for (auto j=0; j<cols.size(); ++j) {
        origin.mColumns[j].resize(nrows + addrows, 64);
        for (auto i=0; i<addrows; ++i) {
            origin.mColumns[j].mShares[0](i + nrows) = add.mColumns[cols[j]].mShares[0](i);
            origin.mColumns[j].mShares[1](i + nrows) = add.mColumns[cols[j]].mShares[1](i);
        }
    }
}

void TakeLastRows(SharedTable &R, u64 lastnrows) {
    u64 nrows = R.rows();
    SharedTable T = R;
    for (auto i=0; i<R.mColumns.size(); ++i) {
        R.mColumns[i].resize(lastnrows, 64);
        for (auto j=0; j<lastnrows; ++j) {
            R.mColumns[i].mShares[0](j) = T.mColumns[i].mShares[0](nrows - lastnrows + j);
            R.mColumns[i].mShares[1](j) = T.mColumns[i].mShares[1](nrows - lastnrows + j);
        }
    }
}

void LevelK(u64 partyIdx, DBServer& srvs, SharedTable R, u64 RKeyID, u64 RValID,
            SharedTable S, u64 SKeyID, u64 SValID, u64 k, SharedTable &T) {
    u64 rnrows = R.rows(), snrows = S.rows();
    u64 tnrows = rnrows * snrows;
    u64 rncols = R.mColumns.size(), sncols = S.mColumns.size();
    u64 tncols = rncols + sncols + 1;

    //Sort S by SKeyID, SValID
    ShuffleQuickSort(partyIdx, srvs, S, SKeyID, SValID, 1);

    // Add a new column ID for S
    S.mColumns.resize(sncols + 1);
    S.mColumns[sncols].resize(snrows, 64);
    for (auto i=0; i<snrows; ++i) {
        S.mColumns[sncols].mShares[0](i) = S.mColumns[sncols].mShares[1](i) = 1;
    }
    SegmentedPrefixSum(partyIdx, srvs, S.mColumns[SKeyID], S.mColumns[sncols]);

    //Sort S by (SIDID, RKeyID) in ascending order
    ShuffleQuickSort(partyIdx, srvs, S, sncols, SKeyID, 0);

    //Sort R by RKeyID in ascending order
    ShuffleQuickSort(partyIdx, srvs, R, RKeyID, 0);

    SharedTable R12;
    ExpandTable(R, 1, 2, R12);

    ComposeColumn(R12.mColumns[RKeyID], R12.mColumns[rncols]);
    ComposeColumn(S.mColumns[SKeyID], S.mColumns[sncols]);

    SharedTable Ttemp;

    PKFKJoin(partyIdx, srvs, R12, rncols, rncols, S, sncols, sncols, Ttemp);

    BetaLibrary lib;
    // auto circ_mul = lib.int_int_mult(64, 64, 64, aby3::CircuitLibrary::Optimized::Depth);
    auto circ_mul = lib.int_int_add(64, 64, 64, aby3::CircuitLibrary::Optimized::Depth);
    Sh3BinaryEvaluator eval_mul;
    
    sbMatrix tagg(Ttemp.rows(), 64), tkeyagg(Ttemp.rows(), 64), tkey(Ttemp.rows(), 64);
    eval_mul.setCir(circ_mul, Ttemp.rows(), srvs.mEnc.mShareGen);
    eval_mul.setInput(0, Ttemp.mColumns[RValID]);
    eval_mul.setInput(1, Ttemp.mColumns[rncols + 1 + SValID]);
    eval_mul.asyncEvaluate(srvs.mRt.noDependencies()).get();
    eval_mul.getOutput(0, tagg);

    auto circ_mux = EqualityCheckAndMuxDepthOptimized();
    Sh3BinaryEvaluator eval_mux;
    eval_mux.setCir(&circ_mux, Ttemp.rows(), srvs.mEnc.mShareGen);
    eval_mux.setInput(0, tagg);
    eval_mux.setInput(1, Ttemp.mColumns[RKeyID]);
    eval_mux.setInput(2, Ttemp.mColumns[rncols + 1 + SKeyID]);
    eval_mux.asyncEvaluate(srvs.mRt.noDependencies()).get();
    eval_mux.getOutput(0, tkeyagg);
    Ttemp.mColumns[rncols + 1 + sncols].mShares = tkeyagg.mShares;

    eval_mux.setCir(&circ_mux, Ttemp.rows(), srvs.mEnc.mShareGen);
    eval_mux.setInput(0, Ttemp.mColumns[rncols]);
    eval_mux.setInput(1, Ttemp.mColumns[RKeyID]);
    eval_mux.setInput(2, Ttemp.mColumns[rncols + 1 + SKeyID]);
    eval_mux.asyncEvaluate(srvs.mRt.noDependencies()).get();
    eval_mux.getOutput(0, tkey);
    Ttemp.mColumns[rncols].mShares = tkey.mShares;

    std::vector<u64> remaining_cols = {};
    for (auto i=0; i<rncols; ++i) {
        remaining_cols.push_back(i);
    }
    for (auto i=0; i<=sncols; ++i) {
        remaining_cols.push_back(rncols + 1 + i);
    }
    AppendTuples(Ttemp, remaining_cols, T);

    std::vector<u64> r_filter_cols = {};
    for (auto i=0; i<=rncols; ++i) {
        r_filter_cols.push_back(i);
    }

    for (auto range = 2; range < k; range *= 2) {
        // std::cerr << "\n============================\n" << range << std::endl;;
        u64 bound = range;

        R = Ttemp;
        TakeColumns(R, r_filter_cols);

        SharedTable weights = Ttemp;
        sb64 kth;
        TakeColumns(weights, {Ttemp.mColumns.size() - 1});
        // std::cerr << "weights\n";
        // STableReveal(srvs, weights);
        ShuffleSelectK(partyIdx, srvs, weights, 0, k);
        // std::cerr << "after shuffle\n";
        // STableReveal(srvs, weights);

        sbMatrix kthmat(Ttemp.rows(), 64);
        for (auto i=0; i<kthmat.rows(); ++i) {
            kthmat.mShares[0](i) = weights.mColumns[0].mShares[0](k-1);
            kthmat.mShares[1](i) = weights.mColumns[0].mShares[1](k-1);
        }
        // std::cerr << "kth\n";
        // SBMatReveal(srvs, kthmat);

        auto circ_lt = LessThanAndMux(64);
        Sh3BinaryEvaluator eval_lt;
        sbMatrix out(Ttemp.rows(), 64);
        eval_lt.setCir(&circ_lt, Ttemp.rows(), srvs.mEnc.mShareGen);
        eval_lt.setInput(0, kthmat);
        eval_lt.setInput(1, Ttemp.mColumns[Ttemp.mColumns.size() - 1]);
        eval_lt.setInput(2, R.mColumns[rncols]);
        eval_lt.asyncEvaluate(srvs.mRt.noDependencies()).get();
        eval_lt.getOutput(0, out);
        R.mColumns[rncols].mShares = out.mShares;


        // std::cerr << "before start" << std::endl;
        // STableReveal(srvs, R);
        DecomposeColumn(R.mColumns[rncols]);
        // std::cerr << "decompose r " << R.rows() << ' ' << bound << std::endl;
        // STableReveal(srvs, R);
        for (auto i=0; i<R.rows(); ++i) {
            R.mColumns[rncols].mShares[0](i) ^= bound;
            R.mColumns[rncols].mShares[1](i) ^= bound;
        }
        // std::cerr << "xor bound " << R.rows() << ' ' << bound << std::endl;
        // STableReveal(srvs, R);

        if (range == 2) {
            TakeLastRows(R, 2 * k / bound);
        } else {
            TakeLastRows(R, 4 * k / bound);
        }
        // std::cerr << "Take Last Rows" << std::endl;
        // STableReveal(srvs, R);
        CompactionZeroEntity(partyIdx, srvs, R, rncols, 1, 2 * k / bound);
        // std::cerr << "compact r " << R.rows() << ' ' << 2 * k / bound << std::endl;
        // STableReveal(srvs, R);
        // std::cerr << "compact r end\n";

        R.mColumns.pop_back();
        SharedTable Rrange;
        ExpandTable(R, range + 1, 2 * range, Rrange);

        // std::cerr << "expand R " << range + 1 << ' ' << 2 * range << std::endl;
        // STableReveal(srvs, Rrange);
        // std::cerr << "S " << std::endl;
        // STableReveal(srvs, S);

        ComposeColumn(Rrange.mColumns[RKeyID], Rrange.mColumns[rncols]);

        PKFKJoin(partyIdx, srvs, Rrange, rncols, rncols, S, sncols, sncols, Ttemp);

        // std::cerr << "join result\n";
        // STableReveal(srvs, Ttemp);

        BetaLibrary lib;
        // auto circ_mul = lib.int_int_mult(64, 64, 64, aby3::CircuitLibrary::Optimized::Depth);
        auto circ_mul = lib.int_int_add(64, 64, 64, aby3::CircuitLibrary::Optimized::Depth);
        Sh3BinaryEvaluator eval_mul;
        
        sbMatrix tagg(Ttemp.rows(), 64), tkeyagg(Ttemp.rows(), 64), tkey(Ttemp.rows(), 64);
        eval_mul.setCir(circ_mul, Ttemp.rows(), srvs.mEnc.mShareGen);
        eval_mul.setInput(0, Ttemp.mColumns[RValID]);
        eval_mul.setInput(1, Ttemp.mColumns[rncols + 1 + SValID]);
        eval_mul.asyncEvaluate(srvs.mRt.noDependencies()).get();
        eval_mul.getOutput(0, tagg);

        auto circ_mux = EqualityCheckAndMuxDepthOptimized();
        Sh3BinaryEvaluator eval_mux;
        eval_mux.setCir(&circ_mux, Ttemp.rows(), srvs.mEnc.mShareGen);
        eval_mux.setInput(0, tagg);
        eval_mux.setInput(1, Ttemp.mColumns[RKeyID]);
        eval_mux.setInput(2, Ttemp.mColumns[rncols + 1 + SKeyID]);
        eval_mux.asyncEvaluate(srvs.mRt.noDependencies()).get();
        eval_mux.getOutput(0, tkeyagg);
        Ttemp.mColumns[rncols + 1 + sncols].mShares = tkeyagg.mShares;

        eval_mux.setCir(&circ_mux, Ttemp.rows(), srvs.mEnc.mShareGen);
        eval_mux.setInput(0, Ttemp.mColumns[rncols]);
        eval_mux.setInput(1, Ttemp.mColumns[RKeyID]);
        eval_mux.setInput(2, Ttemp.mColumns[rncols + 1 + SKeyID]);
        eval_mux.asyncEvaluate(srvs.mRt.noDependencies()).get();
        eval_mux.getOutput(0, tkey);
        Ttemp.mColumns[rncols].mShares = tkey.mShares;

        std::vector<u64> remaining_cols = {};
        for (auto i=0; i<rncols; ++i) {
            remaining_cols.push_back(i);
        }
        for (auto i=0; i<=sncols; ++i) {
            remaining_cols.push_back(rncols + 1 + i);
        }
        // std::cerr << "Ttemp" << std::endl;
        // STableReveal(srvs, Ttemp);
        AppendTuples(Ttemp, remaining_cols, T);
        // std::cerr << "Append T" << std::endl;
        // STableReveal(srvs, T);
        // std::cerr << "\n\n============================\n\n";
    }

    ShuffleSelectK(partyIdx, srvs, T, T.mColumns.size() - 1, k);
    ShuffleQuickSort(partyIdx, srvs, T, T.mColumns.size() - 1, 1);
    // T.mColumns.pop_back();
}