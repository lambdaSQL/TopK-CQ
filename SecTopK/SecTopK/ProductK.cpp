#include "ProductK.h"

void ProductK(u64 partyIdx, DBServer& srvs, SharedTable R, u64 RKeyID, u64 RValID,
            SharedTable S, u64 SKeyID, u64 SValID, u64 k, SharedTable &T) {
    u64 rnrows = R.rows(), snrows = S.rows();
    u64 tnrows = rnrows * snrows;
    u64 rncols = R.mColumns.size(), sncols = S.mColumns.size();
    u64 tncols = rncols + sncols + 1;

    // Calculate the annotation
    sbMatrix rkey(tnrows, 64), skey(tnrows, 64), ragg(tnrows, 64), sagg(tnrows, 64), tagg(tnrows, 64), tkeyagg(tnrows, 64);
    for (auto i = 0, k = 0; i < rnrows; ++i) {
        for (auto j = 0; j < snrows; ++j) {
            rkey.mShares[0](k) = R.mColumns[RKeyID].mShares[0](i);
            rkey.mShares[1](k) = R.mColumns[RKeyID].mShares[1](i);
            skey.mShares[0](k) = S.mColumns[SKeyID].mShares[0](j);
            skey.mShares[1](k) = S.mColumns[SKeyID].mShares[1](j);

            ragg.mShares[0](k) = R.mColumns[RValID].mShares[0](i);
            ragg.mShares[1](k) = R.mColumns[RValID].mShares[1](i);
            sagg.mShares[0](k) = S.mColumns[SValID].mShares[0](j);
            sagg.mShares[1](k) = S.mColumns[SValID].mShares[1](j);

            ++k;
        }
    }
    BetaLibrary lib;
    // auto circ_mul = lib.int_int_mult(64, 64, 64, aby3::CircuitLibrary::Optimized::Depth);
    auto circ_mul = lib.int_int_add(64, 64, 64, aby3::CircuitLibrary::Optimized::Depth);
    Sh3BinaryEvaluator eval_mul;
    
    eval_mul.setCir(circ_mul, tnrows, srvs.mEnc.mShareGen);
    eval_mul.setInput(0, ragg);
    eval_mul.setInput(1, sagg);
    eval_mul.asyncEvaluate(srvs.mRt.noDependencies()).get();
    eval_mul.getOutput(0, tagg);

    // SBMatReveal(srvs, tagg);

    auto circ_mux = EqualityCheckAndMuxDepthOptimized();
    Sh3BinaryEvaluator eval_mux;
    eval_mux.setCir(&circ_mux, tnrows, srvs.mEnc.mShareGen);
    eval_mux.setInput(0, tagg);
    eval_mux.setInput(1, rkey);
    eval_mux.setInput(2, skey);
    eval_mux.asyncEvaluate(srvs.mRt.noDependencies()).get();
    eval_mux.getOutput(0, tkeyagg);
    
    // std::cout << std::endl;
    // SBMatReveal(srvs, tkeyagg);

    T.mColumns.resize(tncols);
    for (auto i=0; i<tncols; ++i) {
        T.mColumns[i].resize(tnrows, 64);
    }

    for (auto i = 0, k = 0; i < rnrows; ++i) {
        for (auto j = 0; j < snrows; ++j) {

            for (auto l=0; l<rncols; ++l) {
                T.mColumns[l].mShares[0](k) = R.mColumns[l].mShares[0](i);
                T.mColumns[l].mShares[1](k) = R.mColumns[l].mShares[1](i);
            }

            for (auto l=0; l<sncols; ++l) {
                T.mColumns[l+rncols].mShares[0](k) = S.mColumns[l].mShares[0](j);
                T.mColumns[l+rncols].mShares[1](k) = S.mColumns[l].mShares[1](j);
            }


            T.mColumns[tncols - 1].mShares[0](k) = tkeyagg.mShares[0](k);
            T.mColumns[tncols - 1].mShares[1](k) = tkeyagg.mShares[1](k);

            ++k;
        }
    }
    /* 
    ShuffleQuickSort(partyIdx, srvs, T, tncols - 1);
    // remove annotation and take the first K
    T.mColumns.pop_back();
    for (auto i=0; i<T.mColumns.size(); ++i) {
        T.mColumns[i].resize(k, 64);
    }
    */
    // STableReveal(srvs, T);
    
    ShuffleSelectK(partyIdx, srvs, T, tncols - 1, k);
    ShuffleQuickSort(partyIdx, srvs, T, tncols - 1, 1);
    // T.mColumns.pop_back();
}