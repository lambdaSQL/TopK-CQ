#include "join.h"

void PSIwithPayload(u64 partyIdx, DBServer& srvs, sbMatrix rkey, sbMatrix skey, sbMatrix spayload, sbMatrix& rpayload) {
    std::vector<ColumnInfo> aCols = { ColumnInfo{ "key", TypeID::IntID, srvs.mKeyBitCount } };
    std::vector<ColumnInfo> bCols = { ColumnInfo{ "key", TypeID::IntID, srvs.mKeyBitCount } , ColumnInfo{ "payload", TypeID::IntID, 64 } };

    u64 rnrows = rkey.rows(), snrows = skey.rows();
    // std::cerr << "psi : " << rnrows << ' ' << snrows << std::endl;
    Table a(rnrows, aCols), b(snrows, bCols);
    SharedTable R, S;

    if (partyIdx == 0) { // This is a very huge but useless initialization, but I don't know how to initialize a sharetable.
        R = srvs.localInput(a);
        S = srvs.localInput(b);
    } else {
        R = srvs.remoteInput(0);
        S = srvs.remoteInput(0);
    }
    // R.mColumns.resize(1);
    // R.mColumns[0].resize(rnrows, srvs.mKeyBitCount);
    // R.mColumns[0].mName = "key";
    // R.mColumns[0].mType = std::make_shared<StringType>(srvs.mKeyBitCount);
    GenDummyKey(partyIdx, srvs, rkey, R.mColumns[0]);

    // S.mColumns.resize(2);
    // S.mColumns[0].resize(snrows, srvs.mKeyBitCount);
    // S.mColumns[0].mName = "key";
    // S.mColumns[0].mType = std::make_shared<StringType>(srvs.mKeyBitCount);
    GenDummyKey(partyIdx, srvs, skey, S.mColumns[0]);
    // S.mColumns[1].resize(snrows, 64);
    // S.mColumns[1].mName = "payload";
    // S.mColumns[1].mType = std::make_shared<IntType>(64);
    S.mColumns[1].mShares[0] = spayload.mShares[0];
    S.mColumns[1].mShares[1] = spayload.mShares[1];

    auto start_cost = srvs.mRt.mComm.mNext.getTotalDataSent() + srvs.mRt.mComm.mPrev.getTotalDataSent();

    SelectQuery query;
	query.noReveal("r");
	query.joinOn(R["key"], S["key"]);
    query.addOutput("payload", query.addInput(S["payload"])); // only output payload as value
	auto out = srvs.joinImpl(query);

    auto end_cost = srvs.mRt.mComm.mNext.getTotalDataSent() + srvs.mRt.mComm.mPrev.getTotalDataSent();
    // std::cerr << partyIdx << ",PSI-stage_comm = " << end_cost - start_cost << std::endl;

    rpayload.resize(rnrows, 64);
    rpayload.mShares[0] = out.mColumns[0].mShares[0];
    rpayload.mShares[1] = out.mColumns[0].mShares[1];
    return;
}

// void PKFKwithPayload(u64 partyIdx, DBServer& srvs, sbMatrix rkey, sbMatrix skey, sbMatrix spayload, sbMatrix& rpayload) {
//     std::vector<ColumnInfo> aCols = { ColumnInfo{ "key", TypeID::IntID, srvs.mKeyBitCount } };
//     std::vector<ColumnInfo> bCols = { ColumnInfo{ "key", TypeID::IntID, srvs.mKeyBitCount } , ColumnInfo{ "payload", TypeID::IntID, 64 } };

//     u64 rnrows = rkey.rows(), snrows = skey.rows();
//     if (snrows < rnrows) {
//         skey.resize(rnrows, skey.bitCount());
//         spayload.resize(rnrows, spayload.bitCount());
//         for (auto i=snrows; i<rnrows; ++i) {
//             skey.mShares[0](i) = skey.mShares[1](i) = 0;
//             spayload.mShares[0](i) = spayload.mShares[1](i) = 0;
//         }
//         snrows = rnrows;
//     }
//     // std::cerr << "pkfk : " << rnrows << ' ' << snrows << std::endl;
//     Table a(rnrows, aCols), b(snrows, bCols);
//     SharedTable R, S;

//     if (partyIdx == 0) { // This is a very huge but useless initialization, but I don't know how to initialize a sharetable.
//         R = srvs.localInput(a);
//         S = srvs.localInput(b);
//     } else {
//         R = srvs.remoteInput(0);
//         S = srvs.remoteInput(0);
//     }
//     // R.mColumns.resize(1);
//     // R.mColumns[0].resize(rnrows, srvs.mKeyBitCount);
//     // R.mColumns[0].mName = "key";
//     // R.mColumns[0].mType = std::make_shared<StringType>(srvs.mKeyBitCount);
//     // GenDummyKey(partyIdx, srvs, rkey, R.mColumns[0]);
//     for (auto i=0; i<R.rows(); ++i) {
//         R.mColumns[0].mShares[0](i, 0) = rkey.mShares[0](i);
//         R.mColumns[0].mShares[0](i, 1) = 0;
//         R.mColumns[0].mShares[1](i, 0) = rkey.mShares[1](i);
//         R.mColumns[0].mShares[1](i, 1) = 0;
//     }

//     GenDummyKey(partyIdx, srvs, skey, S.mColumns[0]);
//     S.mColumns[1].mShares[0] = spayload.mShares[0];
//     S.mColumns[1].mShares[1] = spayload.mShares[1];
    

//     auto start_cost = srvs.mRt.mComm.mNext.getTotalDataSent() + srvs.mRt.mComm.mPrev.getTotalDataSent();

//     SelectQuery query;
// 	query.noReveal("r");
// 	query.joinOn(R["key"], S["key"]);
//     query.addOutput("payload", query.addInput(S["payload"])); // only output payload as value
// 	auto out = srvs.joinImpl(query);

//     auto end_cost = srvs.mRt.mComm.mNext.getTotalDataSent() + srvs.mRt.mComm.mPrev.getTotalDataSent();
//     // std::cerr << "     " << partyIdx << ",PSI-stage_comm = " << end_cost - start_cost << std::endl;

//     rpayload.resize(rnrows, 64);
//     rpayload.mShares[0] = out.mColumns[0].mShares[0];
//     rpayload.mShares[1] = out.mColumns[0].mShares[1];
//     return;
// }

void LowMC(u64 partyIdx, DBServer& srvs, sbMatrix mat, i64Matrix& rmat) {
    const auto blockSize = srvs.mLowMCCir.mInputs[0].size();
    const auto rounds = srvs.mLowMCCir.mInputs.size() - 1;
    const auto nrows = mat.rows();
    aby3::Sh3BinaryEvaluator eval;
    aby3::sbMatrix oprfRoundKey(1, blockSize);

    // std::cerr << blockSize << ' ' << rounds << ' ' << nrows << std::endl;

    // SBMatReveal(srvs, mat);

    eval.setCir(&srvs.mLowMCCir, nrows, srvs.mEnc.mShareGen);
    eval.setInput(0, mat);
    for (u64 j = 0; j < rounds; ++j) {
        srvs.mEnc.rand(oprfRoundKey);
        // SBMatReveal(srvs, oprfRoundKey);
        eval.setReplicatedInput(j + 1, oprfRoundKey);
    }
    eval.asyncEvaluate(srvs.mRt.noDependencies());
    srvs.mRt.runAll();

    sPackedBin tempout(nrows, blockSize);
    eval.getOutput(0, tempout);
    srvs.mEnc.revealAll(srvs.mRt.noDependencies(), tempout, rmat);
    srvs.mRt.runAll();
    // std::cerr << "========================\n";
    // for (auto i=0; i<rmat.rows(); ++i) {
    //     for (auto j=0; j<rmat.cols(); ++j) {
    //         std::cerr << rmat(i, j) << ' ';
    //     }
    //     std::cerr << std::endl;
    // }
    // std::cerr << "========================\n";
    return;
}

void Expansion(u64 partyIdx, DBServer& srvs, SharedTable R, sbMatrix deg, SharedTable& RE, i64 outsize, bool need_PI, u64 pk_id, u64 colb_id) {
    u64 nrows = R.rows();
    sbMatrix agg_deg = deg;
    PrefixSum(partyIdx, srvs, agg_deg);
    if (outsize == -1) {
        sb64 ssum_len;
        ssum_len.mData[0] = agg_deg.mShares[0](nrows - 1);
        ssum_len.mData[1] = agg_deg.mShares[1](nrows - 1);
        srvs.mEnc.revealAll(srvs.mRt.noDependencies(), ssum_len, outsize).get();
    }

    u64 expand_size = outsize + nrows;

    // Now we want a MUX gate to choose the position
    // (deg == 0) ? expand_size - 1 - idx : agg_deg[idx - 1]
    sbMatrix val1(nrows, 64), val2(nrows, 64);
    for (auto i=0; i<nrows; ++i) {
        val1.mShares[0](i) = val1.mShares[1](i) = expand_size - 1 - i;
        if (i == 0) {
            val2.mShares[0](i) = val2.mShares[1](i) = 0;
        } else {
            val2.mShares[0](i) = agg_deg.mShares[0](i-1);
            val2.mShares[1](i) = agg_deg.mShares[1](i-1);
        }
    }
    BetaLibrary lib;
    auto circ = ZeroCheckAndMux();
    Sh3BinaryEvaluator eval;
    eval.setCir(&circ, nrows, srvs.mEnc.mShareGen);
    eval.setInput(0, deg);
    eval.setInput(1, val1);
    eval.setInput(2, val2);
    eval.asyncEvaluate(srvs.mRt.noDependencies()).get();
    sbMatrix pos(nrows, 64);
    eval.getOutput(0, pos);
    // SBMatReveal(srvs, pos);

    // Generate a random permutation of length `expand_size`
    SharedTable randid;
    randid.mColumns.resize(1);
    randid.mColumns[0].resize(expand_size, 64);
    for (auto i=0; i<expand_size; ++i) {
        randid.mColumns[0].mShares[0](i) = randid.mColumns[0].mShares[1](i) = i;
    }
    Permutation perm;
    perm.RandPerm(partyIdx, srvs, randid);
    sbMatrix randpos(expand_size, 64);
    randpos.mShares[0] = randid.mColumns[0].mShares[0];
    randpos.mShares[1] = randid.mColumns[0].mShares[1];
    // SBMatReveal(srvs, randpos);

    // LowMC encryption and reveal of `pos` & `randpos`
    sbMatrix totalpos(nrows + expand_size, 80);
    for (auto i=0; i<nrows + expand_size; ++i) {
        if (i < nrows) {
            totalpos.mShares[0](i, 0) = pos.mShares[0](i, 0);
            totalpos.mShares[1](i, 0) = pos.mShares[1](i, 0);
            totalpos.mShares[0](i, 1) = totalpos.mShares[1](i, 1) = 0;
        } else {
            totalpos.mShares[0](i, 0) = randpos.mShares[0](i - nrows, 0);
            totalpos.mShares[1](i, 0) = randpos.mShares[1](i - nrows, 0);
            totalpos.mShares[0](i, 1) = totalpos.mShares[1](i, 1) = 0;
        }
    }
    // SBMatReveal(srvs, totalpos);
    i64Matrix lowmcres;

    LowMC(partyIdx, srvs, totalpos, lowmcres);

    // std::cerr << "=====================\n";
    // std::cerr << "   low mc results \n";
    // std::cerr << "=====================\n";
    // for (auto i=0; i<lowmcres.rows(); ++i) {
    //     for (auto j=0; j<lowmcres.cols(); ++j) {
    //         std::cerr << lowmcres(i, j) << ' ';
    //     }
    //     std::cerr << std::endl;
    // }
    // std::cerr << "=====================\n";
    sbMatrix expand_pi (expand_size, 64);
    std::map< __int128 , bool > is_occur;
    for (auto i=0; i<nrows; ++i) {
        expand_pi.mShares[0](i) = pos.mShares[0](i);
        expand_pi.mShares[1](i) = pos.mShares[1](i);
        is_occur[(((__int128) lowmcres(i, 0)) << 64) ^ ((__int128) lowmcres(i, 1)) ] = true;
    }
    for (auto i=(u64)0, j=nrows; i<expand_size; ++i) {
        if (is_occur.find ( (((__int128) lowmcres(i + nrows, 0)) << 64) ^ ((__int128) lowmcres(i + nrows, 1)) ) == is_occur.end()) {
            expand_pi.mShares[0](j) = randpos.mShares[0](i);
            expand_pi.mShares[1](j) = randpos.mShares[1](i);
            ++j;
        }
    }
    // SBMatReveal(srvs, expand_pi);

    u64 ncols = R.mColumns.size();
    RE.mColumns.resize(ncols + 2);
    //The first ncols columns are table info
    // (ncols + 1) column is tag (real or dummy)
    // (ncols + 2) column is permutation indices
    for (auto i=0; i<ncols + 2; ++i) {
        RE.mColumns[i].resize(expand_size, 64);
        if (i < ncols) {
            for (auto j=0; j<nrows; ++j) {
                RE.mColumns[i].mShares[0](j) = R.mColumns[i].mShares[0](j);
                RE.mColumns[i].mShares[1](j) = R.mColumns[i].mShares[1](j);
            }
        } else if (i == ncols) {
            for (auto j=0; j<expand_size; ++j) {
                u64 value = (j < nrows) ? 1 : 0;
                RE.mColumns[i].mShares[0](j) = RE.mColumns[i].mShares[1](j) = value;
            }
        } else {
            for (auto j=0; j<expand_size; ++j) {
                RE.mColumns[i].mShares[0](j) = expand_pi.mShares[0](j);
                RE.mColumns[i].mShares[1](j) = expand_pi.mShares[1](j);
            }
        }
    }
    perm.SSPerm(partyIdx, srvs, RE, ncols + 1, 0);
    //Remove the last column (PI)
    RE.mColumns.pop_back();
    //Take the first outsize rows
    for (auto i=0; i<RE.mColumns.size(); ++i) {
        RE.mColumns[i].resize(outsize, 64);
    }
    // STableReveal(srvs, RE);

    //Copy duplicates to the following dummy tuples
    CopyDuplicate(partyIdx, srvs, RE);
}

void BinaryJoin(u64 partyIdx, DBServer& srvs, 
    SharedTable R, u64 RBKeyID, u64 ROrderKeyId, u64 RBPIKeyID, std::vector<u64> RCalPIID,
    SharedTable S, u64 SBKeyID, u64 SOrderKeyId, u64 SBPIKeyID, std::vector<u64> SCalPIID,
    SharedTable &T) {
    
    Permutation perm;
    if (ROrderKeyId != RBKeyID) {
        perm.SSPerm(partyIdx, srvs, R, RBPIKeyID);
    }
    if (SOrderKeyId != SBKeyID) {
        perm.SSPerm(partyIdx, srvs, S, SBPIKeyID);
    }
    std::cout << partyIdx << ",PERM-COMM = " << srvs.mRt.mComm.mNext.getTotalDataSent() + srvs.mRt.mComm.mPrev.getTotalDataSent() << std::endl;

    SharedTable RB, SB;
    u64 rnrows = R.rows(), snrows = S.rows();

    // Calculate Degree
    // Column 0 - Key
    // Column 1 - Degree
    RB.mColumns.resize(3);
    RB.mColumns[0].resize(rnrows, 64);
    RB.mColumns[1].resize(rnrows, 64);
    RB.mColumns[2].resize(rnrows, 64);
    RB.mColumns[0].mShares = R.mColumns[RBKeyID].mShares;
    for (auto i=0; i<rnrows; ++i) { // degree is set to 1 initially
        RB.mColumns[1].mShares[0](i) = 1;
        RB.mColumns[1].mShares[1](i) = 1;
    }
    SegmentedPrefixSum(partyIdx, srvs, RB.mColumns[0], RB.mColumns[1]);

    SB.mColumns.resize(3);
    SB.mColumns[0].resize(snrows, 64);
    SB.mColumns[1].resize(snrows, 64);
    SB.mColumns[2].resize(snrows, 64);
    SB.mColumns[0].mShares = S.mColumns[SBKeyID].mShares;
    for (auto i=0; i<snrows; ++i) {
        SB.mColumns[1].mShares[0](i) = 1;
        SB.mColumns[1].mShares[1](i) = 1;
    }
    SegmentedPrefixSum(partyIdx, srvs, SB.mColumns[0], SB.mColumns[1]);
    // Add Column "J" <- SB.mColumns[1]
    u64 snattrs = S.mColumns.size();
    S.mColumns.resize(snattrs + 1);
    S.mColumns[snattrs].resize(snrows, 64);
    S.mColumns[snattrs].mShares = SB.mColumns[1].mShares;
    // std::cout << partyIdx << ",DEG-PREFIX-SUM-COMM = " << srvs.mRt.mComm.mNext.getTotalDataSent() + srvs.mRt.mComm.mPrev.getTotalDataSent() << std::endl;

    // std::cout << "before PSI with payload, COMM = " << srvs.mRt.mComm.mNext.getTotalDataSent() + srvs.mRt.mComm.mPrev.getTotalDataSent() << std::endl;

    PSIwithPayload(partyIdx, srvs, RB.mColumns[0], SB.mColumns[0], SB.mColumns[1], RB.mColumns[2]);
    PSIwithPayload(partyIdx, srvs, SB.mColumns[0], RB.mColumns[0], RB.mColumns[1], SB.mColumns[2]);
    // std::cout << partyIdx << ",PSI-COMM = " << srvs.mRt.mComm.mNext.getTotalDataSent() + srvs.mRt.mComm.mPrev.getTotalDataSent() << std::endl;

    SegmentedCopy(partyIdx, srvs, RB.mColumns[0], RB.mColumns[1], RB.mColumns[2]);
    SegmentedCopy(partyIdx, srvs, SB.mColumns[0], SB.mColumns[1], SB.mColumns[2]);
    // std::cout << partyIdx << ",COPY-COMM = " << srvs.mRt.mComm.mNext.getTotalDataSent() + srvs.mRt.mComm.mPrev.getTotalDataSent() << std::endl;

    // Update PI
    if (RCalPIID.size() != 0) {
        for (auto piid : RCalPIID) {
            SharedTable PItable;
            PItable.mColumns.resize(3);
            PItable.mColumns[0].resize(rnrows, 64);
            PItable.mColumns[0].mShares = R.mColumns[RBPIKeyID].mShares;
            PItable.mColumns[1].resize(rnrows, 64);
            PItable.mColumns[1].mShares = RB.mColumns[2].mShares;
            PItable.mColumns[2].resize(rnrows, 64);
            PItable.mColumns[2].mShares = R.mColumns[piid].mShares;

            // Sort on PIID
            perm.SSPerm(partyIdx, srvs, PItable, 2);
            sbMatrix agg_deg (rnrows, 64);
            agg_deg.mShares = PItable.mColumns[1].mShares;
            PrefixSum(partyIdx, srvs, agg_deg);
            for (auto i=0; i<rnrows; ++i) {
                if (i == 0) {
                    PItable.mColumns[1].mShares[0](i) = PItable.mColumns[1].mShares[1](i) = 0;
                } else {
                    PItable.mColumns[1].mShares[0](i) = agg_deg.mShares[0](i-1);
                    PItable.mColumns[1].mShares[1](i) = agg_deg.mShares[1](i-1);
                }
            }
            perm.SSPerm(partyIdx, srvs, PItable, 0);
            R.mColumns[piid].mShares = PItable.mColumns[1].mShares;
        }
    }

    if (SCalPIID.size() != 0) {
        for (auto piid : SCalPIID) {
            SharedTable PItable;
            PItable.mColumns.resize(3);
            PItable.mColumns[0].resize(snrows, 64);
            PItable.mColumns[0].mShares = S.mColumns[SBPIKeyID].mShares;
            PItable.mColumns[1].resize(snrows, 64);
            PItable.mColumns[1].mShares = SB.mColumns[2].mShares;
            PItable.mColumns[2].resize(snrows, 64);
            PItable.mColumns[2].mShares = S.mColumns[piid].mShares;

            // Sort on PIID
            perm.SSPerm(partyIdx, srvs, PItable, 2);
            sbMatrix agg_deg (snrows, 64);
            agg_deg.mShares = PItable.mColumns[1].mShares;
            PrefixSum(partyIdx, srvs, agg_deg);
            for (auto i=0; i<snrows; ++i) {
                if (i == 0) {
                    PItable.mColumns[1].mShares[0](i) = PItable.mColumns[1].mShares[1](i) = 0;
                } else {
                    PItable.mColumns[1].mShares[0](i) = agg_deg.mShares[0](i-1);
                    PItable.mColumns[1].mShares[1](i) = agg_deg.mShares[1](i-1);
                }
            }
            perm.SSPerm(partyIdx, srvs, PItable, 0);
            S.mColumns[piid].mShares = PItable.mColumns[1].mShares;
        }
    }

    // Expansion
    SharedTable RE, SE;
    // STableReveal(srvs, R);
    Expansion(partyIdx, srvs, R, RB.mColumns[2], RE, -1);
    // std::cout << partyIdx << ",EXPAND-R-COMM = " << srvs.mRt.mComm.mNext.getTotalDataSent() + srvs.mRt.mComm.mPrev.getTotalDataSent() << std::endl;
    // STableReveal(srvs, RE);

    // STableReveal(srvs, S);

    S.mColumns.resize(snattrs + 2);
    S.mColumns[snattrs + 1].resize(SB.mColumns[1].rows(), 64);
    S.mColumns[snattrs + 1].mShares = SB.mColumns[1].mShares;
    Expansion(partyIdx, srvs, S, SB.mColumns[2], SE, -1, true, SBPIKeyID, SBKeyID);
    // std::cout << partyIdx << ",EXPAND-S-COMM = " << srvs.mRt.mComm.mNext.getTotalDataSent() + srvs.mRt.mComm.mPrev.getTotalDataSent() << std::endl;
    // STableReveal(srvs, SE);

    if (RCalPIID.size() != 0) {
        u64 senrows = SE.rows();
        for (auto piid : RCalPIID) {
            sbMatrix addone(senrows, 64);
            for (auto i=0; i<senrows; ++i) {
                addone.mShares[0](i) = addone.mShares[1](i) = 1;
            }
            SegmentedPrefixSum(partyIdx, srvs, RE.mColumns[piid], addone);
            // SBMatReveal(srvs, addone);
            BetaLibrary lib;
            auto circ = lib.int_int_add(64, 64, 64, aby3::CircuitLibrary::Optimized::Depth);
            Sh3BinaryEvaluator eval;
            eval.setCir(circ, senrows, srvs.mEnc.mShareGen);
            eval.setInput(0, RE.mColumns[piid]);
            eval.setInput(1, addone);
            eval.asyncEvaluate(srvs.mRt.noDependencies()).get();
            eval.getOutput(0, addone);
            // SBMatReveal(srvs, addone);
            RE.mColumns[piid].mShares = addone.mShares;
        }
    }

    if (SCalPIID.size() != 0) {
        u64 senrows = SE.rows();
        for (auto piid : SCalPIID) {
            sbMatrix addone(senrows, 64);
            for (auto i=0; i<senrows; ++i) {
                addone.mShares[0](i) = addone.mShares[1](i) = 1;
            }
            SegmentedPrefixSum(partyIdx, srvs, SE.mColumns[piid], addone);
            // SBMatReveal(srvs, addone);
            BetaLibrary lib;
            auto circ = lib.int_int_add(64, 64, 64, aby3::CircuitLibrary::Optimized::Depth);
            Sh3BinaryEvaluator eval;
            eval.setCir(circ, senrows, srvs.mEnc.mShareGen);
            eval.setInput(0, SE.mColumns[piid]);
            eval.setInput(1, addone);
            eval.asyncEvaluate(srvs.mRt.noDependencies()).get();
            eval.getOutput(0, addone);
            // SBMatReveal(srvs, addone);
            SE.mColumns[piid].mShares = addone.mShares;
        }
    }

    // SE Re-alignment
    CalcAlignmentPI(partyIdx, srvs, SE, SBPIKeyID, SBKeyID);
    // std::cout << partyIdx << ",CalcAlignmentPI-COMM = " << srvs.mRt.mComm.mNext.getTotalDataSent() + srvs.mRt.mComm.mPrev.getTotalDataSent() << std::endl;
    perm.SSPerm(partyIdx, srvs, SE, SE.mColumns.size() - 1, 1);
    SE.mColumns.pop_back();

    T.mColumns.resize(RE.mColumns.size() + SE.mColumns.size());
    for (auto i=0; i<RE.mColumns.size(); ++i) {
        T.mColumns[i] = RE.mColumns[i];
        if (i == RBPIKeyID) {
            for (auto j=0; j<RE.rows(); ++j) {
                T.mColumns[i].mShares[0](j) = T.mColumns[i].mShares[1](j) = j + 1;
            }
        }
    }
    for (auto i=0; i<SE.mColumns.size(); ++i) {
        T.mColumns[i + RE.mColumns.size()] = SE.mColumns[i];
    }
    return;
}

void PKJoin(u64 partyIdx, DBServer& srvs, 
    SharedTable R, u64 RBKeyID, u64 ROrderKeyId, u64 RBPIKeyID, // R is the table with foreign key (R dont need expansion, nor recalculation PI)
    SharedTable S, u64 SBKeyID, u64 SOrderKeyId, u64 SBPIKeyID, std::vector<u64> SCalPIID, // S is the table with primary key
    SharedTable &T) {
    Permutation perm;
    if (ROrderKeyId != RBKeyID) {
        perm.SSPerm(partyIdx, srvs, R, RBPIKeyID);
    }
    if (SOrderKeyId != SBKeyID) {
        perm.SSPerm(partyIdx, srvs, S, SBPIKeyID);
    }
    // std::cerr << partyIdx << ",PERM-COMM = " << srvs.mRt.mComm.mNext.getTotalDataSent() + srvs.mRt.mComm.mPrev.getTotalDataSent() << std::endl;

    SharedTable RB, SB;
    u64 rnrows = R.rows(), snrows = S.rows();

    // Calculate Degree
    // Column 0 - Key
    // Column 1 - Degree
    RB.mColumns.resize(3);
    RB.mColumns[0].resize(rnrows, 64);
    RB.mColumns[1].resize(rnrows, 64);
    RB.mColumns[2].resize(rnrows, 64);
    RB.mColumns[0].mShares = R.mColumns[RBKeyID].mShares;
    for (auto i=0; i<rnrows; ++i) { // degree is set to 1 initially
        RB.mColumns[1].mShares[0](i) = 1;
        RB.mColumns[1].mShares[1](i) = 1;

        RB.mColumns[2].mShares[0](i) = 1;
        RB.mColumns[2].mShares[1](i) = 1;
    }
    SegmentedPrefixSum(partyIdx, srvs, RB.mColumns[0], RB.mColumns[1]);

    SB.mColumns.resize(3);
    SB.mColumns[0].resize(snrows, 64);
    SB.mColumns[1].resize(snrows, 64);
    SB.mColumns[2].resize(snrows, 64);
    SB.mColumns[0].mShares = S.mColumns[SBKeyID].mShares;
    for (auto i=0; i<snrows; ++i) {
        SB.mColumns[1].mShares[0](i) = 1;
        SB.mColumns[1].mShares[1](i) = 1;
    }

    // Add Column "J" <- SB.mColumns[1]
    u64 snattrs = S.mColumns.size();
    S.mColumns.resize(snattrs + 1);
    S.mColumns[snattrs].resize(snrows, 64);
    S.mColumns[snattrs].mShares = SB.mColumns[1].mShares;
    std::cerr << partyIdx << ",DEG-PREFIX-SUM-COMM = " << srvs.mRt.mComm.mNext.getTotalDataSent() + srvs.mRt.mComm.mPrev.getTotalDataSent() << std::endl;

    // std::cout << "before PSI with payload, COMM = " << srvs.mRt.mComm.mNext.getTotalDataSent() + srvs.mRt.mComm.mPrev.getTotalDataSent() << std::endl;

    // PSIwithPayload(partyIdx, srvs, RB.mColumns[0], SB.mColumns[0], SB.mColumns[1], RB.mColumns[2]);
    PSIwithPayload(partyIdx, srvs, SB.mColumns[0], RB.mColumns[0], RB.mColumns[1], SB.mColumns[2]);
    std::cerr << partyIdx << ",PSI-COMM = " << srvs.mRt.mComm.mNext.getTotalDataSent() + srvs.mRt.mComm.mPrev.getTotalDataSent() << std::endl;

    // SegmentedCopy(partyIdx, srvs, RB.mColumns[0], RB.mColumns[1], RB.mColumns[2]);
    SegmentedCopy(partyIdx, srvs, SB.mColumns[0], SB.mColumns[1], SB.mColumns[2]);
    std::cerr << partyIdx << ",COPY-COMM = " << srvs.mRt.mComm.mNext.getTotalDataSent() + srvs.mRt.mComm.mPrev.getTotalDataSent() << std::endl;

    // Update PI
    if (SCalPIID.size() != 0) {
        for (auto piid : SCalPIID) {
            SharedTable PItable;
            PItable.mColumns.resize(3);
            PItable.mColumns[0].resize(snrows, 64);
            PItable.mColumns[0].mShares = S.mColumns[SBPIKeyID].mShares;
            PItable.mColumns[1].resize(snrows, 64);
            PItable.mColumns[1].mShares = SB.mColumns[2].mShares;
            PItable.mColumns[2].resize(snrows, 64);
            PItable.mColumns[2].mShares = S.mColumns[piid].mShares;

            // Sort on PIID
            perm.SSPerm(partyIdx, srvs, PItable, 2);
            sbMatrix agg_deg (snrows, 64);
            agg_deg.mShares = PItable.mColumns[1].mShares;
            PrefixSum(partyIdx, srvs, agg_deg);
            for (auto i=0; i<snrows; ++i) {
                if (i == 0) {
                    PItable.mColumns[1].mShares[0](i) = PItable.mColumns[1].mShares[1](i) = 0;
                } else {
                    PItable.mColumns[1].mShares[0](i) = agg_deg.mShares[0](i-1);
                    PItable.mColumns[1].mShares[1](i) = agg_deg.mShares[1](i-1);
                }
            }
            perm.SSPerm(partyIdx, srvs, PItable, 0);
            S.mColumns[piid].mShares = PItable.mColumns[1].mShares;
        }
    }

    // Expansion
    SharedTable RE = R, SE;
    // STableReveal(srvs, R);
    // std::cerr << partyIdx << ",EXPAND-R-COMM = " << srvs.mRt.mComm.mNext.getTotalDataSent() + srvs.mRt.mComm.mPrev.getTotalDataSent() << std::endl;
    // STableReveal(srvs, RE);

    // STableReveal(srvs, S);

    S.mColumns.resize(snattrs + 2);
    S.mColumns[snattrs + 1].resize(SB.mColumns[1].rows(), 64);
    S.mColumns[snattrs + 1].mShares = SB.mColumns[1].mShares;
    Expansion(partyIdx, srvs, S, SB.mColumns[2], SE, -1, true, SBPIKeyID, SBKeyID);
    std::cerr << partyIdx << ",EXPAND-S-COMM = " << srvs.mRt.mComm.mNext.getTotalDataSent() + srvs.mRt.mComm.mPrev.getTotalDataSent() << std::endl;
    // STableReveal(srvs, SE);

    if (SCalPIID.size() != 0) {
        u64 senrows = SE.rows();
        for (auto piid : SCalPIID) {
            sbMatrix addone(senrows, 64);
            for (auto i=0; i<senrows; ++i) {
                addone.mShares[0](i) = addone.mShares[1](i) = 1;
            }
            SegmentedPrefixSum(partyIdx, srvs, SE.mColumns[piid], addone);
            // SBMatReveal(srvs, addone);
            BetaLibrary lib;
            auto circ = lib.int_int_add(64, 64, 64, aby3::CircuitLibrary::Optimized::Depth);
            Sh3BinaryEvaluator eval;
            eval.setCir(circ, senrows, srvs.mEnc.mShareGen);
            eval.setInput(0, SE.mColumns[piid]);
            eval.setInput(1, addone);
            eval.asyncEvaluate(srvs.mRt.noDependencies()).get();
            eval.getOutput(0, addone);
            // SBMatReveal(srvs, addone);
            SE.mColumns[piid].mShares = addone.mShares;
        }
    }

    // SE Re-alignment
    CalcAlignmentPI(partyIdx, srvs, SE, SBPIKeyID, SBKeyID);
    perm.SSPerm(partyIdx, srvs, SE, SE.mColumns.size() - 1, 1);
    std::cerr << partyIdx << ",ALIGN-S-COMM = " << srvs.mRt.mComm.mNext.getTotalDataSent() + srvs.mRt.mComm.mPrev.getTotalDataSent() << std::endl;
    SE.mColumns.pop_back();

    T.mColumns.resize(RE.mColumns.size() + SE.mColumns.size());
    for (auto i=0; i<RE.mColumns.size(); ++i) {
        T.mColumns[i] = RE.mColumns[i];
        if (i == RBPIKeyID) {
            for (auto j=0; j<RE.rows(); ++j) {
                T.mColumns[i].mShares[0](j) = T.mColumns[i].mShares[1](j) = j + 1;
            }
        }
    }
    for (auto i=0; i<SE.mColumns.size(); ++i) {
        T.mColumns[i + RE.mColumns.size()] = SE.mColumns[i];
    }
    return;
}

void TakeColumns(SharedTable& table, std::vector<u64> col_lists) {
    SharedTable temp;
    temp.mColumns.resize(col_lists.size());
    u64 nrows = table.rows();
    for (auto i=0; i<col_lists.size(); ++i) {
        temp.mColumns[i].resize(nrows, 64);
        temp.mColumns[i].mShares = table.mColumns[col_lists[i]].mShares;
    }
    table = std::move(temp);
}

void SemiJoin(u64 partyIdx, DBServer& srvs, SharedTable R, u64 RBKeyID, u64 ROrderKeyId, u64 RBPIKeyID,
    SharedTable S, u64 SBKeyID, u64 SOrderKeyId, u64 SBPIKeyID, u64 SAggKeyID, u64 AFunType, SharedTable &T) {
    Permutation perm;
    if (ROrderKeyId != RBKeyID) {
        perm.SSPerm(partyIdx, srvs, R, RBPIKeyID);
    }
    if (SOrderKeyId != SBKeyID) {
        perm.SSPerm(partyIdx, srvs, S, SBPIKeyID);
    }
    // std::cout << partyIdx << ",PERM-COMM = " << srvs.mRt.mComm.mNext.getTotalDataSent() + srvs.mRt.mComm.mPrev.getTotalDataSent() << std::endl;

    SharedTable SB;
    u64 rnrows = R.rows(), snrows = S.rows();

    SB.mColumns.resize(2);
    SB.mColumns[0].resize(snrows, 64);
    SB.mColumns[1].resize(snrows, 64);
    SB.mColumns[0].mShares = S.mColumns[SBKeyID].mShares;
    SB.mColumns[1].mShares = S.mColumns[SAggKeyID].mShares;

    // Aggregate function 
    if (AFunType == AggFunc::SUM) {
        SegmentedPrefixSum(partyIdx, srvs, SB.mColumns[0], SB.mColumns[1]);
    } else {
        std::cout << "Current function not support!\n";
        assert(0);
    }

    sbMatrix RBpayload;
    PKFKwithPayload(partyIdx, srvs, R.mColumns[RBKeyID], SB.mColumns[0], SB.mColumns[1], RBpayload);
    // std::cout << "finished \n";

    u64 nattrs = R.mColumns.size();
    T.mColumns.resize(nattrs + 1);
    for (auto i=0; i<nattrs; ++i) {
        T.mColumns[i].mShares = R.mColumns[i].mShares;
    }
    T.mColumns[nattrs].mShares = RBpayload.mShares;
    return;
}

void SemiJoinBaseline(u64 partyIdx, DBServer& srvs, SharedTable R, u64 RBKeyID, u64 ROrderKeyId, u64 RBPIKeyID,
    SharedTable S, u64 SBKeyID, u64 SOrderKeyId, u64 SBPIKeyID, u64 SAggKeyID, u64 AFunType, SharedTable &T) {
    Permutation perm;
    if (ROrderKeyId != RBKeyID) {
        ShuffleQuickSort(partyIdx, srvs, R, RBKeyID);
    }
    if (SOrderKeyId != SBKeyID) {
        ShuffleQuickSort(partyIdx, srvs, S, SBKeyID);
    }
    // std::cout << partyIdx << ",PERM-COMM = " << srvs.mRt.mComm.mNext.getTotalDataSent() + srvs.mRt.mComm.mPrev.getTotalDataSent() << std::endl;

    SharedTable SB;
    u64 rnrows = R.rows(), snrows = S.rows();

    SB.mColumns.resize(2);
    SB.mColumns[0].resize(snrows, 64);
    SB.mColumns[1].resize(snrows, 64);
    SB.mColumns[0].mShares = S.mColumns[SBKeyID].mShares;
    SB.mColumns[1].mShares = S.mColumns[SAggKeyID].mShares;

    // Aggregate function 
    if (AFunType == AggFunc::SUM) {
        SegmentedPrefixSum(partyIdx, srvs, SB.mColumns[0], SB.mColumns[1]);
    } else {
        std::cout << "Current function not support!\n";
        assert(0);
    }

    sbMatrix RBpayload;
    PKFKwithPayload(partyIdx, srvs, R.mColumns[RBKeyID], SB.mColumns[0], SB.mColumns[1], RBpayload);
    // std::cout << "finished \n";

    u64 nattrs = R.mColumns.size();
    T.mColumns.resize(nattrs + 1);
    for (auto i=0; i<nattrs; ++i) {
        T.mColumns[i].mShares = R.mColumns[i].mShares;
    }
    T.mColumns[nattrs].mShares = RBpayload.mShares;
    return;
}

void PKJoinBaseline(u64 partyIdx, DBServer& srvs, 
    SharedTable R, u64 RBKeyID, u64 ROrderKeyId, u64 RBPIKeyID, // R is the table with foreign key (R dont need expansion, nor recalculation PI)
    SharedTable S, u64 SBKeyID, u64 SOrderKeyId, u64 SBPIKeyID, std::vector<u64> SCalPIID, // S is the table with primary key
    SharedTable &T) {
    
    Permutation perm;
    if (ROrderKeyId != RBKeyID) {
        ShuffleQuickSort(partyIdx, srvs, R, RBKeyID);
    }
    if (SOrderKeyId != SBKeyID) {
        ShuffleQuickSort(partyIdx, srvs, S, SBKeyID);
    }
    // std::cout << partyIdx << ",SORT-COMM = " << srvs.mRt.mComm.mNext.getTotalDataSent() + srvs.mRt.mComm.mPrev.getTotalDataSent() << std::endl;

    SharedTable RB, SB;
    u64 rnrows = R.rows(), snrows = S.rows();

    // Calculate Degree
    // Column 0 - Key
    // Column 1 - Degree
    RB.mColumns.resize(3);
    RB.mColumns[0].resize(rnrows, 64);
    RB.mColumns[1].resize(rnrows, 64);
    RB.mColumns[2].resize(rnrows, 64);
    RB.mColumns[0].mShares = R.mColumns[RBKeyID].mShares;
    for (auto i=0; i<rnrows; ++i) { // degree is set to 1 initially
        RB.mColumns[1].mShares[0](i) = 1;
        RB.mColumns[1].mShares[1](i) = 1;

        RB.mColumns[2].mShares[0](i) = 1;
        RB.mColumns[2].mShares[1](i) = 1;
    }
    SegmentedPrefixSum(partyIdx, srvs, RB.mColumns[0], RB.mColumns[1]);

    SB.mColumns.resize(3);
    SB.mColumns[0].resize(snrows, 64);
    SB.mColumns[1].resize(snrows, 64);
    SB.mColumns[2].resize(snrows, 64);
    SB.mColumns[0].mShares = S.mColumns[SBKeyID].mShares;
    for (auto i=0; i<snrows; ++i) {
        SB.mColumns[1].mShares[0](i) = 1;
        SB.mColumns[1].mShares[1](i) = 1;
    }

    // Add Column "J" <- SB.mColumns[1]
    u64 snattrs = S.mColumns.size();
    S.mColumns.resize(snattrs + 1);
    S.mColumns[snattrs].resize(snrows, 64);
    S.mColumns[snattrs].mShares = SB.mColumns[1].mShares;
    // std::cout << partyIdx << ",DEG-PREFIX-SUM-COMM = " << srvs.mRt.mComm.mNext.getTotalDataSent() + srvs.mRt.mComm.mPrev.getTotalDataSent() << std::endl;

    // std::cout << "before PSI with payload, COMM = " << srvs.mRt.mComm.mNext.getTotalDataSent() + srvs.mRt.mComm.mPrev.getTotalDataSent() << std::endl;

    // PSIwithPayload(partyIdx, srvs, RB.mColumns[0], SB.mColumns[0], SB.mColumns[1], RB.mColumns[2]);
    PSIwithPayload(partyIdx, srvs, SB.mColumns[0], RB.mColumns[0], RB.mColumns[1], SB.mColumns[2]);
    // std::cout << partyIdx << ",PSI-COMM = " << srvs.mRt.mComm.mNext.getTotalDataSent() + srvs.mRt.mComm.mPrev.getTotalDataSent() << std::endl;

    // SegmentedCopy(partyIdx, srvs, RB.mColumns[0], RB.mColumns[1], RB.mColumns[2]);
    SegmentedCopy(partyIdx, srvs, SB.mColumns[0], SB.mColumns[1], SB.mColumns[2]);
    // std::cout << partyIdx << ",COPY-COMM = " << srvs.mRt.mComm.mNext.getTotalDataSent() + srvs.mRt.mComm.mPrev.getTotalDataSent() << std::endl;

    // Update PI
    if (SCalPIID.size() != 0) {
        for (auto piid : SCalPIID) {
            SharedTable PItable;
            PItable.mColumns.resize(3);
            PItable.mColumns[0].resize(snrows, 64);
            PItable.mColumns[0].mShares = S.mColumns[SBPIKeyID].mShares;
            PItable.mColumns[1].resize(snrows, 64);
            PItable.mColumns[1].mShares = SB.mColumns[2].mShares;
            PItable.mColumns[2].resize(snrows, 64);
            PItable.mColumns[2].mShares = S.mColumns[piid].mShares;

            // Sort on PIID
            perm.SSPerm(partyIdx, srvs, PItable, 2);
            sbMatrix agg_deg (snrows, 64);
            agg_deg.mShares = PItable.mColumns[1].mShares;
            PrefixSum(partyIdx, srvs, agg_deg);
            for (auto i=0; i<snrows; ++i) {
                if (i == 0) {
                    PItable.mColumns[1].mShares[0](i) = PItable.mColumns[1].mShares[1](i) = 0;
                } else {
                    PItable.mColumns[1].mShares[0](i) = agg_deg.mShares[0](i-1);
                    PItable.mColumns[1].mShares[1](i) = agg_deg.mShares[1](i-1);
                }
            }
            perm.SSPerm(partyIdx, srvs, PItable, 0);
            S.mColumns[piid].mShares = PItable.mColumns[1].mShares;
        }
    }

    // Expansion
    SharedTable RE = R, SE;
    // STableReveal(srvs, R);
    // std::cout << partyIdx << ",EXPAND-R-COMM = " << srvs.mRt.mComm.mNext.getTotalDataSent() + srvs.mRt.mComm.mPrev.getTotalDataSent() << std::endl;
    // STableReveal(srvs, RE);

    // STableReveal(srvs, S);

    S.mColumns.resize(snattrs + 2);
    S.mColumns[snattrs + 1].resize(SB.mColumns[1].rows(), 64);
    S.mColumns[snattrs + 1].mShares = SB.mColumns[1].mShares;
    Expansion(partyIdx, srvs, S, SB.mColumns[2], SE, -1, true, SBPIKeyID, SBKeyID);
    // std::cout << partyIdx << ",EXPAND-S-COMM = " << srvs.mRt.mComm.mNext.getTotalDataSent() + srvs.mRt.mComm.mPrev.getTotalDataSent() << std::endl;
    // STableReveal(srvs, SE);

    if (SCalPIID.size() != 0) {
        u64 senrows = SE.rows();
        for (auto piid : SCalPIID) {
            sbMatrix addone(senrows, 64);
            for (auto i=0; i<senrows; ++i) {
                addone.mShares[0](i) = addone.mShares[1](i) = 1;
            }
            SegmentedPrefixSum(partyIdx, srvs, SE.mColumns[piid], addone);
            // SBMatReveal(srvs, addone);
            BetaLibrary lib;
            auto circ = lib.int_int_add(64, 64, 64, aby3::CircuitLibrary::Optimized::Depth);
            Sh3BinaryEvaluator eval;
            eval.setCir(circ, senrows, srvs.mEnc.mShareGen);
            eval.setInput(0, SE.mColumns[piid]);
            eval.setInput(1, addone);
            eval.asyncEvaluate(srvs.mRt.noDependencies()).get();
            eval.getOutput(0, addone);
            // SBMatReveal(srvs, addone);
            SE.mColumns[piid].mShares = addone.mShares;
        }
    }

    // SE Re-alignment
    CalcAlignmentPI(partyIdx, srvs, SE, SBPIKeyID, SBKeyID);
    perm.SSPerm(partyIdx, srvs, SE, SE.mColumns.size() - 1, 1);
    SE.mColumns.pop_back();

    T.mColumns.resize(RE.mColumns.size() + SE.mColumns.size());
    for (auto i=0; i<RE.mColumns.size(); ++i) {
        T.mColumns[i] = RE.mColumns[i];
        if (i == RBPIKeyID) {
            for (auto j=0; j<RE.rows(); ++j) {
                T.mColumns[i].mShares[0](j) = T.mColumns[i].mShares[1](j) = j + 1;
            }
        }
    }
    for (auto i=0; i<SE.mColumns.size(); ++i) {
        T.mColumns[i + RE.mColumns.size()] = SE.mColumns[i];
    }
    return;
}

void PKFKwithPayload(u64 partyIdx, DBServer& srvs, sbMatrix rkey, sbMatrix skey, sbMatrix spayload, sbMatrix& rpayload) {
    u64 ncols = spayload.bitCount() / 64;

    std::vector<ColumnInfo> aCols = { ColumnInfo{ "key", TypeID::IntID, srvs.mKeyBitCount } };
    std::vector<ColumnInfo> bCols = { ColumnInfo{ "key", TypeID::IntID, srvs.mKeyBitCount } , ColumnInfo{ "payload", TypeID::IntID, spayload.bitCount() } };
    u64 rnrows = rkey.rows(), snrows = skey.rows();
    
    if (snrows < rnrows) {
        skey.resize(rnrows, skey.bitCount());
        spayload.resize(rnrows, spayload.bitCount());
        for (auto i=snrows; i<rnrows; ++i) {
            skey.mShares[0](i) = skey.mShares[1](i) = 0;
            for (auto j=0; j<ncols; ++j) {
                spayload.mShares[0](i, j) = spayload.mShares[1](i, j) = 0;
            }
        }
        snrows = rnrows;
    }

    Table a(rnrows, aCols), b(snrows, bCols);
    SharedTable R, S;

    if (partyIdx == 0) { // This is a very huge but useless initialization, but I don't know how to initialize a sharetable.
        R = srvs.localInput(a);
        S = srvs.localInput(b);
    } else {
        R = srvs.remoteInput(0);
        S = srvs.remoteInput(0);
    }
    GenDummyKey(partyIdx, srvs, rkey, R.mColumns[0]);

    GenDummyKey(partyIdx, srvs, skey, S.mColumns[0]);
    S.mColumns[1].mShares[0] = spayload.mShares[0];
    S.mColumns[1].mShares[1] = spayload.mShares[1];

    SelectQuery query;
	query.noReveal("r");
	query.joinOn(R["key"], S["key"]);
    query.addOutput("payload", query.addInput(S["payload"])); // only output payload as value
	auto out = srvs.joinImpl(query);

    rpayload.resize(rnrows, spayload.bitCount());
    rpayload.mShares[0] = out.mColumns[0].mShares[0];
    rpayload.mShares[1] = out.mColumns[0].mShares[1];

    SegmentedCopy(partyIdx, srvs, rkey, rpayload);
    return;
}

void PKFKJoin(u64 partyIdx, DBServer& srvs, SharedTable R, u64 RBKeyID, u64 ROrderKeyId,
            SharedTable S, u64 SBKeyID, u64 SOrderKeyId, SharedTable &T) {
    // Sort
    if (ROrderKeyId != RBKeyID) {
        ShuffleQuickSort(partyIdx, srvs, R, RBKeyID);
    }
    if (SOrderKeyId != SBKeyID) {
        ShuffleQuickSort(partyIdx, srvs, S, SBKeyID);
    }
    // PK-FK with payload
    u64 snrows = S.rows(), sncols = S.mColumns.size();
    sbMatrix spayload(snrows, sncols * 64);
    for (auto i=0; i<snrows; ++i) {
        for (auto j=0; j<sncols; ++j) {
            spayload.mShares[0](i, j) = S.mColumns[j].mShares[0](i);
            spayload.mShares[1](i, j) = S.mColumns[j].mShares[1](i);
        }
    }
    sbMatrix rpayload;
    PKFKwithPayload(partyIdx, srvs, R.mColumns[RBKeyID], S.mColumns[SBKeyID], spayload, rpayload);
    u64 rnrows = R.rows(), rncols = R.mColumns.size();
    T.mColumns.resize(rncols + sncols);
    for (auto j=0; j<rncols; ++j) {
        T.mColumns[j].resize(rnrows, 64);
        T.mColumns[j].mShares = R.mColumns[j].mShares;
    }
    for (auto j=0; j<sncols; ++j) {
        T.mColumns[rncols + j].resize(rnrows, 64);
        for (auto i=0; i<rnrows; ++i) {
            T.mColumns[rncols + j].mShares[0](i) = rpayload.mShares[0](i, j);
            T.mColumns[rncols + j].mShares[1](i) = rpayload.mShares[1](i, j);
        }
    }
    return;
}