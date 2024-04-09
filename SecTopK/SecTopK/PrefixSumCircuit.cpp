#include "PrefixSumCircuit.h"

void SBMatReveal(DBServer& srvs, sbMatrix mat) {
    aby3::i64Matrix out(mat.rows(), mat.i64Cols());
    srvs.mEnc.revealAll(srvs.mRt.mComm, mat, out);
    for (auto i=0; i<out.rows(); ++i) {
        for (auto j=0; j<out.cols(); ++j) {
            std::cerr << out(i, j) << ' ';
        }
        std::cerr << std::endl;
    }
}

void STableReveal(DBServer& srvs, SharedTable tab) {
    u64 nrows = tab.rows(), nattrs = tab.mColumns.size();
    std::vector<aby3::i64Matrix> RCols(nattrs);
    for (auto i=0; i<nattrs; ++i) {
        RCols[i].resize(nrows, 1);
        srvs.mEnc.revealAll(srvs.mRt.mComm, tab.mColumns[i], RCols[i]);
    }
    
    for (auto i=0; i<nrows; ++i) {
        for (auto j=0; j<nattrs; ++j) {
            std::cerr << RCols[j](i) << ' ';
        }
        std::cerr << std::endl;
    }
}

void STableReveal(DBServer& srvs, SharedTable tab, u64 limit) {
    u64 nrows = tab.rows(), nattrs = tab.mColumns.size();
    std::vector<aby3::i64Matrix> RCols(nattrs);
    for (auto i=0; i<nattrs; ++i) {
        RCols[i].resize(nrows, 1);
        srvs.mEnc.revealAll(srvs.mRt.mComm, tab.mColumns[i], RCols[i]);
    }
    
    for (auto i=0; i<limit; ++i) {
        for (auto j=0; j<nattrs; ++j) {
            std::cerr << RCols[j](i) << ' ';
        }
        std::cerr << std::endl;
    }
}


void PrefixSumTest(std::vector<uint32_t> a) {
    uint32_t n = a.size();
    uint32_t jump = 1;
    for (; jump < n; jump *= 2) {
        for (auto i=jump - 1; i+jump<n; i += jump * 2) {
            a[i + jump] += a[i];
        }
    }
    for (; jump >= 1; jump /= 2) {
        for (auto i=2 * jump - 1; i + jump <n; i += jump * 2) {
            a[i + jump] += a[i];
        }
    }
}

void PrefixSum(u64 partyIdx, DBServer& srvs, sbMatrix& val) {
    u64 nrows = val.rows();

    BetaLibrary lib;
    auto circ = lib.int_int_add(64, 64, 64, aby3::CircuitLibrary::Optimized::Depth);
    // using "aby3::CircuitLibrary::Optimized::Size" (default setting) has smaller size but larger depth.
    Sh3BinaryEvaluator eval;
    
    u64 jump = 1;
    for (; jump < nrows; jump *= 2) {
        u64 size = nrows / (jump * 2);
        sbMatrix a(size, 64), b(size, 64), c(size, 64);
        u64 counter = 0;
        for (auto idx = jump - 1; idx + jump < nrows; idx += jump * 2) {
            a.mShares[0](counter) = val.mShares[0](idx);
            a.mShares[1](counter) = val.mShares[1](idx);
            b.mShares[0](counter) = val.mShares[0](idx + jump);
            b.mShares[1](counter) = val.mShares[1](idx + jump);
            counter += 1;
        }
        if (counter == 0) continue;
        eval.setCir(circ, counter, srvs.mEnc.mShareGen);
        eval.setInput(0, a);
        eval.setInput(1, b);
        eval.asyncEvaluate(srvs.mRt.noDependencies()).get();
        eval.getOutput(0, c);
        counter = 0;
        for (auto idx = jump - 1; idx + jump < nrows; idx += jump * 2) {
            val.mShares[0](idx + jump) = c.mShares[0](counter);
            val.mShares[1](idx + jump) = c.mShares[1](counter);
            counter += 1;
        }
    }
    for ( jump /= 2 ; jump >= 1; jump /= 2) {
        u64 size = (nrows - jump) / (jump * 2);
        sbMatrix a(size, 64), b(size, 64), c(size, 64);
        u64 counter = 0;
        for (auto idx = 2*jump - 1; idx + jump < nrows; idx += jump * 2) {
            a.mShares[0](counter) = val.mShares[0](idx);
            a.mShares[1](counter) = val.mShares[1](idx);
            b.mShares[0](counter) = val.mShares[0](idx + jump);
            b.mShares[1](counter) = val.mShares[1](idx + jump);
            counter += 1;
        }
        if (counter == 0) continue;
        eval.setCir(circ, counter, srvs.mEnc.mShareGen);
        eval.setInput(0, a);
        eval.setInput(1, b);
        eval.asyncEvaluate(srvs.mRt.noDependencies()).get();
        eval.getOutput(0, c);
        counter = 0;
        for (auto idx = 2*jump - 1; idx + jump < nrows; idx += jump * 2) {
            val.mShares[0](idx + jump) = c.mShares[0](counter);
            val.mShares[1](idx + jump) = c.mShares[1](counter);
            counter += 1;
        }
    }
}

BetaCircuit EqualityCheckAndMux() { // ret c <- (k0 == k1) ? a : 0
    BetaCircuit r;
    BetaBundle a(64), k0(64), k1(64);
    BetaBundle c(1), t(1), out(64);
    r.addInputBundle(a);
    r.addInputBundle(k0);
    r.addInputBundle(k1);
    r.addTempWireBundle(c);
    r.addTempWireBundle(t);
    r.addOutputBundle(out);

    r.addGate(k0[0], k1[0], GateType::Nxor, t[0]);
    for (auto i=1; i<64; ++i) {
        r.addGate(k0[i], k1[i], GateType::Nxor, c[0]);
        r.addGate(c[0], t[0], GateType::And, t[0]);
    }
    for (auto i=0; i<64; ++i) {
        r.addGate(t[0], a[i], GateType::And, out[i]);
    }
    return r;
}

BetaCircuit EqualityCheckAndMuxDepthOptimized() { // ret c <- (k0 == k1) ? a : 0
    BetaCircuit r;
    BetaBundle a(64), k0(64), k1(64);
    BetaBundle t(64), out(64);
    r.addInputBundle(a);
    r.addInputBundle(k0);
    r.addInputBundle(k1);
    r.addTempWireBundle(t);
    r.addOutputBundle(out);

    for (auto i=0; i<64; ++i) {
        r.addGate(k0[i], k1[i], GateType::Nxor, t[i]);
    }

    for (auto l=64; l>1; l/=2) {
        for (auto i=0, j=l-1; i<j; ++i, --j) {
            r.addGate(t[i], t[j], GateType::And, t[i]);
        }
    }

    for (auto i=0; i<64; ++i) {
        r.addGate(t[0], a[i], GateType::And, out[i]);
    }
    return r;
}

void SegmentedPrefixSum(u64 partyIdx, DBServer& srvs, const sbMatrix key, sbMatrix& val) {
    u64 nrows = key.rows();
    BetaLibrary lib;
    auto circ_eqc_key = EqualityCheckAndMuxDepthOptimized();
    auto circ_add_val = lib.int_int_add(64, 64, 64, aby3::CircuitLibrary::Optimized::Depth);
    // using "aby3::CircuitLibrary::Optimized::Size" (default setting) has smaller size but larger depth.
    Sh3BinaryEvaluator eval_eqc, eval_add;
    
    u64 jump = 1;
    for (; jump < nrows; jump *= 2) {
        u64 size = nrows / (jump * 2);
        sbMatrix a(size, 64), b(size, 64), k0(size, 64), k1(size, 64), ta(size, 64), c(size, 64);
        u64 counter = 0;
        for (auto idx = jump - 1; idx + jump < nrows; idx += jump * 2) {
            a.mShares[0](counter) = val.mShares[0](idx);
            a.mShares[1](counter) = val.mShares[1](idx);
            b.mShares[0](counter) = val.mShares[0](idx + jump);
            b.mShares[1](counter) = val.mShares[1](idx + jump);
            k0.mShares[0](counter) = key.mShares[0](idx);
            k0.mShares[1](counter) = key.mShares[1](idx);
            k1.mShares[0](counter) = key.mShares[0](idx + jump);
            k1.mShares[1](counter) = key.mShares[1](idx + jump);
            counter += 1;
        }
        if (counter == 0) continue;
        eval_eqc.setCir(&circ_eqc_key, counter, srvs.mEnc.mShareGen);
        eval_eqc.setInput(0, a);
        eval_eqc.setInput(1, k0);
        eval_eqc.setInput(2, k1);
        eval_eqc.asyncEvaluate(srvs.mRt.noDependencies()).get();
        eval_eqc.getOutput(0, ta);

        eval_add.setCir(circ_add_val, counter, srvs.mEnc.mShareGen);
        eval_add.setInput(0, ta);
        eval_add.setInput(1, b);
        eval_add.asyncEvaluate(srvs.mRt.noDependencies()).get();
        eval_add.getOutput(0, c);

        counter = 0;
        for (auto idx = jump - 1; idx + jump < nrows; idx += jump * 2) {
            val.mShares[0](idx + jump) = c.mShares[0](counter);
            val.mShares[1](idx + jump) = c.mShares[1](counter);
            counter += 1;
        }
    }
    for ( jump /= 2 ; jump >= 1; jump /= 2) {
        u64 size = (nrows - jump) / (jump * 2);
        sbMatrix a(size, 64), b(size, 64), k0(size, 64), k1(size, 64), ta(size, 64), c(size, 64);
        u64 counter = 0;
        for (auto idx = 2*jump - 1; idx + jump < nrows; idx += jump * 2) {
            a.mShares[0](counter) = val.mShares[0](idx);
            a.mShares[1](counter) = val.mShares[1](idx);
            b.mShares[0](counter) = val.mShares[0](idx + jump);
            b.mShares[1](counter) = val.mShares[1](idx + jump);
            k0.mShares[0](counter) = key.mShares[0](idx);
            k0.mShares[1](counter) = key.mShares[1](idx);
            k1.mShares[0](counter) = key.mShares[0](idx + jump);
            k1.mShares[1](counter) = key.mShares[1](idx + jump);
            counter += 1;
        }
        if (counter == 0) continue;
        eval_eqc.setCir(&circ_eqc_key, counter, srvs.mEnc.mShareGen);
        eval_eqc.setInput(0, a);
        eval_eqc.setInput(1, k0);
        eval_eqc.setInput(2, k1);
        eval_eqc.asyncEvaluate(srvs.mRt.noDependencies()).get();
        eval_eqc.getOutput(0, ta);

        eval_add.setCir(circ_add_val, counter, srvs.mEnc.mShareGen);
        eval_add.setInput(0, ta);
        eval_add.setInput(1, b);
        eval_add.asyncEvaluate(srvs.mRt.noDependencies()).get();
        eval_add.getOutput(0, c);

        counter = 0;
        for (auto idx = 2*jump - 1; idx + jump < nrows; idx += jump * 2) {
            val.mShares[0](idx + jump) = c.mShares[0](counter);
            val.mShares[1](idx + jump) = c.mShares[1](counter);
            counter += 1;
        }
    }
}

BetaCircuit EqualityCheckDepthOptimized() { // ret c <- (k0 == k1) ? 1 : 0
    BetaCircuit r;
    BetaBundle k0(64), k1(64);
    BetaBundle t(64), out(1);

    r.addInputBundle(k0);
    r.addInputBundle(k1);
    r.addTempWireBundle(t);
    r.addOutputBundle(out);

    for (auto i=0; i<64; ++i) {
        r.addGate(k0[i], k1[i], GateType::Nxor, t[i]);
    }

    for (auto l=64; l>2; l/=2) {
        for (auto i=0, j=l-1; i<j; ++i, --j) {
            r.addGate(t[i], t[j], GateType::And, t[i]);
        }
    }
    r.addGate(t[0], t[1], GateType::And, out[0]);
    return r;
}

void NeighborEquality(u64 partyIdx, DBServer& srvs, const sbMatrix key, sbMatrix& tag) {
    u64 nrows = key.rows();
    BetaLibrary lib;
    auto circ_eqc = EqualityCheckDepthOptimized();
    Sh3BinaryEvaluator eval;

    sbMatrix k0(nrows - 1, 64), k1(nrows - 1, 64);
    for (auto i=0; i<nrows - 1; ++i) {
        k0.mShares[0](i) = key.mShares[0](i);
        k0.mShares[1](i) = key.mShares[1](i);

        k1.mShares[0](i) = key.mShares[0](i+1);
        k1.mShares[1](i) = key.mShares[1](i+1);
    }

    eval.setCir(&circ_eqc, nrows - 1, srvs.mEnc.mShareGen);
    eval.setInput(0, k0);
    eval.setInput(1, k1);
    eval.asyncEvaluate(srvs.mRt.noDependencies()).get();

    tag.resize(nrows - 1, 1);
    eval.getOutput(0, tag);
}

BetaCircuit EqualityCheckAndMuxRandDepthOptimized() { // ret c <- (k0 == k1) ? rk : k0
    BetaCircuit r;
    BetaBundle k0(64), k1(64), rk(80);
    BetaBundle t(64), tk0(64), ntrk(64), out(80);
    r.addInputBundle(k0);
    r.addInputBundle(k1);
    r.addInputBundle(rk);

    r.addTempWireBundle(t);
    r.addTempWireBundle(tk0);
    r.addTempWireBundle(ntrk);

    r.addOutputBundle(out);

    for (auto i=0; i<64; ++i) {
        r.addGate(k0[i], k1[i], GateType::Nxor, t[i]);
    }
    for (auto l=64; l>1; l/=2) {
        for (auto i=0, j=l-1; i<j; ++i, --j) {
            r.addGate(t[i], t[j], GateType::And, t[i]);
        }
    }
    // t[0] <- (k0 == k1)
    for (auto i=0; i<64; ++i) {
        r.addGate(t[0], k0[i], GateType::na_And, tk0[i]);
        r.addGate(t[0], rk[i], GateType::And, ntrk[i]);
    }
    for (auto i=0; i<64; ++i) {
        r.addGate(tk0[i], ntrk[i], GateType::Xor, out[i]);
    }
    for (auto i=64; i<80; ++i) {
        r.addGate(t[0], rk[i], GateType::And, out[i]);
    }
    return r;
}

void GenDummyKey(u64 partyIdx, DBServer& srvs, const sbMatrix key, sbMatrix& key_dum) { // key <- (key0 == key1) ? randkey : key0
    u64 nrows = key.rows();
    BetaLibrary lib;
    auto circ = EqualityCheckAndMuxRandDepthOptimized();
    Sh3BinaryEvaluator eval;

    sbMatrix k0(nrows - 1, 64), k1(nrows - 1, 64), randkey(nrows - 1, 80);

    block prevSeed = srvs.mEnc.mShareGen.mPrevCommon.getSeed();
    block nextSeed = srvs.mEnc.mShareGen.mNextCommon.getSeed();
    PRNG prevPrng(prevSeed), nextPrng(nextSeed);

    for (auto i=0; i<nrows - 1; ++i) {
        k0.mShares[0](i) = key.mShares[0](i);
        k0.mShares[1](i) = key.mShares[1](i);

        k1.mShares[0](i) = key.mShares[0](i+1);
        k1.mShares[1](i) = key.mShares[1](i+1);

        randkey.mShares[0](i, 0) = nextPrng.get<u64>();
        randkey.mShares[0](i, 1) = nextPrng.get<u64>();
        randkey.mShares[1](i, 0) = prevPrng.get<u64>();
        randkey.mShares[1](i, 1) = prevPrng.get<u64>();
        // std::cerr << randkey.mShares[0](i) << ' ' << randkey.mShares[1](i) << std::endl;
    }

    eval.setCir(&circ, nrows - 1, srvs.mEnc.mShareGen);
    eval.setInput(0, k0);
    eval.setInput(1, k1);
    eval.setInput(2, randkey);

    eval.asyncEvaluate(srvs.mRt.noDependencies()).get();

    sbMatrix out(nrows - 1, 80);
    eval.getOutput(0, out);

    // i64Matrix rout(nrows - 1, 2);
    // srvs.mEnc.revealAll(srvs.mRt, out, rout).get();
    // for (auto i=0; i<nrows - 1; ++i) {
    //     std::cerr << rout(i, 0) << ' ' << rout(i, 1) << std::endl;
    // }

    key_dum.resize(nrows, 80);
    for (auto i=0; i<nrows-1; ++i) {
        key_dum.mShares[0](i, 0) = out.mShares[0](i, 0);
        key_dum.mShares[0](i, 1) = out.mShares[0](i, 1);
        key_dum.mShares[1](i, 0) = out.mShares[1](i, 0);
        key_dum.mShares[1](i, 1) = out.mShares[1](i, 1);
    }
    key_dum.mShares[0](nrows - 1, 0) = key.mShares[0](nrows - 1);
    key_dum.mShares[0](nrows - 1, 1) = 0;
    key_dum.mShares[1](nrows - 1, 0) = key.mShares[1](nrows - 1);
    key_dum.mShares[1](nrows - 1, 1) = 0;
    return;
}

BetaCircuit DoubleEqualityCheckAndMux() { // ret c <- (k0 == k1) ? p0 : p1
    BetaCircuit r;
    BetaBundle a0(64), a1(64), b0(64), b1(64), k0(64), k1(64);
    BetaBundle t(64), ta0(64), nta1(64), a(64), tb0(64), ntb1(64), b(64);
    r.addInputBundle(a0);
    r.addInputBundle(a1);
    r.addInputBundle(b0);
    r.addInputBundle(b1);
    r.addInputBundle(k0);
    r.addInputBundle(k1);
    r.addTempWireBundle(t);
    r.addTempWireBundle(ta0);
    r.addTempWireBundle(nta1);
    r.addTempWireBundle(tb0);
    r.addTempWireBundle(ntb1);
    r.addOutputBundle(a);
    r.addOutputBundle(b);

    for (auto i=0; i<64; ++i) {
        r.addGate(k0[i], k1[i], GateType::Nxor, t[i]);
    }

    for (auto l=64; l>1; l/=2) {
        for (auto i=0, j=l-1; i<j; ++i, --j) {
            r.addGate(t[i], t[j], GateType::And, t[i]);
        }
    }

    for (auto i=0; i<64; ++i) {
        r.addGate(t[0], a0[i], GateType::And, ta0[i]);
        r.addGate(t[0], a1[i], GateType::na_And, nta1[i]);
    }
    for (auto i=0; i<64; ++i) {
        r.addGate(t[0], b0[i], GateType::And, tb0[i]);
        r.addGate(t[0], b1[i], GateType::na_And, ntb1[i]);
    }
    for (auto i=0; i<64; ++i) {
        r.addGate(ta0[i], nta1[i], GateType::Xor, a[i]);
        r.addGate(tb0[i], ntb1[i], GateType::Xor, b[i]);
    }
    return r;
}

BetaCircuit SingleEqualityCheckAndMux(u64 size) { // ret c <- (k0 == k1) ? p0 : p1
    BetaCircuit r;
    BetaBundle a0(size), a1(size), k0(64), k1(64);
    BetaBundle t(64), ta0(size), nta1(size), a(size);
    r.addInputBundle(a0);
    r.addInputBundle(a1);
    r.addInputBundle(k0);
    r.addInputBundle(k1);
    r.addTempWireBundle(t);
    r.addTempWireBundle(ta0);
    r.addTempWireBundle(nta1);
    r.addOutputBundle(a);

    for (auto i=0; i<64; ++i) {
        r.addGate(k0[i], k1[i], GateType::Nxor, t[i]);
    }

    for (auto l=64; l>1; l/=2) {
        for (auto i=0, j=l-1; i<j; ++i, --j) {
            r.addGate(t[i], t[j], GateType::And, t[i]);
        }
    }

    for (auto i=0; i<size; ++i) {
        r.addGate(t[0], a0[i], GateType::And, ta0[i]);
        r.addGate(t[0], a1[i], GateType::na_And, nta1[i]);
    }
    for (auto i=0; i<size; ++i) {
        r.addGate(ta0[i], nta1[i], GateType::Xor, a[i]);
    }
    return r;
}

void SegmentedCopy(u64 partyIdx, DBServer& srvs, const sbMatrix key, sbMatrix& value1) {
    u64 nrows = key.rows(), bitcnt = value1.bitCount(), ncols = bitcnt / 64;
    BetaLibrary lib;
    auto circ = SingleEqualityCheckAndMux(bitcnt);
    Sh3BinaryEvaluator eval;
    
    u64 jump = 1;
    for (; jump < nrows; jump *= 2) {
        u64 size = nrows / (jump * 2);
        sbMatrix a0(size, bitcnt), a1(size, bitcnt), k0(size, 64), k1(size, 64), a(size, bitcnt);
        u64 counter = 0;
        for (auto idx = jump - 1; idx + jump < nrows; idx += jump * 2) {
            u64 i = nrows - 1 - idx, j = nrows - 1 - idx - jump;
            for (auto k=0; k<ncols; ++k) {
                a0.mShares[0](counter, k) = value1.mShares[0](i, k);
                a0.mShares[1](counter, k) = value1.mShares[1](i, k);
                a1.mShares[0](counter, k) = value1.mShares[0](j, k);
                a1.mShares[1](counter, k) = value1.mShares[1](j, k);
            }
            

            k0.mShares[0](counter) = key.mShares[0](i);
            k0.mShares[1](counter) = key.mShares[1](i);
            k1.mShares[0](counter) = key.mShares[0](j);
            k1.mShares[1](counter) = key.mShares[1](j);

            counter += 1;
        }
        if (counter == 0) continue;
        eval.setCir(&circ, counter, srvs.mEnc.mShareGen);
        eval.setInput(0, a0);
        eval.setInput(1, a1);
        eval.setInput(2, k0);
        eval.setInput(3, k1);
        eval.asyncEvaluate(srvs.mRt.noDependencies()).get();
        eval.getOutput(0, a);

        counter = 0;
        for (auto idx = jump - 1; idx + jump < nrows; idx += jump * 2) {

            for (auto k=0; k<ncols; ++k) {
                value1.mShares[0](nrows - 1 - idx - jump, k) = a.mShares[0](counter, k);
                value1.mShares[1](nrows - 1 - idx - jump, k) = a.mShares[1](counter, k);
            }
            counter += 1;
        }
    }
    for ( jump /= 2 ; jump >= 1; jump /= 2) {
        u64 size = (nrows - jump) / (jump * 2);
        sbMatrix a0(size, bitcnt), a1(size, bitcnt), k0(size, 64), k1(size, 64), a(size, bitcnt);
        u64 counter = 0;
        for (auto idx = 2*jump - 1; idx + jump < nrows; idx += jump * 2) {
            u64 i = nrows - 1 - idx, j = nrows - 1 - idx - jump;

            for (auto k=0; k<ncols; ++k) {
                a0.mShares[0](counter, k) = value1.mShares[0](i, k);
                a0.mShares[1](counter, k) = value1.mShares[1](i, k);
                a1.mShares[0](counter, k) = value1.mShares[0](j, k);
                a1.mShares[1](counter, k) = value1.mShares[1](j, k);
            }

            k0.mShares[0](counter) = key.mShares[0](i);
            k0.mShares[1](counter) = key.mShares[1](i);
            k1.mShares[0](counter) = key.mShares[0](j);
            k1.mShares[1](counter) = key.mShares[1](j);

            counter += 1;
        }
        if (counter == 0) continue;
        eval.setCir(&circ, counter, srvs.mEnc.mShareGen);
        eval.setInput(0, a0);
        eval.setInput(1, a1);
        eval.setInput(2, k0);
        eval.setInput(3, k1);
        eval.asyncEvaluate(srvs.mRt.noDependencies()).get();
        eval.getOutput(0, a);

        counter = 0;
        for (auto idx = 2*jump - 1; idx + jump < nrows; idx += jump * 2) {
            for (auto k=0; k<ncols; ++k) {
                value1.mShares[0](nrows - 1 - idx - jump, k) = a.mShares[0](counter, k);
                value1.mShares[1](nrows - 1 - idx - jump, k) = a.mShares[1](counter, k);
            }
            counter += 1;
        }
    }
}

void SegmentedCopy(u64 partyIdx, DBServer& srvs, const sbMatrix key, sbMatrix& value1, sbMatrix& value2) {
    u64 nrows = key.rows();
    BetaLibrary lib;
    auto circ = DoubleEqualityCheckAndMux();
    Sh3BinaryEvaluator eval;
    
    u64 jump = 1;
    for (; jump < nrows; jump *= 2) {
        u64 size = nrows / (jump * 2);
        sbMatrix a0(size, 64), a1(size, 64), b0(size, 64), b1(size, 64), k0(size, 64), k1(size, 64), a(size, 64), b(size, 64);
        u64 counter = 0;
        for (auto idx = jump - 1; idx + jump < nrows; idx += jump * 2) {
            u64 i = nrows - 1 - idx, j = nrows - 1 - idx - jump;

            a0.mShares[0](counter) = value1.mShares[0](i);
            a0.mShares[1](counter) = value1.mShares[1](i);
            a1.mShares[0](counter) = value1.mShares[0](j);
            a1.mShares[1](counter) = value1.mShares[1](j);

            b0.mShares[0](counter) = value2.mShares[0](i);
            b0.mShares[1](counter) = value2.mShares[1](i);
            b1.mShares[0](counter) = value2.mShares[0](j);
            b1.mShares[1](counter) = value2.mShares[1](j);

            k0.mShares[0](counter) = key.mShares[0](i);
            k0.mShares[1](counter) = key.mShares[1](i);
            k1.mShares[0](counter) = key.mShares[0](j);
            k1.mShares[1](counter) = key.mShares[1](j);

            counter += 1;
        }
        if (counter == 0) continue;
        eval.setCir(&circ, counter, srvs.mEnc.mShareGen);
        eval.setInput(0, a0);
        eval.setInput(1, a1);
        eval.setInput(2, b0);
        eval.setInput(3, b1);
        eval.setInput(4, k0);
        eval.setInput(5, k1);
        eval.asyncEvaluate(srvs.mRt.noDependencies()).get();
        eval.getOutput(0, a);
        eval.getOutput(1, b);

        counter = 0;
        for (auto idx = jump - 1; idx + jump < nrows; idx += jump * 2) {
            value1.mShares[0](nrows - 1 - idx - jump) = a.mShares[0](counter);
            value1.mShares[1](nrows - 1 - idx - jump) = a.mShares[1](counter);
            value2.mShares[0](nrows - 1 - idx - jump) = b.mShares[0](counter);
            value2.mShares[1](nrows - 1 - idx - jump) = b.mShares[1](counter);
            counter += 1;
        }
    }
    for ( jump /= 2 ; jump >= 1; jump /= 2) {
        u64 size = (nrows - jump) / (jump * 2);
        sbMatrix a0(size, 64), a1(size, 64), b0(size, 64), b1(size, 64), k0(size, 64), k1(size, 64), a(size, 64), b(size, 64);
        u64 counter = 0;
        for (auto idx = 2*jump - 1; idx + jump < nrows; idx += jump * 2) {
            u64 i = nrows - 1 - idx, j = nrows - 1 - idx - jump;

            a0.mShares[0](counter) = value1.mShares[0](i);
            a0.mShares[1](counter) = value1.mShares[1](i);
            a1.mShares[0](counter) = value1.mShares[0](j);
            a1.mShares[1](counter) = value1.mShares[1](j);

            b0.mShares[0](counter) = value2.mShares[0](i);
            b0.mShares[1](counter) = value2.mShares[1](i);
            b1.mShares[0](counter) = value2.mShares[0](j);
            b1.mShares[1](counter) = value2.mShares[1](j);

            k0.mShares[0](counter) = key.mShares[0](i);
            k0.mShares[1](counter) = key.mShares[1](i);
            k1.mShares[0](counter) = key.mShares[0](j);
            k1.mShares[1](counter) = key.mShares[1](j);

            counter += 1;
        }
        if (counter == 0) continue;
        eval.setCir(&circ, counter, srvs.mEnc.mShareGen);
        eval.setInput(0, a0);
        eval.setInput(1, a1);
        eval.setInput(2, b0);
        eval.setInput(3, b1);
        eval.setInput(4, k0);
        eval.setInput(5, k1);
        eval.asyncEvaluate(srvs.mRt.noDependencies()).get();
        eval.getOutput(0, a);
        eval.getOutput(1, b);

        counter = 0;
        for (auto idx = 2*jump - 1; idx + jump < nrows; idx += jump * 2) {
            value1.mShares[0](nrows - 1 - idx - jump) = a.mShares[0](counter);
            value1.mShares[1](nrows - 1 - idx - jump) = a.mShares[1](counter);
            value2.mShares[0](nrows - 1 - idx - jump) = b.mShares[0](counter);
            value2.mShares[1](nrows - 1 - idx - jump) = b.mShares[1](counter);
            counter += 1;
        }
    }
}

BetaCircuit ZeroCheckAndMux() { // ret c <- (a == 0) ? v1 : v2
    BetaCircuit r;
    BetaBundle a(64), v1(64), v2(64);
    BetaBundle t(64), nav1(64), av2(64), out(64);
    r.addInputBundle(a);
    r.addInputBundle(v1);
    r.addInputBundle(v2);
    r.addTempWireBundle(t);
    r.addTempWireBundle(nav1);
    r.addTempWireBundle(av2);
    r.addOutputBundle(out);

    for (auto l=64; l>1; l/=2) {
        // a or b = (a xor b) xor (a and b)
        for (auto i=0, j=l-1; i<j; ++i, --j) {
            r.addGate(a[i], a[j], GateType::Xor, t[i]);
            r.addGate(a[i], a[j], GateType::And, t[j]);
        }
        for (auto i=0, j=l-1; i<j; ++i, --j) {
            r.addGate(t[i], t[j], GateType::Xor, a[i]);
        }
    }

    for (auto i=0; i<64; ++i) {
        r.addGate(a[0], v1[i], GateType::na_And, nav1[i]);
        r.addGate(a[0], v2[i], GateType::And, av2[i]);
    }

    for (auto i=0; i<64; ++i) {
        r.addGate(nav1[i], av2[i], GateType::Xor, out[i]);
    }
    return r;
}

BetaCircuit CopyDuplicateGate(u64 numbits) {
    BetaCircuit r;
    BetaBundle v1(numbits), v2(numbits);
    BetaBundle ntv1(numbits), tv2(numbits), out(numbits);
    r.addInputBundle(v1);
    r.addInputBundle(v2);
    r.addTempWireBundle(ntv1);
    r.addTempWireBundle(tv2);
    r.addOutputBundle(out);
    for (auto i=0; i<numbits; ++i) {
        r.addGate(v2[numbits - 64], v1[i], GateType::na_And, ntv1[i]);
        if (i != numbits - 64) {
            r.addGate(v2[numbits - 64], v2[i], GateType::And, tv2[i]);
        }
    }
    for (auto i=0; i<numbits; ++i) {
        if (i != numbits - 64) {
            r.addGate(ntv1[i], tv2[i], GateType::Xor, out[i]);
        } else {
            r.addGate(ntv1[i], v2[i], GateType::Xor, out[i]);
        }   
    }
    return r;
}

void CopyDuplicate(u64 partyIdx, DBServer& srvs, SharedTable& RE) {
    u64 nrows = RE.rows(), nattrs = RE.mColumns.size(), nbits = 64 * nattrs;
    sbMatrix tempin(nrows, nbits);
    for (auto i=0; i<nrows; ++i) {
        for (auto j=0; j<nattrs; ++j) {
            tempin.mShares[0](i, j) = RE.mColumns[j].mShares[0](i);
            tempin.mShares[1](i, j) = RE.mColumns[j].mShares[1](i);
        }
    }

    BetaLibrary lib;
    auto circ = CopyDuplicateGate(nbits);
    Sh3BinaryEvaluator eval;

    // SBMatReveal(srvs, tempin);
    u64 jump = 1;
    for (; jump < nrows; jump *= 2) {
        u64 size = nrows / (jump * 2);
        sbMatrix v1(size, nbits), v2(size, nbits), out(size, nbits);
        u64 counter = 0;
        for (auto idx = jump - 1; idx + jump < nrows; idx += jump * 2) {
            for (auto j=0; j<tempin.i64Cols(); ++j) {
                v1.mShares[0](counter, j) = tempin.mShares[0](idx, j);
                v1.mShares[1](counter, j) = tempin.mShares[1](idx, j);
                v2.mShares[0](counter, j) = tempin.mShares[0](idx + jump, j);
                v2.mShares[1](counter, j) = tempin.mShares[1](idx + jump, j);
            }
            counter += 1;
        }
        if (counter == 0) continue;
        eval.setCir(&circ, counter, srvs.mEnc.mShareGen);
        eval.setInput(0, v1);
        eval.setInput(1, v2);
        eval.asyncEvaluate(srvs.mRt.noDependencies()).get();
        eval.getOutput(0, out);
        counter = 0;
        for (auto idx = jump - 1; idx + jump < nrows; idx += jump * 2) {
            for (auto j=0; j<tempin.i64Cols(); ++j) {
                tempin.mShares[0](idx + jump, j) = out.mShares[0](counter, j);
                tempin.mShares[1](idx + jump, j) = out.mShares[1](counter, j);
            }
            counter += 1;
        }
    }
    for ( jump /= 2 ; jump >= 1; jump /= 2) {
        u64 size = (nrows - jump) / (jump * 2);
        sbMatrix v1(size, nbits), v2(size, nbits), out(size, nbits);
        u64 counter = 0;
        for (auto idx = 2*jump - 1; idx + jump < nrows; idx += jump * 2) {
            for (auto j=0; j<tempin.i64Cols(); ++j) {
                v1.mShares[0](counter, j) = tempin.mShares[0](idx, j);
                v1.mShares[1](counter, j) = tempin.mShares[1](idx, j);
                v2.mShares[0](counter, j) = tempin.mShares[0](idx + jump, j);
                v2.mShares[1](counter, j) = tempin.mShares[1](idx + jump, j);
            }
            counter += 1;
        }
        if (counter == 0) continue;
        eval.setCir(&circ, counter, srvs.mEnc.mShareGen);
        eval.setInput(0, v1);
        eval.setInput(1, v2);
        eval.asyncEvaluate(srvs.mRt.noDependencies()).get();
        eval.getOutput(0, out);
        counter = 0;
        for (auto idx = 2*jump - 1; idx + jump < nrows; idx += jump * 2) {
            for (auto j=0; j<tempin.i64Cols(); ++j) {
                tempin.mShares[0](idx + jump, j) = out.mShares[0](counter, j);
                tempin.mShares[1](idx + jump, j) = out.mShares[1](counter, j);
            }
            counter += 1;
        }
    }

    RE.mColumns.pop_back(); // remove the tag
    for (auto i=0; i<nrows; ++i) {
        for (auto j=0; j<nattrs - 1; ++j) {
            RE.mColumns[j].mShares[0](i) = tempin.mShares[0](i, j);
            RE.mColumns[j].mShares[1](i) = tempin.mShares[1](i, j);
        }
    }
}

BetaCircuit EqualCheckAndMux() {
    BetaCircuit r;
    BetaBundle k0(64), k1(64), a0(64), a1(64);
    BetaBundle t(64), ta0(64), nta1(64), out(64);

    r.addInputBundle(k0);
    r.addInputBundle(k1);
    r.addInputBundle(a0);
    r.addInputBundle(a1);
    r.addTempWireBundle(t);
    r.addTempWireBundle(ta0);
    r.addTempWireBundle(nta1);
    r.addOutputBundle(out);

    for (auto i=0; i<64; ++i) {
        r.addGate(k0[i], k1[i], GateType::Nxor, t[i]);
    }
    for (auto l=64; l>1; l/=2) {
        for (auto i=0, j=l-1; i<j; ++i, --j) {
            r.addGate(t[i], t[j], GateType::And, t[i]);
        }
    }
    for (auto i=0; i<64; ++i) {
        r.addGate(t[0], a0[i], GateType::And, ta0[i]);
        r.addGate(t[0], a1[i], GateType::na_And, nta1[i]);
    }
    for (auto i=0; i<64; ++i) {
        r.addGate(ta0[i], nta1[i], GateType::Xor, out[i]);
    }

    return r;
}

void CalcAlignmentPI(u64 partyIdx, DBServer& srvs, SharedTable& RE, u64 pkid, u64 bid) {
    u64 nrows = RE.rows(), ncols = RE.mColumns.size();
    sbMatrix colb(nrows, 64), pk(nrows, 64), colj(nrows, 64), coliq(nrows, 64), pos(nrows, 64);
    pk.mShares = RE.mColumns[pkid].mShares;
    colb.mShares = RE.mColumns[bid].mShares;
    colj.mShares = RE.mColumns[ncols - 2].mShares;
    coliq.mShares = RE.mColumns[ncols - 1].mShares;
    for (auto i=0; i<nrows; ++i) {
        pos.mShares[0](i) = pos.mShares[1](i) = i;
    }
    BetaLibrary lib;
    auto circ = EqualCheckAndMux();
    Sh3BinaryEvaluator eval;

    // Get POS
    u64 jump = 1;
    for (; jump < nrows; jump *= 2) {
        u64 size = nrows / (jump * 2);
        sbMatrix k0(size, 64), k1(size, 64), a0(size, 64), a1(size, 64), out(size, 64);
        u64 counter = 0;
        for (auto idx = jump - 1; idx + jump < nrows; idx += jump * 2) {
            k0.mShares[0](counter) = colb.mShares[0](idx);
            k0.mShares[1](counter) = colb.mShares[1](idx);
            k1.mShares[0](counter) = colb.mShares[0](idx + jump);
            k1.mShares[1](counter) = colb.mShares[1](idx + jump);

            a0.mShares[0](counter) = pos.mShares[0](idx);
            a0.mShares[1](counter) = pos.mShares[1](idx);
            a1.mShares[0](counter) = pos.mShares[0](idx + jump);
            a1.mShares[1](counter) = pos.mShares[1](idx + jump);
            counter += 1;
        }
        if (counter == 0) continue;
        eval.setCir(&circ, counter, srvs.mEnc.mShareGen);
        eval.setInput(0, k0);
        eval.setInput(1, k1);
        eval.setInput(2, a0);
        eval.setInput(3, a1);
        eval.asyncEvaluate(srvs.mRt.noDependencies()).get();
        eval.getOutput(0, out);
        counter = 0;
        for (auto idx = jump - 1; idx + jump < nrows; idx += jump * 2) {
            pos.mShares[0](idx + jump) = out.mShares[0](counter);
            pos.mShares[1](idx + jump) = out.mShares[1](counter);
            counter += 1;
        }
    }
    for ( jump /= 2 ; jump >= 1; jump /= 2) {
        u64 size = (nrows - jump) / (jump * 2);
        sbMatrix k0(size, 64), k1(size, 64), a0(size, 64), a1(size, 64), out(size, 64);
        u64 counter = 0;
        for (auto idx = 2 * jump - 1; idx + jump < nrows; idx += jump * 2) {
            k0.mShares[0](counter) = colb.mShares[0](idx);
            k0.mShares[1](counter) = colb.mShares[1](idx);
            k1.mShares[0](counter) = colb.mShares[0](idx + jump);
            k1.mShares[1](counter) = colb.mShares[1](idx + jump);

            a0.mShares[0](counter) = pos.mShares[0](idx);
            a0.mShares[1](counter) = pos.mShares[1](idx);
            a1.mShares[0](counter) = pos.mShares[0](idx + jump);
            a1.mShares[1](counter) = pos.mShares[1](idx + jump);
            counter += 1;
        }
        if (counter == 0) continue;
        eval.setCir(&circ, counter, srvs.mEnc.mShareGen);
        eval.setInput(0, k0);
        eval.setInput(1, k1);
        eval.setInput(2, a0);
        eval.setInput(3, a1);
        eval.asyncEvaluate(srvs.mRt.noDependencies()).get();
        eval.getOutput(0, out);
        counter = 0;
        for (auto idx = 2 * jump - 1; idx + jump < nrows; idx += jump * 2) {
            pos.mShares[0](idx + jump) = out.mShares[0](counter);
            pos.mShares[1](idx + jump) = out.mShares[1](counter);
            counter += 1;
        }
    }
    // std::cerr << "pos" << std::endl;
    // SBMatReveal(srvs, pos);
    // std::cerr << std::endl;

    // Get sum(col_iq)
    SegmentedPrefixSum(partyIdx, srvs, pk, coliq);
    // std::cerr << "sum col iq" << std::endl;
    // SBMatReveal(srvs, coliq);

    sbMatrix k0(nrows - 1, 64), k1(nrows - 1, 64), val(nrows - 1, 64), out(nrows - 1, 64);
    for (auto i=1; i<nrows; ++i) {
        k0.mShares[0](i-1) = pk.mShares[0](i-1);
        k0.mShares[1](i-1) = pk.mShares[1](i-1);
        k1.mShares[0](i-1) = pk.mShares[0](i);
        k1.mShares[1](i-1) = pk.mShares[1](i);
        val.mShares[0](i-1) = coliq.mShares[0](i-1);
        val.mShares[1](i-1) = coliq.mShares[1](i-1);
    }
    auto circ_mux = EqualityCheckAndMuxDepthOptimized();
    eval.setCir(&circ_mux, nrows - 1, srvs.mEnc.mShareGen);
    eval.setInput(0, val);
    eval.setInput(1, k0);
    eval.setInput(2, k1);
    eval.asyncEvaluate(srvs.mRt.noDependencies()).get();
    eval.getOutput(0, out);
    for (auto i=0; i<nrows; ++i) {
        if (i == 0) {
            coliq.mShares[0](i) = coliq.mShares[1](i) = 0;
        } else {
            coliq.mShares[0](i) = out.mShares[0](i-1);
            coliq.mShares[1](i) = out.mShares[1](i-1);
        }
    }
    // std::cerr << "col iq" << std::endl;
    // SBMatReveal(srvs, coliq);
    // std::cerr << std::endl;

    auto circ_add = lib.int_int_add(64, 64, 64, aby3::CircuitLibrary::Optimized::Depth);
    auto circ_sub = lib.int_int_subtract(64, 64, 64);

    Sh3BinaryEvaluator eval_add, eval_sub;

    sbMatrix posaddcolj(nrows, 64), res(nrows, 64);

    eval_add.setCir(circ_add, nrows, srvs.mEnc.mShareGen);
    eval_add.setInput(0, pos);
    eval_add.setInput(1, colj);
    eval_add.asyncEvaluate(srvs.mRt.noDependencies()).get();
    eval_add.getOutput(0, posaddcolj);
    // std::cerr << "pos + colj" << std::endl;
    // SBMatReveal(srvs, posaddcolj);
    // std::cerr << std::endl;

    eval.setCir(circ_add, nrows, srvs.mEnc.mShareGen);
    eval.setInput(0, posaddcolj);
    eval.setInput(1, coliq);
    eval.asyncEvaluate(srvs.mRt.noDependencies()).get();
    eval.getOutput(0, res);
    // std::cerr << "final" << std::endl;
    // SBMatReveal(srvs, res);
    // std::cerr << std::endl;

    RE.mColumns.pop_back();
    ncols = RE.mColumns.size();
    RE.mColumns[ncols - 1].mShares = res.mShares;
}

BetaCircuit LessThan(u64 size) {
    BetaCircuit r;
    BetaBundle a(size), b(size);
    BetaBundle eq(size), lt(size), seq(size), eqlt(size);
    BetaBundle c(1);
    r.addInputBundle(a);
    r.addInputBundle(b);

    r.addTempWireBundle(eq);
    r.addTempWireBundle(lt);
    r.addTempWireBundle(seq);
    r.addTempWireBundle(eqlt);

    r.addOutputBundle(c);

    // eq[i] = (a[i] == b[i])
    // lt[i] = (a[i] < b[i])
    for (auto i=0; i<size; ++i) {
        r.addGate(a[size - 1 - i], b[size - 1 - i], GateType::Nxor, eq[i]);
        r.addGate(a[size - 1 - i], b[size - 1 - i], GateType::na_And, lt[i]);
    }

    // seq[i] = (eq[0] and eq[1] and ... and eq[i])
    for (auto i=1; i<size; ++i) {
        if (i == 1) {
            r.addGate(eq[i-1], eq[i], GateType::And, seq[i]);
        } else {
            r.addGate(seq[i-1], eq[i], GateType::And, seq[i]);
        }
    }

    // eqlt[i] = (seq[i-1] and lt[i])
    for (auto i=1; i<size; ++i) {
        if (i == 1) {
            r.addGate(eq[i-1], lt[i], GateType::And, eqlt[i]);
            r.addGate(lt[i-1], eqlt[i], GateType::Xor, c[0]);
        } else {
            r.addGate(seq[i-1], lt[i], GateType::And, eqlt[i]);
            r.addGate(c[0], eqlt[i], GateType::Xor, c[0]);
        }
    }
    
    return r;
}

BetaCircuit LessThanAndMux(u64 size) { // (a < b) ? out : 0
    BetaCircuit r;
    BetaBundle a(size), b(size), value(64);
    BetaBundle eq(size), lt(size), seq(size), eqlt(size);
    BetaBundle c(1);
    BetaBundle out(64);

    r.addInputBundle(a);
    r.addInputBundle(b);
    r.addInputBundle(value);

    r.addTempWireBundle(eq);
    r.addTempWireBundle(lt);
    r.addTempWireBundle(seq);
    r.addTempWireBundle(eqlt);
    r.addTempWireBundle(c);

    r.addOutputBundle(out);

    // eq[i] = (a[i] == b[i])
    // lt[i] = (a[i] < b[i])
    for (auto i=0; i<size; ++i) {
        r.addGate(a[size - 1 - i], b[size - 1 - i], GateType::Nxor, eq[i]);
        r.addGate(a[size - 1 - i], b[size - 1 - i], GateType::na_And, lt[i]);
    }

    // seq[i] = (eq[0] and eq[1] and ... and eq[i])
    for (auto i=1; i<size; ++i) {
        if (i == 1) {
            r.addGate(eq[i-1], eq[i], GateType::And, seq[i]);
        } else {
            r.addGate(seq[i-1], eq[i], GateType::And, seq[i]);
        }
    }

    // eqlt[i] = (seq[i-1] and lt[i])
    for (auto i=1; i<size; ++i) {
        if (i == 1) {
            r.addGate(eq[i-1], lt[i], GateType::And, eqlt[i]);
            r.addGate(lt[i-1], eqlt[i], GateType::Xor, c[0]);
        } else {
            r.addGate(seq[i-1], lt[i], GateType::And, eqlt[i]);
            r.addGate(c[0], eqlt[i], GateType::Xor, c[0]);
        }
    }

    for (auto i=0; i<size; ++i) {
        r.addGate(c[0], value[i], GateType::And, out[i]);
    }
    
    return r;
}