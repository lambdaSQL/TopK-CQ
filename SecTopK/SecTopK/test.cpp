#include <iostream>
#include <vector>

#include <cryptoTools/Network/IOService.h>
#include "cryptoTools/Common/Timer.h"
#include "aby3/sh3/Sh3Runtime.h"
#include "aby3/sh3/Sh3Encryptor.h"
#include "aby3/sh3/Sh3Evaluator.h"
#include "aby3/sh3/Sh3BinaryEvaluator.h"
#include "aby3-DB/DBServer.h"

#include "Permutation.h"
#include "PrefixSumCircuit.h"
#include "join.h"
#include "sort.h"
#include "ProductK.h"
#include "LevelK.h"

using namespace oc;
using namespace aby3;

// =========================================

void CheckProjectSetup() {
    std::cout << "==================================================\n";
    std::cout << "Hello World!\n";
    std::cout << "SMQP is merged into the project!\n";
    std::cout << "==================================================\n";
}

void Setup(u64 partyIdx, IOService& ios, Sh3Encryptor& enc, Sh3Evaluator& eval, Sh3Runtime& runtime) {
	CommPkg comm;
	switch (partyIdx) {
        case 0:
            comm.mNext = Session(ios, "127.0.0.1:1111", SessionMode::Server, "01").addChannel();
            comm.mPrev = Session(ios, "127.0.0.1:2222", SessionMode::Server, "02").addChannel();
            break;
        case 1:
            comm.mNext = Session(ios, "127.0.0.1:3333", SessionMode::Server, "12").addChannel();
            comm.mPrev = Session(ios, "127.0.0.1:1111", SessionMode::Client, "01").addChannel();
            break;
        default:
            comm.mNext = Session(ios, "127.0.0.1:2222", SessionMode::Client, "02").addChannel();
            comm.mPrev = Session(ios, "127.0.0.1:3333", SessionMode::Client, "12").addChannel();
            break;
	}

	enc.init(partyIdx, comm, sysRandomSeed());
	eval.init(partyIdx, comm, sysRandomSeed());
	runtime.init(partyIdx, comm);
}

void DBSvrSetup(u64 partyIdx, IOService& ios, DBServer& srvs) {
    // PRNG prng(oc::ZeroBlock);
	if (partyIdx == 0) {
        PRNG prng(oc::toBlock(u64(0), u64(0)));
        Session s01(ios, "127.0.0.1:1111", SessionMode::Server, "01");
        Session s02(ios, "127.0.0.1:2222", SessionMode::Server, "02");
        srvs.init(0, s02, s01, prng);
    } else if (partyIdx == 1) {
        PRNG prng(oc::toBlock(u64(0), u64(1)));
        Session s10(ios, "127.0.0.1:1111", SessionMode::Client, "01");
        Session s12(ios, "127.0.0.1:3333", SessionMode::Server, "12");
        srvs.init(1, s10, s12, prng);
    } else {
        PRNG prng(oc::toBlock(u64(0), u64(2)));
        Session s20(ios, "127.0.0.1:2222", SessionMode::Client, "02");
    	Session s21(ios, "127.0.0.1:3333", SessionMode::Client, "12");
        srvs.init(2, s21, s20, prng);
    }
}

void CheckInputAndOutput(u64 partyIdx) {
    // std::cerr << "Current ID " << partyIdx << std::endl;
    IOService ios;

	Sh3Encryptor enc;
	Sh3Evaluator eval;
	Sh3Runtime runtime;

    Timer t;
    t.setTimePoint("start");

    Setup(partyIdx, ios, enc, eval, runtime);
    t.setTimePoint("setup");

    si64 sval;
    sb64 sbval;
    // si64 is an arithmetic sharing of int64
    // sb64 is a Boolean sharing of int64

    // Each party send its share by using localInt / localBinary
    // If it does not hold a piece of share, use remoteInt / remoteBinary
    Sh3Task inputTask;
    if (true) {
        inputTask = enc.localInt(runtime, partyIdx * 11, sval);
        inputTask &= enc.localBinary(runtime, partyIdx * 11, sbval);
    } else {
        inputTask = enc.remoteInt(runtime, sval);
    }
    inputTask.get();
    t.setTimePoint("input");

    i64 val;
    i64 bval;

    Sh3Task outputTask;
    outputTask = enc.revealAll(runtime, sval, val); // reveal to all parties
    if (partyIdx == 0) {
        outputTask &= enc.reveal(runtime, sbval, bval); // party 0 gets the result
    } else {
        outputTask &= enc.reveal(runtime, 0, sbval); // reveal to party 0
    }
    outputTask.get();
    t.setTimePoint("output");

    // std::string filename = "outputlog" + std::to_string(partyIdx) + ".log";
    // freopen(filename.c_str(), "w", stdout);

    if (partyIdx == 0) {
        std::cout << " output value = " << val << std::endl;
        std::cout << " output binary value = " << bval << std::endl;
        std::cout << std::endl;
        std::cout << t << std::endl;
    }
    std::cout << "Party " << partyIdx << " comm cost = " << runtime.mComm.mNext.getTotalDataSent() + runtime.mComm.mPrev.getTotalDataSent() << std::endl;
}

void CheckPSI(u64 partyIdx) {
    IOService ios;
    DBServer srvs;
    Timer t;
    t.setTimePoint("start");

    DBSvrSetup(partyIdx, ios, srvs);
    t.setTimePoint("setup");

    std::vector<ColumnInfo> aCols = { ColumnInfo{ "key", TypeID::IntID, srvs.mKeyBitCount } };
    std::vector<ColumnInfo> bCols = { ColumnInfo{ "key", TypeID::IntID, srvs.mKeyBitCount } , ColumnInfo{ "payload", TypeID::IntID, 64 } };

    u64 arows = 150000, brows = 100000;
    Table a(arows, aCols), b(brows, bCols);

    for (auto i=0; i<arows; ++i) {
        for (auto j=0; j<a.mColumns.size(); ++j) {
            a.mColumns[j].mData(i, 1) = 0;
            a.mColumns[j].mData(i, 0) = i / 10 + 1;
            // if (partyIdx == 0) std::cerr << a.mColumns[j].mData(i, 0) << ' ';
        }
        // if (partyIdx == 0) std::cerr << std::endl;
    }

    for (auto i=0; i<brows; ++i) {
        for (auto j=0; j<b.mColumns.size(); ++j) {
            if (j == 0) {
                b.mColumns[j].mData(i, 1) = 0;
                b.mColumns[j].mData(i, 0) = i + 1;
            } else {
                b.mColumns[j].mData(i, 0) = (i + 1) * 11; //10 - i + j * 10;
            }
            // if (partyIdx == 0) std::cerr << b.mColumns[j].mData(i, 0) << ' ';
        }
        // if (partyIdx == 0) std::cerr << std::endl;
    }

    SharedTable A, B;

    if (partyIdx == 0) {
        A = srvs.localInput(a);
        B = srvs.localInput(b);
    } else {
        A = srvs.remoteInput(0);
        B = srvs.remoteInput(0);
    }

    t.setTimePoint("inputs");
    srvs.setTimer(t);

    SelectQuery query;
	query.noReveal("r");
	query.joinOn(A["key"], B["key"]);
	// query.addOutput("key", aKey);
    query.addOutput("payload", query.addInput(B["payload"])); // only output payload as value

	auto AB = srvs.joinImpl(query);

    // AB = A Semijoin B
    // AB has two columns: payload and indicator (joined or not)
    // AB keeps the same size of A

    t.setTimePoint("intersect");

    aby3::i64Matrix out(AB.mColumns[0].rows(), AB.mColumns[0].i64Cols());

    // AB.mColumns[0].mShares[0 / 1](i)
    // is the first / second piece of shares from AB(0, i)

    // for (auto i=0; i<AB.mColumns[0].rows(); ++i) {
    //     std::string str = std::to_string(partyIdx) + "," + std::to_string(i) + ":" + std::to_string(AB.mColumns[0].mShares[0](i)) + "," + std::to_string(AB.mColumns[0].mShares[1](i));
    //     std::cout << str << std::endl;
    // }

    srvs.mEnc.revealAll(srvs.mRt.mComm, AB.mColumns[0], out);
    
    t.setTimePoint("reveal");

    if (partyIdx == 0) {
        for (auto i=0; i<out.rows(); ++i) {
            for (auto j=0; j<out.cols(); ++j) {
                std::cout << out(i, j);
            }
            std::cout << std::endl;
        }
    }

    std::cout << t << std::endl;
    std::cout << "comm = " << srvs.mRt.mComm.mNext.getTotalDataSent() + srvs.mRt.mComm.mPrev.getTotalDataSent() << std::endl;
}

void CheckSharedTable(u64 partyIdx) {
    IOService ios;
    DBServer srvs;
    DBSvrSetup(partyIdx, ios, srvs);

    std::vector<ColumnInfo> ColInfo = { ColumnInfo{ "A", TypeID::IntID, 64 } , ColumnInfo{ "B", TypeID::IntID, 64 } };
    u64 nrows = 10;
    Table R(nrows, ColInfo);
    for (auto i=0; i<nrows; ++i) {
        for (auto j=0; j<R.mColumns.size(); ++j) {
            R.mColumns[j].mData(i, 0) = i + j * nrows; // We set 64 bits for each key, thus it uses one int64 to store.
        }
    }
    SharedTable SR;
    if (partyIdx == 0) {
        SR = srvs.localInput(R);
    } else {
        SR = srvs.remoteInput(0); // party 0 inputs the table
    }

    std::vector<u64> localPerm = {3, 2, 1, 0, 4, 5, 6, 9, 8, 7};

    // Local Permutation

    // SharedTable PSR (SR);
    SharedTable PSR;
    PSR.mColumns.resize(2);
    PSR.mColumns[0].resize(nrows, 64);
    PSR.mColumns[1].resize(nrows, 64);

    for (auto i=0; i<PSR.mColumns.size(); ++i) {
        for (auto j=0; j<nrows; ++j) {
            for (auto k=0; k<PSR.mColumns[i].mShares[0].cols(); ++k) {
                PSR.mColumns[i].mShares[0](j, k) = SR.mColumns[i].mShares[0](localPerm[j], k);
                PSR.mColumns[i].mShares[1](j, k) = SR.mColumns[i].mShares[1](localPerm[j], k);
            }
        }
    }

    aby3::i64Matrix ACols(nrows, 1), BCols(nrows, 1);
    if (partyIdx == 1) {
        srvs.mEnc.reveal(srvs.mRt.mComm, PSR.mColumns[0], ACols);
        srvs.mEnc.reveal(srvs.mRt.mComm, PSR.mColumns[1], BCols);
    } else {
        srvs.mEnc.reveal(srvs.mRt.mComm, 1, PSR.mColumns[0]);
        srvs.mEnc.reveal(srvs.mRt.mComm, 1, PSR.mColumns[1]);
    }
    if (partyIdx == 1) {
        for (auto i=0; i<nrows; ++i) {
            std::cout << ACols(i, 0) << ' ' << BCols(i, 0) << std::endl;
        }
    }
}

BetaCircuit GenMuxCircuit() { // return t ? b : a
    BetaCircuit r;
    BetaBundle a(64), b(64), t(1), ant(64), bt(64), out(64);
    r.addInputBundle(a);
    r.addInputBundle(b);
    r.addInputBundle(t);
    r.addTempWireBundle(ant);
    r.addTempWireBundle(bt);
    r.addOutputBundle(out);
    for (auto i=0; i<64; ++i) {
        r.addGate(t[0], a[i], GateType::na_And, ant[i]);
        r.addGate(b[i], t[0], GateType::And, bt[i]);
        r.addGate(ant[i], bt[i], GateType::Xor, out[i]);
    }
    return r;
}

void BasicCircuitConstrcution(u64 partyIdx) {
    IOService ios;
    DBServer srvs;
    DBSvrSetup(partyIdx, ios, srvs);

    std::vector<ColumnInfo> ColInfo = { ColumnInfo{ "A", TypeID::IntID, 64 } , ColumnInfo{ "B", TypeID::IntID, 64 } , ColumnInfo{ "T", TypeID::IntID, 1 } };
	u64 nrows = 10;
    Table R(nrows, ColInfo);
    for (auto i=0; i<nrows; ++i) {
        for (auto j=0; j<R.mColumns.size(); ++j) {
            if (j < 2) {
                R.mColumns[j].mData(i, 0) = i + j; // We set 64 bits for each key, thus it uses one int64 to store.
            } else {
                R.mColumns[j].mData(i, 0) = 1 - i % 2;
            }
        }
    }
    SharedTable SR;
    if (partyIdx == 0) {
        SR = srvs.localInput(R);
    } else {
        SR = srvs.remoteInput(0); // party 0 inputs the table
    }

    BetaLibrary lib;
    auto circ = lib.int_int_add(64, 64, 64);

    Sh3BinaryEvaluator eval;
    eval.setCir(circ, nrows, srvs.mEnc.mShareGen);

    u64 i=0;
    eval.setInput(i++, SR.mColumns[1]);
    eval.setInput(i++, SR.mColumns[0]);

    sPackedBin outs(nrows, 64);
    eval.asyncEvaluate(srvs.mRt.noDependencies()).get();
    eval.getOutput(0, outs);
    srvs.mRt.runAll();

    i64Matrix outf(nrows, 1);
    srvs.mEnc.revealAll(srvs.mRt.noDependencies(), outs, outf).get();
    if (partyIdx == 0) {
        for (auto i=0; i<nrows; ++i) {
            std::cerr << i + 1 << " - " << i << ' ' << outf(i, 0) << std::endl;
        }
    }

    // Sh3BinaryEvaluator eval1;
    // eval1.setCir(circ, nrows, srvs.mEnc.mShareGen);
    // eval1.setInput(0, outs);
    // eval1.setInput(1, outs);
    // eval1.asyncEvaluate(srvs.mRt.noDependencies()).get();
    // sbMatrix out(nrows, 64);
    // eval1.getOutput(0, out);

    // i64Matrix outf(nrows, 1);
    // srvs.mEnc.revealAll(srvs.mRt.noDependencies(), out, outf).get();
    // if (partyIdx == 0) {
    //     for (auto i=0; i<nrows; ++i) {
    //         std::cerr << i << ':' << i + 1 << ' ' << outf(i, 0) << std::endl;
    //     }
    // }

    // Sh3BinaryEvaluator eval2;
    // auto maxcirc = GenMuxCircuit();
    // eval2.setCir(&maxcirc, nrows, srvs.mEnc.mShareGen);
    // eval2.setInput(0, SR.mColumns[0]);
    // eval2.setInput(1, SR.mColumns[1]);
    // eval2.setInput(2, SR.mColumns[2]);
    // eval2.asyncEvaluate(srvs.mRt.noDependencies()).get();
    // sbMatrix out2(nrows, 64);
    // eval2.getOutput(0, out2);
    // i64Matrix out22(nrows, 1);
    // srvs.mEnc.revealAll(srvs.mRt.noDependencies(), out2, out22).get();
    // if (partyIdx == 0) {
    //     for (auto i=0; i<nrows; ++i) {
    //         std::cerr << i << ':' << i + 1 << ' ' << out22(i, 0) << std::endl;
    //     }
    // }
}

void CheckPrefixSum(u64 partyIdx) {
    IOService ios;
    DBServer srvs;
    DBSvrSetup(partyIdx, ios, srvs);

    std::vector<ColumnInfo> ColInfo = { ColumnInfo{ "A", TypeID::IntID, 64 } , ColumnInfo{ "B", TypeID::IntID, 64 } };
	u64 nrows = 10;
    Table R(nrows, ColInfo);
    for (auto i=0; i<nrows; ++i) {
        R.mColumns[0].mData(i, 0) = i / 3;
        R.mColumns[1].mData(i, 0) = i+1;
    }
    /*
    0 1 --> 1
    0 2 --> 3
    0 3 --> 6
    1 4 --> 4
    1 5 --> 9
    1 6 --> 15
    2 7 --> 7
    2 8 --> 15
    2 9 --> 24
    3 10 --> 10
    */
    SharedTable SR;
    if (partyIdx == 0) {
        SR = srvs.localInput(R);
    } else {
        SR = srvs.remoteInput(0); // party 0 inputs the table
    }

    // PrefixSum(partyIdx, srvs, SR.mColumns[1]);
    SegmentedPrefixSum(partyIdx, srvs, SR.mColumns[0], SR.mColumns[1]);

    aby3::i64Matrix ACols(nrows, 1), BCols(nrows, 1);
    if (partyIdx == 0) {
        srvs.mEnc.reveal(srvs.mRt.mComm, SR.mColumns[0], ACols);
        srvs.mEnc.reveal(srvs.mRt.mComm, SR.mColumns[1], BCols);
    } else {
        srvs.mEnc.reveal(srvs.mRt.mComm, 0, SR.mColumns[0]);
        srvs.mEnc.reveal(srvs.mRt.mComm, 0, SR.mColumns[1]);
    }
    if (partyIdx == 0) {
        for (auto i=0; i<nrows; ++i) {
            std::cout << ACols(i, 0) << ' ' << BCols(i, 0) << std::endl;
        }
    }

    std::cout << "COMM = " << srvs.mRt.mComm.mNext.getTotalDataSent() + srvs.mRt.mComm.mPrev.getTotalDataSent() << std::endl;
}

void CheckBasicOperation() {
    if (false) {
        std::thread t0 = std::thread(CheckInputAndOutput, 0);
        std::thread t1 = std::thread(CheckInputAndOutput, 1);
        std::thread t2 = std::thread(CheckInputAndOutput, 2);

        t0.join();
        t1.join();
        t2.join();
    }

    if (false) {
        std::thread t0 = std::thread(CheckPSI, 0);
        std::thread t1 = std::thread(CheckPSI, 1);
        std::thread t2 = std::thread(CheckPSI, 2);

        t0.join();
        t1.join();
        t2.join();
    }

    if (true) {
        std::thread t0 = std::thread(CheckSharedTable, 0);
        std::thread t1 = std::thread(CheckSharedTable, 1);
        std::thread t2 = std::thread(CheckSharedTable, 2);

        t0.join();
        t1.join();
        t2.join();
    }

    if (false) {
        std::thread t0 = std::thread(BasicCircuitConstrcution, 0);
        std::thread t1 = std::thread(BasicCircuitConstrcution, 1);
        std::thread t2 = std::thread(BasicCircuitConstrcution, 2);

        t0.join();
        t1.join();
        t2.join();
    }
}

void CheckRandPerm(u64 partyIdx) {
    IOService ios;
    DBServer srvs;
    DBSvrSetup(partyIdx, ios, srvs);

    std::vector<ColumnInfo> ColInfo = { ColumnInfo{ "A", TypeID::IntID, 64 } } ; //, ColumnInfo{ "B", TypeID::IntID, 64 } , ColumnInfo{ "C", TypeID::IntID, 64 } };
	u64 nrows = 4;
    Table R(nrows, ColInfo);
    for (auto i=0; i<nrows; ++i) {
        R.mColumns[0].mData(i, 0) = i+1;
        // R.mColumns[1].mData(i, 0) = (i+1) * 10;
        // R.mColumns[2].mData(i, 0) = (i+1) * 100;
    }
    SharedTable SR;
    if (partyIdx == 0) {
        SR = srvs.localInput(R);
    } else {
        SR = srvs.remoteInput(0);
    }
    Permutation permfunc;

    permfunc.RandPerm(partyIdx, srvs, SR);

    aby3::i64Matrix ACols(nrows, 1), BCols(nrows, 1), CCols(nrows, 1);
    if (partyIdx == 0) {
        srvs.mEnc.reveal(srvs.mRt.mComm, SR.mColumns[0], ACols);
        // srvs.mEnc.reveal(srvs.mRt.mComm, SR.mColumns[1], BCols);
        // srvs.mEnc.reveal(srvs.mRt.mComm, SR.mColumns[2], CCols);
    } else {
        srvs.mEnc.reveal(srvs.mRt.mComm, 0, SR.mColumns[0]);
        // srvs.mEnc.reveal(srvs.mRt.mComm, 0, SR.mColumns[1]);
        // srvs.mEnc.reveal(srvs.mRt.mComm, 0, SR.mColumns[2]);
    }
    std::cout << "COMM = " << srvs.mRt.mComm.mNext.getTotalDataSent() + srvs.mRt.mComm.mPrev.getTotalDataSent() << std::endl;
    if (partyIdx == 0) {
        for (auto i=0; i<nrows; ++i) {
            std::cout << ACols(i, 0) << ' ' << BCols(i, 0) << ' ' << CCols(i, 0) << std::endl;
        }
    }
}

void CheckSSPerm(u64 partyIdx) {
    IOService ios;
    DBServer srvs;
    DBSvrSetup(partyIdx, ios, srvs);

    // std::vector<ColumnInfo> ColInfo = { ColumnInfo{ "A", TypeID::IntID, 64 } , ColumnInfo{ "B", TypeID::IntID, 64 } };
	// u64 nrows = 100;
    // Table R(nrows, ColInfo);
    // for (auto i=0; i<nrows; ++i) {
    //     R.mColumns[0].mData(i, 0) = (i + 6) % nrows + 1;
    //     R.mColumns[1].mData(i, 0) = (i+1) * 10;
    // }
    // SharedTable SR;
    // if (partyIdx == 0) {
    //     SR = srvs.localInput(R);
    // } else {
    //     SR = srvs.remoteInput(0);
    // }
    // Permutation permfunc;
    // permfunc.SSPerm(partyIdx, srvs, SR, 3);

    std::vector<ColumnInfo> RColInfo = { ColumnInfo{ "A", TypeID::IntID, 64 }, ColumnInfo{ "B", TypeID::IntID, 64 }, ColumnInfo{ "PB", TypeID::IntID, 64 }, ColumnInfo{ "PB", TypeID::IntID, 64 } };
	u64 nrows = 8;
    Table R(nrows, RColInfo);
    std::vector<u64> R_A = {1, 1, 1, 2, 2, 2, 3, 3}, R_B = {1, 2, 4, 1, 3, 4, 1, 2}, R_PA = {1, 2, 3, 4, 5, 6, 7, 8}, R_PB = {1, 4, 7, 2, 6, 8, 3, 5};
    for (auto i=0; i<nrows; ++i) {
        R.mColumns[0].mData(i, 0) = R_A[i];
        R.mColumns[1].mData(i, 0) = R_B[i];
        R.mColumns[2].mData(i, 0) = R_PA[i];
        R.mColumns[3].mData(i, 0) = R_PB[i];
    }
    SharedTable SR;
    if (partyIdx == 0) {
        SR = srvs.localInput(R);
    } else {
        SR = srvs.remoteInput(0);
    }
    Permutation permfunc;
    permfunc.SSPerm(partyIdx, srvs, SR, 3);

    aby3::i64Matrix ACols(nrows, 1), BCols(nrows, 1);
    if (partyIdx == 0) {
        srvs.mEnc.reveal(srvs.mRt.mComm, SR.mColumns[0], ACols);
        srvs.mEnc.reveal(srvs.mRt.mComm, SR.mColumns[1], BCols);
    } else {
        srvs.mEnc.reveal(srvs.mRt.mComm, 0, SR.mColumns[0]);
        srvs.mEnc.reveal(srvs.mRt.mComm, 0, SR.mColumns[1]);
    }
    std::cout << "COMM = " << srvs.mRt.mComm.mNext.getTotalDataSent() + srvs.mRt.mComm.mPrev.getTotalDataSent() << std::endl;
    if (partyIdx == 0) {
        for (auto i=0; i<nrows; ++i) {
            std::cout << ACols(i, 0) << ' ' << BCols(i, 0) << std::endl;
        }
    }
}

void CheckGenDummyKey(u64 partyIdx) {
    IOService ios;
    DBServer srvs;
    DBSvrSetup(partyIdx, ios, srvs);

    std::vector<ColumnInfo> ColInfo = { ColumnInfo{ "key", TypeID::IntID, 64 }  };
	u64 nrows = 10;
    Table R(nrows, ColInfo);
    for (auto i=0; i<nrows; ++i) {
        R.mColumns[0].mData(i, 0) = i / 3 + 1;
    }
    SharedTable SR;
    if (partyIdx == 0) {
        SR = srvs.localInput(R);
    } else {
        SR = srvs.remoteInput(0);
    }

    sbMatrix out(nrows, 80);

    GenDummyKey(partyIdx, srvs, SR.mColumns[0], out);

    aby3::i64Matrix ACols(nrows, 2); 
    if (partyIdx == 0) {
        srvs.mEnc.reveal(srvs.mRt.mComm, out, ACols);
    } else {
        srvs.mEnc.reveal(srvs.mRt.mComm, 0, out);
    }
    std::cout << "COMM = " << srvs.mRt.mComm.mNext.getTotalDataSent() + srvs.mRt.mComm.mPrev.getTotalDataSent() << std::endl;
    if (partyIdx == 0) {
        for (auto i=0; i<nrows; ++i) {
            std::cout << ACols(i, 0) << ' ' << ACols(i, 1) << std::endl;
        }
    }
}

void CheckExpansion(u64 partyIdx) {
    IOService ios;
    DBServer srvs;
    DBSvrSetup(partyIdx, ios, srvs);

    std::vector<ColumnInfo> ColInfo = { ColumnInfo{ "A", TypeID::IntID, 64 } , ColumnInfo{ "B", TypeID::IntID, 64 } , ColumnInfo{ "C", TypeID::IntID, 64 } };
    std::vector<u64> degv = {2, 2, 2, 2, 2, 2, 0, 0};
	u64 nrows = degv.size();
    Table R(nrows, ColInfo);
    for (auto i=0; i<nrows; ++i) {
        R.mColumns[0].mData(i, 0) = i+1;
        R.mColumns[1].mData(i, 0) = (i+1) * 10;
        R.mColumns[2].mData(i, 0) = (i+1) * 100;
    }

    SharedTable SR, RE;
    if (partyIdx == 0) {
        SR = srvs.localInput(R);
    } else {
        SR = srvs.remoteInput(0);
    }
    
    sbMatrix sdeg(nrows, 64);
    for (auto i=0; i<nrows; ++i) {
        sdeg.mShares[0](i) = degv[i];
        sdeg.mShares[1](i) = degv[i];
    }

    Expansion(partyIdx, srvs, SR, sdeg, RE, -1);

    STableReveal(srvs, RE);
}

void CheckSort(u64 partyIdx) {
    IOService ios;
    DBServer srvs;
    DBSvrSetup(partyIdx, ios, srvs);

    std::vector<ColumnInfo> ColInfo = { ColumnInfo{ "A", TypeID::IntID, 64 } , ColumnInfo{ "B", TypeID::IntID, 64 } };
	u64 nrows = 1000;
    Table R(nrows, ColInfo);
    for (auto i=0; i<nrows; ++i) {
        R.mColumns[0].mData(i, 0) = 1; //i+1;
        R.mColumns[1].mData(i, 0) = (i+1) * 10;
    }

    SharedTable SR;
    if (partyIdx == 0) {
        SR = srvs.localInput(R);
    } else {
        SR = srvs.remoteInput(0);
    }

    auto start = clock();
    ShuffleQuickSort(partyIdx, srvs, SR, 0);
    auto end = clock();
    std::cout << "TIME = " << 1.0 * (end - start) / CLOCKS_PER_SEC << std::endl;
    std::cout << "COMM = " << srvs.mRt.mComm.mNext.getTotalDataSent() + srvs.mRt.mComm.mPrev.getTotalDataSent() << std::endl;
}

void CheckSelectK(u64 partyIdx) {
    IOService ios;
    DBServer srvs;
    DBSvrSetup(partyIdx, ios, srvs);

    std::vector<ColumnInfo> ColInfo = { ColumnInfo{ "A", TypeID::IntID, 64 } , ColumnInfo{ "B", TypeID::IntID, 64 } };
	u64 nrows = 10000;
    Table R(nrows, ColInfo);
    for (auto i=0; i<nrows; ++i) {
        R.mColumns[0].mData(i, 0) = 1; //i+1;
        R.mColumns[1].mData(i, 0) = (i+1) * 10;
    }

    SharedTable SR;
    if (partyIdx == 0) {
        SR = srvs.localInput(R);
    } else {
        SR = srvs.remoteInput(0);
    }

    auto start = clock();
    ShuffleSelectK(partyIdx, srvs, SR, 0, 1000);
    auto end = clock();
    STableReveal(srvs, SR);

    std::cout << "TIME = " << 1.0 * (end - start) / CLOCKS_PER_SEC << std::endl;
    std::cout << "COMM = " << srvs.mRt.mComm.mNext.getTotalDataSent() + srvs.mRt.mComm.mPrev.getTotalDataSent() << std::endl;
}

void CheckCompaction(u64 partyIdx) {
    IOService ios;
    DBServer srvs;
    DBSvrSetup(partyIdx, ios, srvs);

    std::vector<ColumnInfo> ColInfo = { ColumnInfo{ "A", TypeID::IntID, 64 } , ColumnInfo{ "B", TypeID::IntID, 64 } , ColumnInfo{ "C", TypeID::IntID, 64 } };
    std::vector<u64> degv = {2, 2, 2, 2, 2, 2, 0, 0};
	u64 nrows = degv.size();
    Table R(nrows, ColInfo);
    for (auto i=0; i<nrows; ++i) {
        R.mColumns[0].mData(i, 0) = (i+1) % 2;
        R.mColumns[1].mData(i, 0) = (i+1) * 10;
        R.mColumns[2].mData(i, 0) = (i+1) * 100;
    }

    SharedTable SR, RE;
    if (partyIdx == 0) {
        SR = srvs.localInput(R);
    } else {
        SR = srvs.remoteInput(0);
    }

    CompactionZeroEntity(partyIdx, srvs, SR, 0);
}

void CheckFunction() {
    if (false) {
        std::thread t0 = std::thread(CheckRandPerm, 0);
        std::thread t1 = std::thread(CheckRandPerm, 1);
        std::thread t2 = std::thread(CheckRandPerm, 2);

        t0.join();
        t1.join();
        t2.join();
    }

    if (false) {
        std::thread t0 = std::thread(CheckSSPerm, 0);
        std::thread t1 = std::thread(CheckSSPerm, 1);
        std::thread t2 = std::thread(CheckSSPerm, 2);

        t0.join();
        t1.join();
        t2.join();
    }

    if (false) {
        std::thread t0 = std::thread(CheckPrefixSum, 0);
        std::thread t1 = std::thread(CheckPrefixSum, 1);
        std::thread t2 = std::thread(CheckPrefixSum, 2);

        t0.join();
        t1.join();
        t2.join();
    }

    if (false) {
        std::thread t0 = std::thread(CheckGenDummyKey, 0);
        std::thread t1 = std::thread(CheckGenDummyKey, 1);
        std::thread t2 = std::thread(CheckGenDummyKey, 2);

        t0.join();
        t1.join();
        t2.join();
    }

    if (true) {
        std::thread t0 = std::thread(CheckExpansion, 0);
        std::thread t1 = std::thread(CheckExpansion, 1);
        std::thread t2 = std::thread(CheckExpansion, 2);

        t0.join();
        t1.join();
        t2.join();
    }
}

void CheckBinaryJoin(u64 partyIdx) {
    IOService ios;
    DBServer srvs;
    DBSvrSetup(partyIdx, ios, srvs);

    std::vector<ColumnInfo> RColInfo = { ColumnInfo{ "A", TypeID::IntID, 64 }, ColumnInfo{ "B", TypeID::IntID, 64 }, ColumnInfo{ "PB", TypeID::IntID, 64 }, ColumnInfo{ "PB", TypeID::IntID, 64 } };
    // std::vector<u64> R_A = {1, 1, 1, 2, 2, 2, 3, 3}, R_B = {1, 2, 4, 1, 3, 4, 1, 2}, R_PA = {1, 2, 3, 4, 5, 6, 7, 8}, R_PB = {1, 4, 7, 2, 6, 8, 3, 5};
    std::vector<u64> R_A = {1, 1, 1, 2, 2, 2, 3, 3, 3, 3}, R_B = {2, 3, 4, 1, 2, 5, 1, 2, 6, 7}, R_PA = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, R_PB = {3, 6, 7, 1, 4, 8, 2, 5, 9, 10};
    u64 rnrows = R_A.size();
    Table R(rnrows, RColInfo);
    
    for (auto i=0; i<rnrows; ++i) {
        R.mColumns[0].mData(i, 0) = R_A[i];
        R.mColumns[1].mData(i, 0) = R_B[i];
        R.mColumns[2].mData(i, 0) = R_PA[i];
        R.mColumns[3].mData(i, 0) = R_PB[i];
    }
    SharedTable SR;
    if (partyIdx == 0) {
        SR = srvs.localInput(R);
    } else {
        SR = srvs.remoteInput(0);
    }

    std::vector<ColumnInfo> SColInfo = { ColumnInfo{ "B", TypeID::IntID, 64 }, ColumnInfo{ "C", TypeID::IntID, 64 }, ColumnInfo{ "PB", TypeID::IntID, 64 }, ColumnInfo{ "PC", TypeID::IntID, 64 } };
    // std::vector<u64> S_B = {1, 1, 2, 2, 3, 3, 5, 6}, S_C = {1, 2, 1, 2, 1, 2, 2, 1}, S_PB = {1, 2, 3, 4, 5, 6, 7, 8}, S_PC = {1, 5, 2, 6, 3, 7, 8, 4};
    std::vector<u64> S_B = {1, 1, 2, 2, 2, 3}, S_C = {11, 111, 22, 222, 2222, 33}, S_PB = {1, 2, 3, 4, 5, 6}, S_PC = {1, 2, 3, 4, 5, 6};
    u64 snrows = S_B.size();
    Table S(snrows, SColInfo);
    for (auto i=0; i<snrows; ++i) {
        S.mColumns[0].mData(i, 0) = S_B[i];
        S.mColumns[1].mData(i, 0) = S_C[i];
        S.mColumns[2].mData(i, 0) = S_PB[i];
        S.mColumns[3].mData(i, 0) = S_PC[i];
    }
    SharedTable SS;
    if (partyIdx == 0) {
        SS = srvs.localInput(S);
    } else {
        SS = srvs.remoteInput(0);
    }
    
    SharedTable ST;

    BinaryJoin(partyIdx, srvs, SR, 1, 0, 3, {2}, SS, 0, 0, 2, {3}, ST);
    std::vector<u64> col_lists = {0, 1, 5, 2, 3, 7};
    TakeColumns(ST, col_lists);

    u64 nattrs = ST.mColumns.size();
    u64 nrows = ST.rows();
    std::vector<aby3::i64Matrix> RCols(nattrs);
    if (partyIdx == 0) {
        for (auto i=0; i<nattrs; ++i) {
            RCols[i].resize(nrows, 1);
            srvs.mEnc.reveal(srvs.mRt.mComm, ST.mColumns[i], RCols[i]);
        }
    } else {
        for (auto i=0; i<nattrs; ++i) {
            RCols[i].resize(nrows, 1);
            srvs.mEnc.reveal(srvs.mRt.mComm, 0, ST.mColumns[i]);
        }
    }
    std::cout << partyIdx << ",COMM = " << srvs.mRt.mComm.mNext.getTotalDataSent() + srvs.mRt.mComm.mPrev.getTotalDataSent() << std::endl;
    if (partyIdx == 0) {
        for (auto i=0; i<nrows; ++i) {
            for (auto j=0; j<nattrs; ++j) {
                std::cerr << RCols[j](i) << ' ';
            }
            std::cerr << std::endl;
        }
    }
}

void CheckPKJoin(u64 partyIdx) {
    IOService ios;
    DBServer srvs;
    DBSvrSetup(partyIdx, ios, srvs);

    std::vector<ColumnInfo> RColInfo = { ColumnInfo{ "A", TypeID::IntID, 64 }, ColumnInfo{ "B", TypeID::IntID, 64 }, ColumnInfo{ "PB", TypeID::IntID, 64 }, ColumnInfo{ "PB", TypeID::IntID, 64 } };
    // std::vector<u64> R_A = {1, 1, 1, 2, 2, 2, 3, 3}, R_B = {1, 2, 4, 1, 3, 4, 1, 2}, R_PA = {1, 2, 3, 4, 5, 6, 7, 8}, R_PB = {1, 4, 7, 2, 6, 8, 3, 5};
    std::vector<u64> R_A = {1, 1, 1, 2, 2, 2, 3, 3, 3, 3}, R_B = {2, 3, 4, 1, 2, 5, 1, 2, 6, 7}, R_PA = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, R_PB = {3, 6, 7, 1, 4, 8, 2, 5, 9, 10};
    u64 rnrows = R_A.size();
    Table R(rnrows, RColInfo);
    
    for (auto i=0; i<rnrows; ++i) {
        R.mColumns[0].mData(i, 0) = R_A[i];
        R.mColumns[1].mData(i, 0) = R_B[i];
        R.mColumns[2].mData(i, 0) = R_PA[i];
        R.mColumns[3].mData(i, 0) = R_PB[i];
    }
    SharedTable SR;
    if (partyIdx == 0) {
        SR = srvs.localInput(R);
    } else {
        SR = srvs.remoteInput(0);
    }

    std::vector<ColumnInfo> SColInfo = { ColumnInfo{ "B", TypeID::IntID, 64 }, ColumnInfo{ "C", TypeID::IntID, 64 }, ColumnInfo{ "PB", TypeID::IntID, 64 }, ColumnInfo{ "PC", TypeID::IntID, 64 } };
    // std::vector<u64> S_B = {1, 1, 2, 2, 3, 3, 5, 6}, S_C = {1, 2, 1, 2, 1, 2, 2, 1}, S_PB = {1, 2, 3, 4, 5, 6, 7, 8}, S_PC = {1, 5, 2, 6, 3, 7, 8, 4};
    std::vector<u64> S_B = {1, 2, 3, 4, 5, 6, 7}, S_C = {11, 22, 33, 44, 55, 66, 77}, S_PB = {1, 2, 3, 4, 5, 6, 7}, S_PC = {1, 2, 3, 4, 5, 6, 7};
    u64 snrows = S_B.size();
    Table S(snrows, SColInfo);
    for (auto i=0; i<snrows; ++i) {
        S.mColumns[0].mData(i, 0) = S_B[i];
        S.mColumns[1].mData(i, 0) = S_C[i];
        S.mColumns[2].mData(i, 0) = S_PB[i];
        S.mColumns[3].mData(i, 0) = S_PC[i];
    }
    SharedTable SS;
    if (partyIdx == 0) {
        SS = srvs.localInput(S);
    } else {
        SS = srvs.remoteInput(0);
    }
    
    SharedTable ST;

    PKJoin(partyIdx, srvs, SR, 1, 0, 3, SS, 0, 0, 2, {3}, ST);
    std::vector<u64> col_lists = {0, 1, 5, 2, 3, 7};
    TakeColumns(ST, col_lists);

    u64 nattrs = ST.mColumns.size();
    u64 nrows = ST.rows();
    std::vector<aby3::i64Matrix> RCols(nattrs);
    if (partyIdx == 0) {
        for (auto i=0; i<nattrs; ++i) {
            RCols[i].resize(nrows, 1);
            srvs.mEnc.reveal(srvs.mRt.mComm, ST.mColumns[i], RCols[i]);
        }
    } else {
        for (auto i=0; i<nattrs; ++i) {
            RCols[i].resize(nrows, 1);
            srvs.mEnc.reveal(srvs.mRt.mComm, 0, ST.mColumns[i]);
        }
    }
    std::cout << partyIdx << ",COMM = " << srvs.mRt.mComm.mNext.getTotalDataSent() + srvs.mRt.mComm.mPrev.getTotalDataSent() << std::endl;
    if (partyIdx == 0) {
        for (auto i=0; i<nrows; ++i) {
            for (auto j=0; j<nattrs; ++j) {
                std::cerr << RCols[j](i) << ' ';
            }
            std::cerr << std::endl;
        }
    }
}

void CheckSemiJoin(u64 partyIdx) {
    IOService ios;
    DBServer srvs;
    DBSvrSetup(partyIdx, ios, srvs);

    std::vector<ColumnInfo> RColInfo = { ColumnInfo{ "A", TypeID::IntID, 64 }, ColumnInfo{ "B", TypeID::IntID, 64 }, ColumnInfo{ "PA", TypeID::IntID, 64 }, ColumnInfo{ "PB", TypeID::IntID, 64 } };
    // std::vector<u64> R_A = {1, 1, 1, 2, 2, 2, 3, 3}, R_B = {1, 2, 4, 1, 3, 4, 1, 2}, R_PA = {1, 2, 3, 4, 5, 6, 7, 8}, R_PB = {1, 4, 7, 2, 6, 8, 3, 5};
    std::vector<u64> R_A = {1, 1, 1, 2, 2, 2, 3, 3, 3, 3}, R_B = {2, 3, 4, 1, 2, 5, 1, 2, 6, 7}, R_PA = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10}, R_PB = {3, 6, 7, 1, 4, 8, 2, 5, 9, 10};
    u64 rnrows = R_A.size();
    Table R(rnrows, RColInfo);
    
    for (auto i=0; i<rnrows; ++i) {
        R.mColumns[0].mData(i, 0) = R_A[i];
        R.mColumns[1].mData(i, 0) = R_B[i];
        R.mColumns[2].mData(i, 0) = R_PA[i];
        R.mColumns[3].mData(i, 0) = R_PB[i];
    }
    SharedTable SR;
    if (partyIdx == 0) {
        SR = srvs.localInput(R);
    } else {
        SR = srvs.remoteInput(0);
    }

    std::vector<ColumnInfo> SColInfo = { ColumnInfo{ "B", TypeID::IntID, 64 }, ColumnInfo{ "C", TypeID::IntID, 64 }, ColumnInfo{ "PB", TypeID::IntID, 64 }, ColumnInfo{ "PC", TypeID::IntID, 64 } };
    // std::vector<u64> S_B = {1, 1, 2, 2, 3, 3, 5, 6}, S_C = {1, 2, 1, 2, 1, 2, 2, 1}, S_PB = {1, 2, 3, 4, 5, 6, 7, 8}, S_PC = {1, 5, 2, 6, 3, 7, 8, 4};
    // std::vector<u64> S_B = {1, 2, 3, 4, 5, 6, 7}, S_C = {11, 22, 33, 44, 55, 66, 77}, S_PB = {1, 2, 3, 4, 5, 6, 7}, S_PC = {1, 2, 3, 4, 5, 6, 7};
    std::vector<u64> S_B = {1, 1, 2, 2, 3, 3, 4}, S_C = {1, 10, 2, 20, 33, 300, 444}, S_PB = {1, 2, 3, 4, 5, 6, 7}, S_PC = {1, 2, 3, 4, 5, 6, 7};
    u64 snrows = S_B.size();
    Table S(snrows, SColInfo);
    for (auto i=0; i<snrows; ++i) {
        S.mColumns[0].mData(i, 0) = S_B[i];
        S.mColumns[1].mData(i, 0) = S_C[i];
        S.mColumns[2].mData(i, 0) = S_PB[i];
        S.mColumns[3].mData(i, 0) = S_PC[i];
    }
    SharedTable SS;
    if (partyIdx == 0) {
        SS = srvs.localInput(S);
    } else {
        SS = srvs.remoteInput(0);
    }
    
    SharedTable ST;

    SemiJoin(partyIdx, srvs, SR, 1, 0, 3, SS, 0, 0, 2, 1, AggFunc::SUM, ST);

    u64 nattrs = ST.mColumns.size();
    u64 nrows = ST.rows();
    std::vector<aby3::i64Matrix> RCols(nattrs);
    if (partyIdx == 0) {
        for (auto i=0; i<nattrs; ++i) {
            RCols[i].resize(nrows, 1);
            srvs.mEnc.reveal(srvs.mRt.mComm, ST.mColumns[i], RCols[i]);
        }
    } else {
        for (auto i=0; i<nattrs; ++i) {
            RCols[i].resize(nrows, 1);
            srvs.mEnc.reveal(srvs.mRt.mComm, 0, ST.mColumns[i]);
        }
    }
    std::cout << partyIdx << ",COMM = " << srvs.mRt.mComm.mNext.getTotalDataSent() + srvs.mRt.mComm.mPrev.getTotalDataSent() << std::endl;
    if (partyIdx == 0) {
        for (auto i=0; i<nrows; ++i) {
            for (auto j=0; j<nattrs; ++j) {
                std::cerr << RCols[j](i) << ' ';
            }
            std::cerr << std::endl;
        }
    }
}

void CheckJoin() {
    if (true) {
        std::thread t0 = std::thread(CheckBinaryJoin, 0);
        std::thread t1 = std::thread(CheckBinaryJoin, 1);
        std::thread t2 = std::thread(CheckBinaryJoin, 2);

        t0.join();
        t1.join();
        t2.join();
    }
    if (true) {
        std::thread t0 = std::thread(CheckPKJoin, 0);
        std::thread t1 = std::thread(CheckPKJoin, 1);
        std::thread t2 = std::thread(CheckPKJoin, 2);

        t0.join();
        t1.join();
        t2.join();
    }
}

void CheckLessThan(u64 partyIdx) {
    IOService ios;
    DBServer srvs;
    DBSvrSetup(partyIdx, ios, srvs);

    std::vector<ColumnInfo> RColInfo = { ColumnInfo{ "A", TypeID::IntID, 64 }, ColumnInfo{ "B", TypeID::IntID, 64 }, ColumnInfo{ "PB", TypeID::IntID, 64 }, ColumnInfo{ "PB", TypeID::IntID, 64 } };
	u64 nrows = 8;
    Table R(nrows, RColInfo);
    std::vector<u64> R_A = {1, 1, 1, 2, 2, 2, 3, 3}, R_B = {1, 2, 4, 1, 3, 4, 1, 2}, R_PA = {1, 2, 3, 4, 5, 6, 7, 8}, R_PB = {1, 4, 7, 2, 6, 8, 3, 5};
    for (auto i=0; i<nrows; ++i) {
        R.mColumns[0].mData(i, 0) = R_A[i];
        R.mColumns[1].mData(i, 0) = R_B[i];
        R.mColumns[2].mData(i, 0) = R_PA[i];
        R.mColumns[3].mData(i, 0) = R_PB[i];
    }
    SharedTable SR;
    if (partyIdx == 0) {
        SR = srvs.localInput(R);
    } else {
        SR = srvs.remoteInput(0);
    }

    u64 neles = 5;
    BetaLibrary lib;
    Sh3BinaryEvaluator eval;
    sbMatrix compa(neles, 128), compb(neles, 128), compr(neles, 128);
    for (auto i=0; i<neles; ++i) {
        compa.mShares[0](i, 1) = compa.mShares[1](i, 1) = i;
        compa.mShares[0](i, 0) = compa.mShares[1](i, 0) = i;

        compb.mShares[0](i, 1) = compb.mShares[1](i, 1) = 3;
        compb.mShares[0](i, 0) = compb.mShares[1](i, 0) = 3;
    }
    std::cerr << "\nA\n";
    SBMatReveal(srvs, compa);
    std::cerr << "\nB\n";
    SBMatReveal(srvs, compb);

    auto circ = LessThan(128);  // LessThan(32);
    eval.setCir(&circ, neles, srvs.mEnc.mShareGen);
    eval.setInput(0, compa);
    eval.setInput(1, compb);
    eval.asyncEvaluate(srvs.mRt.noDependencies()).get();
    eval.getOutput(0, compr);

    SBMatReveal(srvs, compr);
    return;
}

void CheckPKFKJoin(u64 partyIdx) {
    IOService ios;
    DBServer srvs;
    DBSvrSetup(partyIdx, ios, srvs);

    std::vector<ColumnInfo> RColInfo = { ColumnInfo{ "A", TypeID::IntID, 64 } };
    std::vector<u64> R_A = {1, 1, 1, 2, 2, 2, 3, 3, 3, 3};
    u64 rnrows = R_A.size();
    Table R(rnrows, RColInfo);
    
    for (auto i=0; i<rnrows; ++i) {
        R.mColumns[0].mData(i, 0) = R_A[i];
    }
    SharedTable SR;
    if (partyIdx == 0) {
        SR = srvs.localInput(R);
    } else {
        SR = srvs.remoteInput(0);
    }

    std::vector<ColumnInfo> SColInfo = { ColumnInfo{ "B", TypeID::IntID, 64 }, ColumnInfo{ "C", TypeID::IntID, 64 } };
    std::vector<u64> S_B = {1, 2, 3, 4, 5, 6, 7}, S_C = {11, 22, 33, 44, 55, 66, 77};
    u64 snrows = S_B.size();
    Table S(snrows, SColInfo);
    for (auto i=0; i<snrows; ++i) {
        S.mColumns[0].mData(i, 0) = S_B[i];
        S.mColumns[1].mData(i, 0) = S_C[i];
    }
    SharedTable SS;
    if (partyIdx == 0) {
        SS = srvs.localInput(S);
    } else {
        SS = srvs.remoteInput(0);
    }
    
    SharedTable ST;
    sbMatrix SRT;
    // PKFKwithPayload(partyIdx, srvs, SR.mColumns[0], SS.mColumns[0], SS.mColumns[1], SRT);
    PKFKJoin(partyIdx, srvs, SR, 0, 0, SS, 0, 0, ST);
}

void CheckLevelK(u64 partyIdx) {
    IOService ios;
    DBServer srvs;
    DBSvrSetup(partyIdx, ios, srvs);

    std::vector<ColumnInfo> RColInfo = { ColumnInfo{ "A", TypeID::IntID, 64 }, ColumnInfo("C", TypeID::IntID, 64)};
    // std::vector<u64> R_A = {1, 1, 1, 2, 2, 2, 3, 3, 3, 3}, R_B = {1, 11, 111, 2, 22, 222, 3, 33, 333, 3333};
    u64 k = 128;
    u64 rnrows = k;
    Table R(rnrows, RColInfo);
    
    for (auto i=0; i<rnrows; ++i) {
        R.mColumns[0].mData(i, 0) = i / 10 + 1;
        R.mColumns[1].mData(i, 0) = ( i / 10 ) * 100 + i;
    }
    SharedTable SR;
    if (partyIdx == 0) {
        SR = srvs.localInput(R);
    } else {
        SR = srvs.remoteInput(0);
    }

    std::vector<ColumnInfo> SColInfo = { ColumnInfo{ "B", TypeID::IntID, 64 }, ColumnInfo{ "C", TypeID::IntID, 64 } };
    std::vector<u64> S_B = {1, 2, 3, 1, 2, 1, 1, 1}, S_C = {10001, 201, 301, 10002, 202, 10003, 10004, 1005};
    u64 snrows = k;
    Table S(snrows, SColInfo);
    for (auto i=0; i<snrows; ++i) {
        S.mColumns[0].mData(i, 0) = i / 100 + 1; //S_B[i];
        S.mColumns[1].mData(i, 0) =  ( i / 100 ) * 100 + i; //S_C[i];
    }
    SharedTable SS;
    if (partyIdx == 0) {
        SS = srvs.localInput(S);
    } else {
        SS = srvs.remoteInput(0);
    }
    
    SharedTable ST;
    auto start = clock();
    LevelK(partyIdx, srvs, SR, 0, 1, SS, 0, 1, k, ST);
    auto end = clock();
    auto atime = 1.0 * (end - start) / CLOCKS_PER_SEC;
    auto acomm = srvs.mRt.mComm.mNext.getTotalDataSent() + srvs.mRt.mComm.mPrev.getTotalDataSent() + srvs.mRt.mComm.mNext.getTotalDataRecv() + srvs.mRt.mComm.mPrev.getTotalDataRecv();
    STableReveal(srvs, ST);
    std::cerr << std::endl;

    auto old = srvs.mRt.mComm.mNext.getTotalDataSent() + srvs.mRt.mComm.mPrev.getTotalDataSent() + srvs.mRt.mComm.mNext.getTotalDataRecv() + srvs.mRt.mComm.mPrev.getTotalDataRecv();
    start = clock();
    ProductK(partyIdx, srvs, SR, 0, 1, SS, 0, 1, k, ST);
    end = clock();
    auto btime =  1.0 * (end - start) / CLOCKS_PER_SEC;
    auto bcomm = srvs.mRt.mComm.mNext.getTotalDataSent() + srvs.mRt.mComm.mPrev.getTotalDataSent() + srvs.mRt.mComm.mNext.getTotalDataRecv() + srvs.mRt.mComm.mPrev.getTotalDataRecv() - old;
    STableReveal(srvs, ST);

    std::cerr << "k = " << k << std::endl;
    std::cerr << "level k, time = " << atime << std::endl << "level k, comm = " << acomm << std::endl;
    std::cerr << "prodct k, time = " << btime << std::endl << "product k, comm = " << bcomm << std::endl;

}

int main(int argc, char** argv) {
    if (argc == 0) return 0;

    // CheckProjectSetup();

    // if (argc == 0) return 0;
    // CheckInputAndOutput((argv[1][0] - '0'));

    // CheckPSI((argv[1][0] - '0'));

    // CheckBasicOperation();

    // CheckCompaction((argv[1][0] - '0'));
    // CheckFunction();

    // CheckJoin();
    // CheckBinaryJoin((argv[1][0] - '0'));
    // CheckPKFKJoin((argv[1][0] - '0'));
    // CheckSemiJoin((argv[1][0] - '0'));

    // CheckRandPerm((argv[1][0] - '0'));
    // CheckSSPerm((argv[1][0] - '0'));
    // CheckGenDummyKey((argv[1][0] - '0'));
    // CheckExpansion((argv[1][0] - '0'));

    // BasicCircuitConstrcution((argv[1][0] - '0'));
    // CheckLessThan((argv[1][0] - '0'));
    // CheckSort((argv[1][0] - '0'));
    // CheckSelectK((argv[1][0] - '0'));
    CheckLevelK((argv[1][0] - '0'));

    return 0;
}