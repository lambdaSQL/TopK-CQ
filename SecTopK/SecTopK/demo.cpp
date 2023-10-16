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
#include "CalcSHA.h"
#include "LevelK.h"

using namespace oc;
using namespace aby3;

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

void CheckPhase(u64 partyIdx) {
    IOService ios;
    DBServer srvs;
    DBSvrSetup(partyIdx, ios, srvs);

    std::vector<ColumnInfo> RColInfo = { ColumnInfo{ "A", TypeID::IntID, 64 }, ColumnInfo{ "B", TypeID::IntID, 64 }, ColumnInfo{ "Annot", TypeID::IntID, 64 }, ColumnInfo{ "SHA", TypeID::IntID, 64 } };
    std::vector<u64> R_A = {1, 2, 3}; //{1, 1, 1, 2, 2, 2, 3, 3, 3, 3};
    std::vector<u64> R_B = {1, 2, 3}; //{2, 3, 4, 1, 2, 5, 1, 2, 6, 7};
    std::vector<u64> R_Annot = {3, 2, 1}; //{1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
    std::vector<u64> R_SHA = {3, 2, 1}; //{3, 6, 7, 1, 4, 8, 2, 5, 9, 10};
    u64 rnrows = R_A.size();
    Table R(rnrows, RColInfo);
    for (auto i=0; i<rnrows; ++i) {
        R.mColumns[0].mData(i, 0) = R_A[i];
        R.mColumns[1].mData(i, 0) = R_B[i];
        R.mColumns[2].mData(i, 0) = R_Annot[i];
        R.mColumns[3].mData(i, 0) = R_SHA[i];
    }
    SharedTable SR;
    if (partyIdx == 0) {
        SR = srvs.localInput(R);
    } else {
        SR = srvs.remoteInput(0);
    }

    std::vector<ColumnInfo> SColInfo = { ColumnInfo{ "B", TypeID::IntID, 64 }, ColumnInfo{ "C", TypeID::IntID, 64 }, ColumnInfo{ "Annot", TypeID::IntID, 64 }, ColumnInfo{ "SHA", TypeID::IntID, 64 } };
    std::vector<u64> S_B = {1, 1, 1, 2, 2, 3}; //{1, 1, 2, 2, 2, 3};
    std::vector<u64> S_C = {1, 2, 3, 1, 2, 3}; //{11, 111, 22, 222, 2222, 33};
    std::vector<u64> S_Annot = {4, 3, 2, 5, 7, 1}; //{1, 2, 3, 4, 5, 6};
    std::vector<u64> S_SHA = {4, 3, 2, 5, 7, 1}; //{1, 2, 3, 4, 5, 6};
    u64 snrows = S_B.size();
    Table S(snrows, SColInfo);
    for (auto i=0; i<snrows; ++i) {
        S.mColumns[0].mData(i, 0) = S_B[i];
        S.mColumns[1].mData(i, 0) = S_C[i];
        S.mColumns[2].mData(i, 0) = S_Annot[i];
        S.mColumns[3].mData(i, 0) = S_SHA[i];
    }
    SharedTable SS;
    if (partyIdx == 0) {
        SS = srvs.localInput(S);
    } else {
        SS = srvs.remoteInput(0);
    }

    // ProductK(partyIdx, srvs, SR, 1, 2, SS, 0, 2, 3, ST);
    // STableReveal(srvs, ST);
    
    CalcSHA(partyIdx, srvs, SR, 1, 3, SS, 0, 3);
    STableReveal(srvs, SR);

    std::cout << partyIdx << ",COMM = " << srvs.mRt.mComm.mNext.getTotalDataSent() + srvs.mRt.mComm.mPrev.getTotalDataSent() << std::endl;

}

void L3(u64 partyIdx) {
    IOService ios;
    DBServer srvs;
    DBSvrSetup(partyIdx, ios, srvs);

    std::vector<ColumnInfo> RColInfo = { ColumnInfo{ "A", TypeID::IntID, 64 }, ColumnInfo{ "B", TypeID::IntID, 64 }, ColumnInfo{ "Annot", TypeID::IntID, 64 }, ColumnInfo{ "SHA", TypeID::IntID, 64 } };
    std::vector<u64> R_A = {1, 2, 3}; 
    std::vector<u64> R_B = {1, 2, 3};
    std::vector<u64> R_Annot = {3, 2, 1}; 
    std::vector<u64> R_SHA = {3, 2, 1}; 
    u64 rnrows = R_A.size();
    Table R(rnrows, RColInfo);
    for (auto i=0; i<rnrows; ++i) {
        R.mColumns[0].mData(i, 0) = R_A[i];
        R.mColumns[1].mData(i, 0) = R_B[i];
        R.mColumns[2].mData(i, 0) = R_Annot[i];
        R.mColumns[3].mData(i, 0) = R_SHA[i];
    }
    SharedTable SR;
    if (partyIdx == 0) {
        SR = srvs.localInput(R);
    } else {
        SR = srvs.remoteInput(0);
    }

    std::vector<ColumnInfo> SColInfo = { ColumnInfo{ "B", TypeID::IntID, 64 }, ColumnInfo{ "C", TypeID::IntID, 64 }, ColumnInfo{ "Annot", TypeID::IntID, 64 }, ColumnInfo{ "SHA", TypeID::IntID, 64 } };
    std::vector<u64> S_B = {1, 1, 1, 2, 2, 3}; 
    std::vector<u64> S_C = {1, 2, 3, 1, 2, 3}; 
    std::vector<u64> S_Annot = {4, 3, 2, 5, 7, 1}; 
    std::vector<u64> S_SHA = {4, 3, 2, 5, 7, 1}; 
    u64 snrows = S_B.size();
    Table S(snrows, SColInfo);
    for (auto i=0; i<snrows; ++i) {
        S.mColumns[0].mData(i, 0) = S_B[i];
        S.mColumns[1].mData(i, 0) = S_C[i];
        S.mColumns[2].mData(i, 0) = S_Annot[i];
        S.mColumns[3].mData(i, 0) = S_SHA[i];
    }
    SharedTable SS;
    if (partyIdx == 0) {
        SS = srvs.localInput(S);
    } else {
        SS = srvs.remoteInput(0);
    }

    std::vector<ColumnInfo> TColInfo = { ColumnInfo{ "C", TypeID::IntID, 64 }, ColumnInfo{ "Annot", TypeID::IntID, 64 }, ColumnInfo{ "SHA", TypeID::IntID, 64 } };
    std::vector<u64> T_C = {1, 2, 3}; 
    std::vector<u64> T_Annot = {3, 1, 1000};
    std::vector<u64> T_SHA = {3, 1, 1000};
    u64 tnrows = T_C.size();
    Table T(tnrows, TColInfo);
    for (auto i=0; i<tnrows; ++i) {
        T.mColumns[0].mData(i, 0) = T_C[i];
        T.mColumns[1].mData(i, 0) = T_Annot[i];
        T.mColumns[2].mData(i, 0) = T_SHA[i];
    }
    SharedTable ST;
    if (partyIdx == 0) {
        ST = srvs.localInput(T);
    } else {
        ST = srvs.remoteInput(0);
    }

    u64 k = 3;
    SharedTable SRS, SRST;

    auto start_time = clock();
    // SS: A, B, Annot, SHA
    // SR: B, C, Annot, SHA
    // ST: C, Annot, SHA
    CalcSHA(partyIdx, srvs, SS, 1, 3, ST, 0, 2);
    CalcSHA(partyIdx, srvs, SR, 1, 3, SS, 0, 3);

    ShuffleSelectK(partyIdx, srvs, SR, 3, k);
    TakeColumns(SR, {0, 1, 2});

    FilterNextK(partyIdx, srvs, SR, 1, 2, SS, 0, 3, k);
    LevelK(partyIdx, srvs, SR, 1, 2, SS, 0, 3, k, SRS);

    // SRS: A, B, Annot1, B, C, Annot2, SHA2, Annot1*SHA2
    ColumnMult(partyIdx, srvs, SRS, 2, 5, 2);
    TakeColumns(SRS, {0, 1, 4, 2});

    std::cout << "\nR Join S\n";
    STableReveal(srvs, SRS);

    // SRS: A, B, C, Annot
    FilterNextK(partyIdx, srvs, SRS, 2, 3, ST, 0, 2, k);
    LevelK(partyIdx, srvs, SRS, 2, 3, ST, 0, 2, k, SRST);

    // SRST: A, B, C, Annot1, C, Annot2, SHA2, Annot1*SHA2
    ColumnMult(partyIdx, srvs, SRST, 3, 5, 3);
    TakeColumns(SRST, {0, 1, 2, 3});

    std::cout << "\nRS Join T\n";
    STableReveal(srvs, SRST);
    auto end_time = clock();

    std::cout << "TIME = " << 1.0 * (end_time - start_time) / CLOCKS_PER_SEC << " s" << std::endl;
    std::cout << "COMM = " << srvs.mRt.mComm.mNext.getTotalDataSent() + srvs.mRt.mComm.mPrev.getTotalDataSent() + srvs.mRt.mComm.mNext.getTotalDataRecv() + srvs.mRt.mComm.mPrev.getTotalDataRecv() << " Bytes" << std::endl;

}

void L4Star(u64 partyIdx) {
    IOService ios;
    DBServer srvs;
    DBSvrSetup(partyIdx, ios, srvs);

    std::vector<ColumnInfo> RColInfo = { ColumnInfo{ "A", TypeID::IntID, 64 }, ColumnInfo{ "B", TypeID::IntID, 64 }, ColumnInfo{ "Annot", TypeID::IntID, 64 }, ColumnInfo{ "SHA", TypeID::IntID, 64 } };
    std::vector<u64> R_A = {1, 1, 2, 2, 2, 3}; 
    std::vector<u64> R_B = {1, 2, 3, 4, 5, 6};
    std::vector<u64> R_Annot = {1, 10, 2, 4, 8, 9}; 
    u64 rnrows = R_A.size();
    Table R(rnrows, RColInfo);
    for (auto i=0; i<rnrows; ++i) {
        R.mColumns[0].mData(i, 0) = R_A[i];
        R.mColumns[1].mData(i, 0) = R_B[i];
        R.mColumns[2].mData(i, 0) = R_Annot[i];
        R.mColumns[3].mData(i, 0) = R_Annot[i];
    }
    SharedTable SR1, SR2, SR3, SR4;
    if (partyIdx == 0) {
        SR1 = srvs.localInput(R);
        SR2 = srvs.localInput(R);
        SR3 = srvs.localInput(R);
        SR4 = srvs.localInput(R);
    } else {
        SR1 = srvs.remoteInput(0);
        SR2 = srvs.remoteInput(0);
        SR3 = srvs.remoteInput(0);
        SR4 = srvs.remoteInput(0);
    }


    u64 k = 4;
    SharedTable SR14, SR134, ST;

    auto start_time = clock();

    // Step 1: R1 Join R4
    CalcSHA(partyIdx, srvs, SR1, 0, 3, SR2, 0, 3);
    CalcSHA(partyIdx, srvs, SR1, 0, 3, SR3, 0, 3);

    ShuffleSelectK(partyIdx, srvs, SR1, 3, k);
    TakeColumns(SR1, {0, 1, 2});

    FilterNextK(partyIdx, srvs, SR1, 0, 2, SR4, 0, 3, k);
    ProductK(partyIdx, srvs, SR1, 0, 2, SR4, 0, 3, k, SR14);

    // A, B, Annot1, A, C, Annot2, SHA2, Annot1*SHA2
    ColumnMult(partyIdx, srvs, SR14, 2, 5, 2);
    TakeColumns(SR14, {0, 1, 4, 2, 2});
    // SR14: A, B, C, annot, SHA (=annot)

    // STableReveal(srvs, SR14);
    
    // Step 2: R14 Join R3
    CalcSHA(partyIdx, srvs, SR14, 0, 4, SR2, 0, 3);
    ShuffleSelectK(partyIdx, srvs, SR14, 4, k);
    TakeColumns(SR14, {0, 1, 2, 3});

    FilterNextK(partyIdx, srvs, SR14, 0, 3, SR3, 0, 3, k);
    ProductK(partyIdx, srvs, SR14, 0, 3, SR3, 0, 3, k, SR134);

    // A, B, C, Annot1, A, D, Annot2, SHA2, Annot1*SHA2
    ColumnMult(partyIdx, srvs, SR134, 3, 6, 3);
    TakeColumns(SR134, {0, 1, 2, 5, 3});
    // SR134: A, B, C, D, annot

    // STableReveal(srvs, SR134);

    // Step 3: R134 Join R2
    FilterNextK(partyIdx, srvs, SR134, 0, 4, SR2, 0, 3, k);
    ProductK(partyIdx, srvs, SR134, 0, 4, SR2, 0, 3, k, ST);

    // ST: A, B, C, D, Annot1, A, E, Annot2, SHA2, Annot1*SHA2
    ColumnMult(partyIdx, srvs, ST, 4, 7, 4);
    TakeColumns(ST, {0, 1, 2, 3, 6, 4});

    STableReveal(srvs, ST);
    auto end_time = clock();

    std::cout << "TIME = " << 1.0 * (end_time - start_time) / CLOCKS_PER_SEC << " s" << std::endl;
    std::cout << "COMM = " << srvs.mRt.mComm.mNext.getTotalDataSent() + srvs.mRt.mComm.mPrev.getTotalDataSent() + srvs.mRt.mComm.mNext.getTotalDataRecv() + srvs.mRt.mComm.mPrev.getTotalDataRecv() << " Bytes" << std::endl;

}

void L3Star(u64 partyIdx) {
    IOService ios;
    DBServer srvs;
    DBSvrSetup(partyIdx, ios, srvs);

    std::vector<ColumnInfo> RColInfo = { ColumnInfo{ "A", TypeID::IntID, 64 }, ColumnInfo{ "B", TypeID::IntID, 64 }, ColumnInfo{ "Annot", TypeID::IntID, 64 }, ColumnInfo{ "SHA", TypeID::IntID, 64 } };
    std::vector<u64> R_A = {1, 1, 2, 2, 2, 3}; 
    std::vector<u64> R_B = {1, 2, 3, 4, 5, 6};
    std::vector<u64> R_Annot = {1, 10, 2, 4, 8, 9}; 
    u64 rnrows = R_A.size();
    Table R(rnrows, RColInfo);
    for (auto i=0; i<rnrows; ++i) {
        R.mColumns[0].mData(i, 0) = R_A[i];
        R.mColumns[1].mData(i, 0) = R_B[i];
        R.mColumns[2].mData(i, 0) = R_Annot[i];
        R.mColumns[3].mData(i, 0) = R_Annot[i];
    }
    SharedTable SR1, SR2, SR3;
    if (partyIdx == 0) {
        SR1 = srvs.localInput(R);
        SR2 = srvs.localInput(R);
        SR3 = srvs.localInput(R);
    } else {
        SR1 = srvs.remoteInput(0);
        SR2 = srvs.remoteInput(0);
        SR3 = srvs.remoteInput(0);
    }


    u64 k = 4;
    SharedTable SR13, ST;

    auto start_time = clock();
    
    // Step 1: R1 Join R3
    CalcSHA(partyIdx, srvs, SR1, 0, 3, SR2, 0, 3);
    ShuffleSelectK(partyIdx, srvs, SR1, 3, k);
    TakeColumns(SR1, {0, 1, 2});

    FilterNextK(partyIdx, srvs, SR1, 0, 2, SR3, 0, 3, k);
    ProductK(partyIdx, srvs, SR1, 0, 2, SR3, 0, 3, k, SR13);

    // A, B, Annot1, A, D, Annot2, SHA2, Annot1*SHA2
    ColumnMult(partyIdx, srvs, SR13, 2, 5, 2);
    TakeColumns(SR13, {0, 1, 4, 2});
    // SR13: A, B, C, annot

    STableReveal(srvs, SR13);

    // Step 2: R13 Join R2
    FilterNextK(partyIdx, srvs, SR13, 0, 3, SR2, 0, 3, k);
    ProductK(partyIdx, srvs, SR13, 0, 3, SR2, 0, 3, k, ST);

    // ST: A, B, C, Annot1, A, E, Annot2, SHA2, Annot1*SHA2
    ColumnMult(partyIdx, srvs, ST, 3, 6, 3);
    TakeColumns(ST, {0, 1, 2, 5, 3});

    STableReveal(srvs, ST);
    auto end_time = clock();

    std::cout << "TIME = " << 1.0 * (end_time - start_time) / CLOCKS_PER_SEC << " s" << std::endl;
    std::cout << "COMM = " << srvs.mRt.mComm.mNext.getTotalDataSent() + srvs.mRt.mComm.mPrev.getTotalDataSent() + srvs.mRt.mComm.mNext.getTotalDataRecv() + srvs.mRt.mComm.mPrev.getTotalDataRecv() << " Bytes" << std::endl;

}


int main(int argc, char** argv) {
    if (argc == 0) return 0;
    
    // CheckPhase((argv[1][0] - '0'));
    // L3((argv[1][0] - '0'));
    // L4Star((argv[1][0] - '0'));
    L3Star((argv[1][0] - '0'));
    return 0;
}