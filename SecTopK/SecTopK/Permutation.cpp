#include "Permutation.h"

#include <random>
#include <algorithm>

void DBServerSetup(u64 partyIdx, IOService& ios, DBServer& srvs) {
	if (partyIdx == 0) {
        // Each party sets its own private random seed.
        // Here we use seed = partyIdx for simplicity.
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

void Permutation::RandPerm(u64 partyIdx, DBServer& srvs, SharedTable& R) {

    block prevSeed = srvs.mEnc.mShareGen.mPrevCommon.getSeed();
    block nextSeed = srvs.mEnc.mShareGen.mNextCommon.getSeed();

    randPermCounter += 1; // for each permutation, we need to call a new seed
    PRNG prevPrng(prevSeed ^ oc::toBlock(u64(randPermCounter), u64(67119970127)));
    PRNG nextPrng(nextSeed ^ oc::toBlock(u64(randPermCounter), u64(67119970127)));

    u64 nrows = R.rows(), ncols = R.mColumns.size();
    std::vector<u64> prevPerm(nrows), nextPerm(nrows);
    for (auto i=0; i<nrows; ++i) {
        prevPerm[i] = nextPerm[i] = i;
    }
    std::shuffle(prevPerm.begin(), prevPerm.end(), prevPrng);
    std::shuffle(nextPerm.begin(), nextPerm.end(), nextPrng);

    // std::cerr << partyIdx << " prev perm \n";
    // for (auto i=0; i<nrows; ++i) {
    //     std::cerr << prevPerm[i] << ' ';
    // }
    // std::cerr << std::endl;

    // std::cerr << partyIdx << " next perm \n";
    // for (auto i=0; i<nrows; ++i) {
    //     std::cerr << nextPerm[i] << ' ';
    // }
    // std::cerr << std::endl;

    RowStoreTable prevMask(nrows, ncols), nextMask(nrows, ncols);
    prevMask.GenRand(prevPrng);
    nextMask.GenRand(nextPrng);

    // prevMask.Print(std::to_string(partyIdx) + " prev mask");
    // nextMask.Print(std::to_string(partyIdx) + " next mask");

    if (partyIdx == 0) {
        RowStoreTable A(nrows, ncols), B(nrows, ncols), Abar(nrows, ncols), Bbar(nrows, ncols);
        A.LoadData(R, 1); B.LoadData(R, 0);
        Abar.GenRand(prevPrng); Bbar.GenRand(nextPrng);

        // A.Print("0 A"); B.Print("0 B");

        auto X1 = A ^ B; X1 ^= nextMask; X1.Permute(nextPerm);

        auto X2 = X1 ^ prevMask; X2.Permute(prevPerm);
        
        srvs.mRt.mComm.mNext.send(X2.Serialize());

        Abar.AssignData(R, 1);  Bbar.AssignData(R, 0);
    } else if (partyIdx == 1) {
        RowStoreTable B(nrows, ncols), C(nrows, ncols), Bbar(nrows, ncols), Cbar(nrows, ncols);
        B.LoadData(R, 1); C.LoadData(R, 0);
        Bbar.GenRand(prevPrng);

        // B.Print("1 B"); C.Print("1 C");

        auto Y1 = C ^ prevMask; Y1.Permute(prevPerm);
        srvs.mRt.mComm.mNext.send(Y1.Serialize());

        std::vector<u64> serX2;
        srvs.mRt.mComm.mPrev.recv(serX2);
        RowStoreTable X2(nrows, ncols); X2.LoadSer(serX2);

        auto X3 = X2 ^ nextMask; X3.Permute(nextPerm);
        auto C1 = X3 ^ Bbar;

        srvs.mRt.mComm.mNext.send(C1.Serialize());
        std::vector<u64> serC2;
        srvs.mRt.mComm.mNext.recv(serC2);
        RowStoreTable C2(nrows, ncols); C2.LoadSer(serC2);

        Cbar = C1 ^ C2;

        Bbar.AssignData(R, 1); Cbar.AssignData(R, 0);
    } else {
        RowStoreTable C(nrows, ncols), A(nrows, ncols), Cbar(nrows, ncols), Abar(nrows, ncols);
        C.LoadData(R, 1); A.LoadData(R, 0);
        Abar.GenRand(nextPrng);

        // C.Print("2 C"); A.Print("2 A");

        std::vector<u64> serY1;
        srvs.mRt.mComm.mPrev.recv(serY1);
        RowStoreTable Y1(nrows, ncols); Y1.LoadSer(serY1);

        auto Y2 = Y1 ^ nextMask; Y2.Permute(nextPerm);
        auto Y3 = Y2 ^ prevMask; Y3.Permute(prevPerm);
        auto C2 = Y3 ^ Abar;

        std::vector<u64> serC1;
        srvs.mRt.mComm.mPrev.recv(serC1);
        RowStoreTable C1(nrows, ncols); C1.LoadSer(serC1);

        srvs.mRt.mComm.mPrev.send(C2.Serialize());

        Cbar = C1 ^ C2;
        Cbar.AssignData(R, 1); Abar.AssignData(R, 0);
    }
    return;
}

void Permutation::SSPerm(u64 partyIdx, DBServer& srvs, SharedTable& R, u64 colIdx, u64 offset) {
    
    block prevSeed = srvs.mEnc.mShareGen.mPrevCommon.getSeed();
    block nextSeed = srvs.mEnc.mShareGen.mNextCommon.getSeed();

    randPermCounter += 1; // for each permutation, we need to call a new seed
    PRNG prevPrng(prevSeed ^ oc::toBlock(u64(randPermCounter), u64(67119970127)));
    PRNG nextPrng(nextSeed ^ oc::toBlock(u64(randPermCounter), u64(67119970127)));

    u64 nrows = R.rows(), ncols = R.mColumns.size();
    std::vector<u64> prevPerm(nrows), nextPerm(nrows);
    for (auto i=0; i<nrows; ++i) {
        prevPerm[i] = nextPerm[i] = i;
    }
    std::shuffle(prevPerm.begin(), prevPerm.end(), prevPrng);
    std::shuffle(nextPerm.begin(), nextPerm.end(), nextPrng);

    RowStoreTable prevMask(nrows, ncols), nextMask(nrows, ncols);
    prevMask.GenRand(prevPrng);
    nextMask.GenRand(nextPrng);

    sbMatrix sColPI(nrows, 1);
    i64Matrix ColPI(nrows, 1);
    std::vector<u64> ssperm(nrows);

    if (partyIdx == 0) {
        RowStoreTable A(nrows, ncols), B(nrows, ncols), Abar(nrows, ncols), Bbar(nrows, ncols);
        A.LoadData(R, 1); B.LoadData(R, 0);
        Abar.GenRand(prevPrng); Bbar.GenRand(nextPrng);

        // A.Print("0 A"); B.Print("0 B");

        auto X1 = A ^ B; X1 ^= nextMask; X1.Permute(nextPerm);

        auto X2 = X1 ^ prevMask; X2.Permute(prevPerm);
        
        srvs.mRt.mComm.mNext.send(X2.Serialize());

        Abar.AssignData(sColPI, 1, colIdx);
        Bbar.AssignData(sColPI, 0, colIdx);
        srvs.mEnc.revealAll(srvs.mRt.mComm, sColPI, ColPI);
        for (auto i=0; i<nrows; ++i) {
            ssperm[i] = ColPI(i, 0) - offset;
        }
        Abar.Permute(ssperm); Bbar.Permute(ssperm);

        Abar.AssignData(R, 1);  Bbar.AssignData(R, 0);
    } else if (partyIdx == 1) {
        RowStoreTable B(nrows, ncols), C(nrows, ncols), Bbar(nrows, ncols), Cbar(nrows, ncols);
        B.LoadData(R, 1); C.LoadData(R, 0);
        Bbar.GenRand(prevPrng);

        // B.Print("1 B"); C.Print("1 C");

        auto Y1 = C ^ prevMask; Y1.Permute(prevPerm);
        srvs.mRt.mComm.mNext.send(Y1.Serialize());

        std::vector<u64> serX2;
        srvs.mRt.mComm.mPrev.recv(serX2);
        RowStoreTable X2(nrows, ncols); X2.LoadSer(serX2);

        auto X3 = X2 ^ nextMask; X3.Permute(nextPerm);
        auto C1 = X3 ^ Bbar;

        srvs.mRt.mComm.mNext.send(C1.Serialize());
        std::vector<u64> serC2;
        srvs.mRt.mComm.mNext.recv(serC2);
        RowStoreTable C2(nrows, ncols); C2.LoadSer(serC2);

        Cbar = C1 ^ C2;

        Bbar.AssignData(sColPI, 1, colIdx);
        Cbar.AssignData(sColPI, 0, colIdx);
        srvs.mEnc.revealAll(srvs.mRt.mComm, sColPI, ColPI);
        for (auto i=0; i<nrows; ++i) {
            ssperm[i] = ColPI(i, 0) - offset;
        }
        Bbar.Permute(ssperm); Cbar.Permute(ssperm);

        Bbar.AssignData(R, 1); Cbar.AssignData(R, 0);
    } else {
        RowStoreTable C(nrows, ncols), A(nrows, ncols), Cbar(nrows, ncols), Abar(nrows, ncols);
        C.LoadData(R, 1); A.LoadData(R, 0);
        Abar.GenRand(nextPrng);

        // C.Print("2 C"); A.Print("2 A");

        std::vector<u64> serY1;
        srvs.mRt.mComm.mPrev.recv(serY1);
        RowStoreTable Y1(nrows, ncols); Y1.LoadSer(serY1);

        auto Y2 = Y1 ^ nextMask; Y2.Permute(nextPerm);
        auto Y3 = Y2 ^ prevMask; Y3.Permute(prevPerm);
        auto C2 = Y3 ^ Abar;

        std::vector<u64> serC1;
        srvs.mRt.mComm.mPrev.recv(serC1);
        RowStoreTable C1(nrows, ncols); C1.LoadSer(serC1);

        srvs.mRt.mComm.mPrev.send(C2.Serialize());

        Cbar = C1 ^ C2;

        Cbar.AssignData(sColPI, 1, colIdx);
        Abar.AssignData(sColPI, 0, colIdx);
        srvs.mEnc.revealAll(srvs.mRt.mComm, sColPI, ColPI);
        for (auto i=0; i<nrows; ++i) {
            ssperm[i] = ColPI(i, 0) - offset;
        }
        Cbar.Permute(ssperm); Abar.Permute(ssperm);

        Cbar.AssignData(R, 1); Abar.AssignData(R, 0);
    }
    return;
}
