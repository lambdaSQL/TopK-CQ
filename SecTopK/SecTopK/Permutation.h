// Random permutation
// Secret-shared permutation

#pragma once

#include <iostream>
#include <vector>

#include "cryptoTools/Network/IOService.h"
#include "cryptoTools/Common/Timer.h"
#include "aby3/sh3/Sh3Runtime.h"
#include "aby3/sh3/Sh3Encryptor.h"
#include "aby3/sh3/Sh3Evaluator.h"
#include "aby3/sh3/Sh3BinaryEvaluator.h"
#include "aby3-DB/DBServer.h"

using namespace oc;
using namespace aby3;

void DBServerSetup(u64 partyIdx, IOService& ios, DBServer& srvs);

class Permutation {
public:
    Permutation() {randPermCounter = 0;}
    void RandPerm(u64 partyIdx, DBServer& srvs, SharedTable& R);
    void SSPerm(u64 partyIdx, DBServer& srvs, SharedTable& R, u64 colIdx, u64 offset = 1);
private:
    u64 randPermCounter;
};

// extern Permutation perm;

class RowStoreTable {
public:
    RowStoreTable(u64 nrows, u64 ncols) {
        Init(nrows, ncols);
    }
    void Init(u64 nrows, u64 ncols) {
        nrows_ = nrows;
        ncols_ = ncols;
        table_.resize(nrows);
        for (auto i=0; i<nrows; ++i) {
            table_[i].resize(ncols);
        }
    }
    void LoadData(const SharedTable R, u64 shareID) {
        for (auto i=0; i<nrows_; ++i) {
            for (auto j=0; j<ncols_; ++j) {
                table_[i][j] = R.mColumns[j].mShares[shareID](i);
            }
        }
    }
    void AssignData(SharedTable &R, u64 shareID) {
        for (auto i=0; i<nrows_; ++i) {
            for (auto j=0; j<ncols_; ++j) {
                R.mColumns[j].mShares[shareID](i) = table_[i][j];
            }
        }
    }
    void AssignData(sbMatrix &sm, u64 shareID, u64 colID) {
        for (auto i=0; i<nrows_; ++i) {
            sm.mShares[shareID](i) = table_[i][colID];
        }
    }
    void GenRand(oc::PRNG& prng) {
        for (auto i=0; i<nrows_; ++i) {
            for (auto j=0; j<ncols_; ++j) {
                table_[i][j] = prng.get<u64>();
            }
        }
    }
    void operator^=(const RowStoreTable B) {
        for (auto i=0; i<nrows_; ++i) {
            for (auto j=0; j<ncols_; ++j) {
                table_[i][j] ^= B.table_[i][j];
            }
        }
    }
    RowStoreTable operator^(const RowStoreTable B) {
        RowStoreTable R(nrows_, ncols_);
        for (auto i=0; i<nrows_; ++i) {
            for (auto j=0; j<ncols_; ++j) {
                R.table_[i][j] = table_[i][j] ^ B.table_[i][j];
            }
        }
        return R;
    }
    void Permute(const std::vector<u64> perm) { // Permute as perm^-1(X)
        std::vector<std::vector<u64>> temp = table_;
        for (auto i=0; i<perm.size(); ++i) {
            // table_[i] = temp[perm[i]];  // This permutes as perm(X)
            table_[perm[i]] = temp[i]; // This special case for SSPerm
        }
    }
    std::vector<u64> Serialize() {
        std::vector<u64> ser;
        for (auto i=0; i<nrows_; ++i) {
            ser.insert(ser.end(), table_[i].begin(), table_[i].end());
        }
        return ser;
    }
    void LoadSer(std::vector<u64> ser) {
        for (auto i=0, k=0; i<nrows_; ++i) {
            for (auto j=0; j<ncols_; ++j) {
                table_[i][j] = ser[k++];
            }
        }
    }
    void Print(std::string printinfo) {
        std::cerr << printinfo << std::endl;
        for (auto i=0; i<nrows_; ++i) {
            for (auto j=0; j<ncols_; ++j) {
                std::cerr << table_[i][j] << ' ';
            }
            std::cerr << std::endl;
        }
    }
    u64 nrows_, ncols_;
    std::vector<std::vector<u64>> table_;
};