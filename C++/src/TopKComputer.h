#pragma once

#include "Table.h"
#include <stdexcept>

class TopKComputer
{
public:
    TopKComputer(int _K, AssociateOperator _oplus, AssociateOperator _otimes, std::string _resultOpColName) : K(_K), oplus(_oplus), otimes(_otimes), resultOpColName(_resultOpColName)
    {
        if (oplus != OP_MAX && oplus != OP_MIN)
            throw std::invalid_argument("OPLUS should be either MIN or MAX!");
        desc = oplus == OP_MAX;
    }
    void setLeft(Table *table, std::vector<int> joinCols, int annotCol);
    void setRight(Table *table, std::vector<int> joinCols, int annotCol);
    void truncateTableByJoin(Table *table, std::vector<int> cols, int annot_col, Table *t, std::vector<int> t_cols, int t_annot_col);
    void semiAnnotJoin(Table *left, std::vector<int> leftCols, int leftAnnotCol, Table *right, std::vector<int> rightCols, int rightAnnotCol);
    int getRightColAfterJoin(int rightCol);
    Table productK();
    Table levelK(int base = 2);
    int K;
    bool desc;
    AssociateOperator oplus, otimes;

private:
    Table *leftTable = NULL, *rightTable = NULL;
    std::vector<int> leftJoinCols, rightJoinCols;
    int leftAnnotCol, rightAnnotCol;
    std::string resultOpColName;
    void checkTables();
    // return: whether next level exists
    bool levelKLevelJoin(Table *left, Table *output, std::unordered_map<long long, Table> &rightMap, int begin, int end);
};
