#include "TopKComputer.h"
#include <numeric>
#include <iostream>

int TopKComputer::getRightColAfterJoin(int rightCol)
{
    auto ret = rightCol + leftTable->numColumns();
    for (auto col : rightJoinCols)
    {
        // visiting join column that has been removed
        if (col == rightAnnotCol)
            return -1;
        if (col < rightAnnotCol)
            ret--;
    }
    return ret;
}

void TopKComputer::setLeft(Table *table, std::vector<int> joinCols, int annotCol)
{
    leftTable = table;
    leftJoinCols = joinCols;
    leftAnnotCol = annotCol;
}

void TopKComputer::setRight(Table *table, std::vector<int> joinCols, int annotCol)
{
    rightTable = table;
    rightJoinCols = joinCols;
    rightAnnotCol = annotCol;
}

void TopKComputer::checkTables()
{
    if (leftTable == NULL)
        throw std::invalid_argument("Left table not set! Try TopKComputer::setLeft");
    if (rightTable == NULL)
        throw std::invalid_argument("Right table not set! Try TopKComputer::setRight");
    if (leftJoinCols.size() != rightJoinCols.size())
        throw std::invalid_argument("Join columns sizes do not match!");
}

// right table is unique on join columns
// R <-- select R.*, R.annot+S.annot from R natural join S
void TopKComputer::semiAnnotJoin(Table *left, std::vector<int> leftCols, int leftAnnotCol, Table *right, std::vector<int> rightCols, int rightAnnotCol)
{
    left->m_columnNames.push_back("[semiAnnotJoin]");
    std::vector<Tuple> new_tuples;
    auto rightMap = right->groupBy(rightCols);
    for (auto &tuple : left->m_tuples)
    {
        auto it = rightMap.find(tuple.hash(leftCols));
        if (it != rightMap.end())
        {
            if (it->second.size() != 1)
                throw std::invalid_argument("Right table not unique on join key!");
            tuple.data.push_back(otimes.invoke(tuple.at(leftAnnotCol), it->second.m_tuples[0].at(rightAnnotCol)));
            new_tuples.push_back(tuple);
        }
    }
    left->m_tuples = new_tuples;
}

void TopKComputer::truncateTableByJoin(Table *table, std::vector<int> cols, int annot_col, Table *t, std::vector<int> t_cols, int t_annot_col)
{
    if (table->size() <= 2 * K)
        return;
    Table aggRight = t->copy();
    aggRight.groupByAggregate(t_cols, t_annot_col, "[truncate]", oplus);
    std::vector<int> aggJoinCols(t_cols.size());
    std::iota(aggJoinCols.begin(), aggJoinCols.end(), 0);
    semiAnnotJoin(table, cols, annot_col, &aggRight, aggJoinCols, aggJoinCols.size());
    table->selectK(K, {table->numColumns() - 1}, desc);
    table->antiProject({table->numColumns() - 1});
}

Table TopKComputer::productK()
{
    checkTables();
    truncateTableByJoin(leftTable, leftJoinCols, leftAnnotCol, rightTable, rightJoinCols, rightAnnotCol);
    truncateTableByJoin(rightTable, rightJoinCols, rightAnnotCol, leftTable, leftJoinCols, leftAnnotCol);
    Table joined = leftTable->join(leftJoinCols, *rightTable, rightJoinCols);
    joined.colOp(leftAnnotCol, getRightColAfterJoin(rightAnnotCol), otimes, resultOpColName);
    joined.selectK(K, {joined.numColumns() - 1}, desc);
    return joined;
}

bool TopKComputer::levelKLevelJoin(Table *left, Table *output, std::unordered_map<long long, Table> &rightMap, int begin, int end)
{
    Table joined(leftTable->numColumns() + rightTable->numColumns() + 1);
    joined.m_columnNames = left->m_columnNames;
    joined.m_columnNames.insert(joined.m_columnNames.end(), rightTable->m_columnNames.begin(), rightTable->m_columnNames.end());
    joined.m_columnNames.push_back("[id]");
    for (auto &tuple : left->m_tuples)
    {
        auto it = rightMap.find(tuple.hash(leftJoinCols));
        if (it != rightMap.end())
        {
            Table &tb = it->second;
            int this_end = end > tb.size() ? tb.size() : end;
            for (int i = begin; i < this_end; i++)
                joined.m_tuples.push_back(tuple.concat(tb.m_tuples[i]));
        }
    }
    int s = left->aggregate(OP_ADD, leftAnnotCol);
    joined.colOp(leftAnnotCol, rightAnnotCol + leftTable->numColumns(), otimes, "[levelKLevel]");
    int t  = joined.aggregate(OP_ADD, leftAnnotCol);
    output->unionAll(joined);
    output->selectK(K, {output->numColumns() - 1}, desc);
    *left = output->copy();
    int idCol = left->numColumns() - 2;
    left->filter([idCol, end](const Tuple &tuple)
                 { return tuple.at(idCol) == end - 1; });
    if (left->size() == 0)
        return false;
    std::vector<int> leftAllCols(leftTable->numColumns());
    std::iota(leftAllCols.begin(), leftAllCols.end(), 0);
    left->project(leftAllCols);
    return true;
}

Table TopKComputer::levelK(int base)
{
    checkTables();
    truncateTableByJoin(rightTable, rightJoinCols, rightAnnotCol, leftTable, leftJoinCols, leftAnnotCol);
    auto rightMap = rightTable->groupBy(rightJoinCols);

    // preprocessing
    for (auto &it : rightMap)
    {
        Table &tb = it.second;
        tb.sort({rightAnnotCol}, desc);
        // append id
        tb.m_columnNames.push_back("[id]");
        for (int i = 0; i < tb.size(); i++)
            tb.m_tuples[i].data.push_back(i);
    }

    // The first round
    Table joined, left = leftTable->copy();
    bool has_next_level = true;
    int begin = 0, end = base, lv = 0;
    while (has_next_level)
    {
        // std::cout << "Left table rows=" << left.size() << " at level=" << lv++ << std::endl;
        assert(begin <= K);
        has_next_level = levelKLevelJoin(&left, &joined, rightMap, begin, end);
        begin = end;
        end = begin * base;
    }
    auto it = std::copy(leftTable->m_columnNames.begin(), leftTable->m_columnNames.end(), joined.m_columnNames.begin());
    std::copy(rightTable->m_columnNames.begin(), rightTable->m_columnNames.end(), it);
    joined.removeRightJoinColsAfterJoin(leftTable->numColumns(), rightJoinCols);
    // remove [id]
    joined.antiProject({joined.numColumns() - 2});
    joined.m_columnNames[joined.numColumns() - 1] = resultOpColName;
    return joined;
}