#pragma once
#include <vector>
#include <string>
#include <cassert> 
#include "Operators.h"
#include "Tuple.h"
#include <unordered_map>
#include <functional>

class TopKComputer;

class Table
{
public:
    Table(int num_cols=0);
    Table(const char *filePath);
    Table(const Tuple t);
    Table(std::vector<Tuple> &tuples);
    Table(std::vector<Tuple> &tuples, std::vector<std::string> &col_names);
    int size() const;
    int numColumns() const;
    Table copy();
    long long hash(int seed=0) const;
    void print(int limit_size = 10); 
    std::vector<int> getColumnIdsByNames(std::vector<std::string> column_names);
    void addSuffixToColumnNames(std::string suffix);
    void appendColumn(std::string column_name, int default_value=0);
    void copyColumn(int original_col, std::string column_name);
    void colOp(int col1, int col2, AssociateOperator op, std::string column_name);
    void filter(std::function<bool(const Tuple&)> predicate);
    void sort(const std::vector<int> columns, bool desc=false);
    void selectK(int K, const std::vector<int> columns, bool desc=false);
    void project(const std::vector<int> columns);
    void antiProject(const std::vector<int> columns);
    int aggregate(AssociateOperator &op, int column) const;
    void groupByAggregate(std::vector<int> group_by_columns, int agg_column, std::string agg_name, AssociateOperator &op);
    void unionAll(const Table &t);
    Table cartesianJoin(const Table &t) const;
    Table join(std::vector<int> cols, const Table &t, std::vector<int> t_cols) const;
    friend class TopKComputer;
protected:
    std::vector<Tuple> m_tuples;
    std::vector<std::string> m_columnNames;
    // Load data into the Table 
    void loadDataFromFile(const char *filePath, std::vector<Tuple> &output);
    std::unordered_map<long long, Table> groupBy(std::vector<int> cols) const;
    void removeRightJoinColsAfterJoin(int numLeftCols, std::vector<int> rightJoinCols);
};
