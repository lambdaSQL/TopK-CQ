#include "Table.h"
#include <algorithm>
#include <iostream>
#include <fstream>
#include <sstream>
#include <random>
#include <cmath>
#include <unordered_map>
#include <numeric>

Table::Table(int num_cols)
{
    m_columnNames.resize(num_cols);
}

Table::Table(const char *filePath)
{
    loadDataFromFile(filePath, m_tuples);
}

Table::Table(const Tuple t)
{
    m_tuples = {t};
    m_columnNames.resize(t.size());
}

Table::Table(std::vector<Tuple> &tuples)
{
    m_tuples = tuples;
    if (tuples.size() > 0)
        m_columnNames.resize(tuples[0].size());
}

Table::Table(std::vector<Tuple> &tuples, std::vector<std::string> &col_names)
{
    m_tuples = tuples;
    m_columnNames = col_names;
}

// is only called by creator
void Table::loadDataFromFile(const char *filePath, std::vector<Tuple> &output)
{
    output.clear();

    // open file filePath
    std::ifstream fin(filePath, std::ios::in);
    if (!fin.is_open())
    {
        std::cerr << "ERROR: Cannot open data file: " << filePath << std::endl;
        throw;
    }
    std::string line;
    getline(fin, line);
    std::istringstream header(line);
    std::string column_name;
    while (header >> column_name)
        m_columnNames.push_back(column_name);
    int num_columns = numColumns();
    int num_rows = 0;
    while (getline(fin, line))
    {
        num_rows++;
        if (line.length() == 0)
            continue; // ignore empty lines
        std::istringstream ssline(line);
        std::vector<int> data(num_columns);
        for (int i = 0; i < num_columns; i++)
        {
            if (!(ssline >> data[i]))
            {
                std::cerr << "ERROR: Cannot read line " << num_rows << std::endl;
                throw;
            }
        }
        output.push_back(Tuple(data));
    }
    fin.close();
}

int Table::size() const
{
    return m_tuples.size();
}

int Table::numColumns() const
{
    return m_columnNames.size();
}

Table Table::copy()
{
    return Table(m_tuples, m_columnNames);
}

long long Table::hash(int seed) const
{
    long long res = 0;
    for (auto &tuple : m_tuples)
        res ^= tuple.hash(seed);
    return res;
}

void Table::print(int limit_size)
{
    std::cout << "(" << size() << " rows)" << std::endl;
    for (auto column_name : m_columnNames)
        std::cout << " " << column_name;
    std::cout << std::endl;
    int counter = 0;
    for (auto &tuple : m_tuples)
    {
        if (counter > limit_size)
        {
            std::cout << " ..." << std::endl;
            break;
        }
        for (auto v : tuple.data)
            std::cout << " " << v;
        std::cout << std::endl;
        counter++;
    }
    std::cout << std::endl;
}

std::vector<int> Table::getColumnIdsByNames(std::vector<std::string> column_names)
{
    std::vector<int> column_ids;
    for (auto column_name : column_names)
        for (int i = 0; i < numColumns(); i++)
            if (column_name == m_columnNames[i])
                column_ids.push_back(i);
    return column_ids;
}

void Table::addSuffixToColumnNames(std::string suffix)
{
    for (auto &colName : m_columnNames)
        colName.append(suffix);
}

void Table::project(const std::vector<int> columns)
{
    std::vector<std::string> projected_column_names;
    for (auto cid : columns)
        projected_column_names.push_back(m_columnNames[cid]);
    m_columnNames = projected_column_names;
    for (int i = 0; i < (int)m_tuples.size(); i++)
        m_tuples[i] = m_tuples[i].getSubTuple(columns);
}

void Table::antiProject(const std::vector<int> columns)
{
    int c = numColumns();
    std::vector<int> projected_cols;
    for (int i = 0; i < c; i++)
        if (std::find(columns.begin(), columns.end(), i) == columns.end())
            projected_cols.push_back(i);
    project(projected_cols);
}

void Table::appendColumn(std::string column_name, int default_value)
{
    m_columnNames.push_back(column_name);
    for (auto &tuple : m_tuples)
        tuple.data.push_back(default_value);
}

void Table::copyColumn(int original_col, std::string column_name)
{
    m_columnNames.push_back(column_name);
    for (int i = 0; i < size(); i++)
        m_tuples[i].data.push_back(m_tuples[i].at(original_col));
}

void Table::colOp(int col1, int col2, AssociateOperator op, std::string column_name)
{
    m_columnNames.push_back(column_name);
    for (int i = 0; i < size(); i++)
        m_tuples[i].data.push_back(op.invoke(m_tuples[i].at(col1), m_tuples[i].at(col2)));
}

void Table::sort(const std::vector<int> columns, bool desc)
{
    std::sort(m_tuples.begin(), m_tuples.end(), [&columns, &desc](Tuple &lhs, Tuple &rhs)
              { return lhs.compareTo(rhs, columns, desc); });
}

void Table::selectK(int K, const std::vector<int> columns, bool desc)
{
    if (size() <= K)
        return;
    std::nth_element(m_tuples.begin(), m_tuples.begin() + K, m_tuples.end(), [&columns, &desc](Tuple &lhs, Tuple &rhs)
                     { return lhs.compareTo(rhs, columns, desc); });
    m_tuples.resize(K);
}

int Table::aggregate(AssociateOperator &op, int column) const
{
    int ret = op.zero;
    for (auto &tuple : m_tuples)
        ret = op.invoke(ret, tuple.at(column));
    return ret;
}

std::unordered_map<long long, Table> Table::groupBy(std::vector<int> cols) const
{
    std::unordered_map<long long, Table> hashTupleMap;
    for (auto &tuple : m_tuples)
    {
        auto hashed = tuple.hash(cols);
        auto it = hashTupleMap.find(hashed);
        if (it != hashTupleMap.end())
            it->second.m_tuples.push_back(tuple);
        else
            hashTupleMap.emplace(hashed, tuple);
    }
    return hashTupleMap;
}

void Table::groupByAggregate(std::vector<int> group_by_columns, int agg_column, std::string agg_name, AssociateOperator &op)
{
    int num_columns = group_by_columns.size();
    group_by_columns.push_back(agg_column);
    project(group_by_columns);
    m_columnNames[num_columns] = agg_name;
    group_by_columns.resize(num_columns);
    std::iota(group_by_columns.begin(), group_by_columns.end(), 0);
    agg_column = num_columns;
    auto hashTupleMap = groupBy(group_by_columns);
    std::vector<Tuple> new_tuples;
    new_tuples.reserve(hashTupleMap.size());
    for (auto &it : hashTupleMap)
    {
        Tuple t = it.second.m_tuples[0];
        t.data[agg_column] = it.second.aggregate(op, agg_column);
        new_tuples.push_back(t);
    }
    m_tuples = new_tuples;
}

void Table::unionAll(const Table &t)
{
    // empty table can be unioned to another directly
    if(this->size() == 0 && this->m_columnNames.size() == 0)
        this->m_columnNames = t.m_columnNames;
    else
        assert(numColumns() == t.numColumns());
    m_tuples.insert(m_tuples.end(), t.m_tuples.begin(), t.m_tuples.end());
}

Table Table::cartesianJoin(const Table &t) const
{
    Table new_table;
    new_table.m_columnNames = m_columnNames;
    new_table.m_columnNames.insert(new_table.m_columnNames.end(), t.m_columnNames.begin(), t.m_columnNames.end());
    new_table.m_tuples.reserve(size() * t.size());
    for (auto &tuple : m_tuples)
        for (auto &t_tuple : t.m_tuples)
            new_table.m_tuples.push_back(tuple.concat(t_tuple));
    return new_table;
}

Table Table::join(std::vector<int> cols, const Table &t, std::vector<int> t_cols) const
{
    assert(cols.size() == t_cols.size());
    Table joined;
    joined.m_columnNames = m_columnNames;
    joined.m_columnNames.insert(joined.m_columnNames.end(), t.m_columnNames.begin(), t.m_columnNames.end());
    auto hashTupleMap = this->groupBy(cols);
    auto t_hashTupleMap = t.groupBy(t_cols);
    for (auto &it : hashTupleMap)
    {
        auto t_it = t_hashTupleMap.find(it.first);
        if (t_it != t_hashTupleMap.end())
            joined.unionAll(it.second.cartesianJoin(t_it->second));
    }
    joined.removeRightJoinColsAfterJoin(numColumns(), t_cols);
    return joined;
}

void Table::removeRightJoinColsAfterJoin(int numLeftCols, std::vector<int> rightJoinCols)
{
    for (auto &col: rightJoinCols)
        col += numLeftCols;
    antiProject(rightJoinCols);
}

void Table::filter(std::function<bool(const Tuple&)> predicate)
{
    std::vector<Tuple> new_tuples;
    for(auto &tuple: m_tuples)
        if(predicate(tuple))
            new_tuples.push_back(tuple);
    m_tuples = new_tuples;
}