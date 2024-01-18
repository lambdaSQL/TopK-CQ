#include "Table.h"
#include "TopKComputer.h"
#include <iostream>
#include <vector>
#include <string>
#include <random>
#include <unordered_map>
#include <chrono>
#include <cstdlib>
#include <functional>

long long Tick(std::string name, bool printTickTime = true)
{
    static std::unordered_map<std::string, std::chrono::system_clock::time_point> tick_table;
    auto it = tick_table.find(name);
    if (it != tick_table.end())
    {
        auto duration = std::chrono::system_clock::now() - it->second;
        tick_table.erase(it);
        long long elaspe = std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();
        if (printTickTime)
            std::cout << name << ": " << elaspe << "ms" << std::endl;
        return elaspe;
    }
    tick_table[name] = std::chrono::system_clock::now();
    return 0;
}

std::vector<Table> copy_tables(Table &table, int n)
{
    std::vector<Table> ret(n);
    for (int i = 0; i < n; i++)
    {
        ret[i] = table.copy();
        ret[i].addSuffixToColumnNames(std::to_string(i));
    }
    return ret;
}

// print digests of Top-K result table to verify correctness
void print_digest(Table &table)
{
    int col_id = table.numColumns() - 1;
    std::cout << "Result digest: MIN=" << table.aggregate(OP_MIN, col_id) << ", ";
    std::cout << "MAX=" << table.aggregate(OP_MAX, col_id) << ", ";
    std::cout << "SUM=" << table.aggregate(OP_ADD, col_id) << std::endl;
}

enum METHOD
{
    PRODUCTK,
    LEVELK,
    ALL
};

inline std::string ToString(METHOD method)
{
    switch (method)
    {
    case PRODUCTK:
        return "productK";
    case LEVELK:
        return "levelK";
    default:
        return "[Unknown method]";
    }
}

Table line3_naive(std::vector<Table> &tables, TopKComputer &tkc)
{
    Tick("Total");

    Tick("Joins");
    auto table01 = tables[0].join({1}, tables[1], {0});
    auto ret = table01.join({3}, tables[2], {1});
    ret.colOp(2, 4, tkc.otimes, "[annot0*annot1]");
    ret.colOp(7, 6, tkc.otimes, "annot");
    ret.antiProject({2, 4, 6, 7});
    Tick("Joins");

    Tick("Top-K");
    ret.selectK(tkc.K, {4}, tkc.desc);
    ret.sort({4}, tkc.desc);
    Tick("Top-K");
    Tick("Total");
    return ret;
}

Table line2(std::vector<Table> &tables, TopKComputer &tkc, METHOD method, int base)
{
    assert(tables.size() == 2);
    /*
        tables0 (src0, dst0, annot0), size=n
        tables1 (src1, dst1, annot1), size=n
    */
    Tick("Total");
    Tick("Preprocessing");
    tkc.truncateTableByJoin(&tables[0], {1}, 2, &tables[1], {0}, 2);
    Tick("Preprocessing");
    /*
        tables0 (src0, dst0, annot0), size=k
        tables1 (src1, dst1, annot1), size=n
    */
    Tick("Binary joins");
    tkc.setLeft(&tables[0], {1}, 2);
    tkc.setRight(&tables[1], {0}, 2);
    Table output;
    if (method == PRODUCTK)
        output = tkc.productK();
    else
        output = tkc.levelK(base);
    output.antiProject({2, 4});
    Tick("Binary joins");
    output.sort({output.numColumns() - 1}, tkc.desc);
    Tick("Total");
    return output;
}

Table line3(std::vector<Table> &tables, TopKComputer &tkc, METHOD method, int base)
{
    assert(tables.size() == 3);
    /*
        tables0 (src0, dst0, annot0), size=n
        tables1 (src1, dst1, annot1), size=n
        tables2 (src2, dst2, annot2), size=n
    */
    Tick("Total");
    Tick("Preprocessing");
    auto table2_agg = tables[2].copy();
    table2_agg.groupByAggregate({0}, 2, "[annot2_agg]", tkc.oplus);
    tkc.semiAnnotJoin(&tables[1], {1}, 2, &table2_agg, {0}, 1);
    tkc.truncateTableByJoin(&tables[0], {1}, 2, &tables[1], {0}, 3);
    Tick("Preprocessing");
    /*
        tables0 (src0, dst0, annot0), size=k
        tables1 (src1, dst1, annot1, annot1'), size=n
        tables2 (src2, dst2, annot2), size=n
        annot1' = annot1+min(annot2)
    */
    Tick("Binary joins");
    tkc.setLeft(&tables[0], {1}, 2);
    tkc.setRight(&tables[1], {0}, 3);
    Table output;
    if (method == PRODUCTK)
        output = tkc.productK();
    else
        output = tkc.levelK(base);
    output.colOp(2, 4, tkc.otimes, "[annot01]");
    // remove (annot0, annot1, annot1', annot0+annot1')
    output.antiProject({2, 4, 5, 6});
    /*
        output (src0, dst0, dst1, annot01), size=n
        tables2 (src2, dst2, annot2), size=n
        annot01 = annot0+annot1
    */
    tkc.setLeft(&output, {2}, 3);
    tkc.setRight(&tables[2], {0}, 2);
    if (method == PRODUCTK)
        output = tkc.productK();
    else
        output = tkc.levelK(base);
    output.antiProject({3, 5});
    Tick("Binary joins");
    output.sort({output.numColumns() - 1}, tkc.desc);
    Tick("Total");
    return output;
}

Table line4(std::vector<Table> &tables, TopKComputer &tkc, METHOD method, int base)
{
    assert(tables.size() == 4);
    /*
        tables0 (src0, dst0, annot0), size=n
        tables1 (src1, dst1, annot1), size=n
        tables2 (src2, dst2, annot2), size=n
        tables3 (src3, dst3, annot3), size=n
    */
    Tick("Total");
    Tick("Preprocessing");
    auto table3_agg = tables[3].copy();
    table3_agg.groupByAggregate({0}, 2, "[annot3_agg]", tkc.oplus);
    tkc.semiAnnotJoin(&tables[2], {1}, 2, &table3_agg, {0}, 1);
    auto table2_agg = tables[2].copy();
    table2_agg.groupByAggregate({0}, 3, "[annot2_agg]", tkc.oplus);
    tkc.semiAnnotJoin(&tables[1], {1}, 2, &table2_agg, {0}, 1);
    tkc.truncateTableByJoin(&tables[0], {1}, 2, &tables[1], {0}, 3);
    Tick("Preprocessing");
    /*
        tables0 (src0, dst0, annot0), size=k
        tables1 (src1, dst1, annot1, annot1'), size=n
        tables2 (src2, dst2, annot2, annot2'), size=n
        tables3 (src3, dst3, annot3), size=n
        annot2' = annot2+min(annot3)
        annot1' = annot1+min(annot2')
    */
    Tick("Binary joins");
    tkc.setLeft(&tables[0], {1}, 2);
    tkc.setRight(&tables[1], {0}, 3);
    Table output;
    if (method == PRODUCTK)
        output = tkc.productK();
    else
        output = tkc.levelK(base);
    output.colOp(2, 4, tkc.otimes, "[annot01]");
    // remove (annot0, annot1, annot1', annot0+annot1')
    output.antiProject({2, 4, 5, 6});
    /*
        output (src0, dst0, dst1, annot01), size=n
        tables2 (src2, dst2, annot2, annot2'), size=n
        tables3 (src3, dst3, annot3), size=n
        annot01 = annot0+annot1
    */
    tkc.setLeft(&output, {2}, 3);
    tkc.setRight(&tables[2], {0}, 3);
    if (method == PRODUCTK)
        output = tkc.productK();
    else
        output = tkc.levelK(base);
    output.colOp(3, 5, tkc.otimes, "[annot012]");
    output.antiProject({3, 5, 6, 7});
    /*
         output (src0, dst0, dst1, dst2, annot012), size=n
         tables3 (src3, dst3, annot3), size=n
         annot01 = annot0+annot1
     */
    tkc.setLeft(&output, {3}, 4);
    tkc.setRight(&tables[3], {0}, 2);
    if (method == PRODUCTK)
        output = tkc.productK();
    else
        output = tkc.levelK(base);
    output.antiProject({4, 6});
    Tick("Binary joins");
    output.sort({output.numColumns() - 1}, tkc.desc);
    Tick("Total");
    return output;
}

std::vector<METHOD> parse_method(char *arg)
{
    std::string s(arg);
    if (s == "productK")
        return {PRODUCTK};
    if (s == "levelK")
        return {LEVELK};
    // if (s == "naive")
    //     return {NAIVE};
    if (s == "all")
        return {PRODUCTK, LEVELK};
    std::cout << "Unknown method: " << s << std::endl;
    return {};
}

std::function<Table(std::vector<Table> &, TopKComputer &, METHOD, int)> parse_query(char *arg, int &num_tables)
{
    std::string s(arg);
    std::function<void(std::vector<Table> &, TopKComputer &, METHOD, int)> query_func;
   if (s == "line2")
    {
        num_tables = 2;
        return line2;
    }
    if (s == "line3")
    {
        num_tables = 3;
        return line3;
    }
    if (s == "line4")
    {
        num_tables = 4;
        return line4;
    }
    std::cout << "Unknown query: " << s << std::endl;
    return NULL;
}

TopKComputer *parse_options(int argc, char **argv, int &base)
{
    int argid = 4;
    int K = 1024;
    AssociateOperator oplus = OP_MIN, otimes = OP_ADD;
    while (argid < argc)
    {
        std::string option(argv[argid++]);
        if (option == "-K")
            K = atoi(argv[argid++]);
        else if (option == "-b")
            base = atoi(argv[argid++]);
        else if (option == "-max")
            oplus = OP_MAX;
        else if (option == "-mul")
            otimes = OP_MUL;
        else
        {
            std::cout << "Unknown argument: " << option << std::endl;
            return NULL;
        }
    }
    return new TopKComputer(K, oplus, otimes, "annot");
}

int main(int argc, char **argv)
{
    if (argc <= 3)
    {
        std::cout << "Usage: TopK <input_file> <method> <query> <options>" << std::endl;
        // does not support naive, because it is too slow
        std::cout << "Method: productK/levelK/all" << std::endl;
        std::cout << "Query: line2/line3/line4" << std::endl;
        std::cout << "Options: [-K int] [-b int] [-max] [-mul]" << std::endl;
        return 1;
    }
    Table gtable(argv[1]);
    auto methods = parse_method(argv[2]);
    int num_tables;
    auto query = parse_query(argv[3], num_tables);
    if (query == NULL)
        return 1;
    int base = 2;
    auto tkc = parse_options(argc, argv, base);
    if (tkc == NULL)
        return 1;
    std::cout << "Data file = " << argv[1] << ", ";
    std::cout << "Query = " << argv[3] << std::endl;
    std::cout << "K=" << tkc->K << ",base=" << base << ",oplus=" << tkc->oplus.name << ",otimes=" << tkc->otimes.name << std::endl << std::endl;
    for (auto method : methods)
    {
        std::cout << "--- Method " << ToString(method) << " ---" << std::endl;
        auto tables = copy_tables(gtable, num_tables);
        auto result = query(tables, *tkc, method, base);
        print_digest(result);
        std::cout << std::endl;
    }

    return 0;
}