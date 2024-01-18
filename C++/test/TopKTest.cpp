#include "Table.h"
#include "TopKComputer.h"
#include <iostream>
#include <vector>
#include <string>
#include <random>

Table copy(Table &table, int i)
{
    auto ret = table.copy();
    ret.addSuffixToColumnNames(std::to_string(i));
    return ret;
}

int test_productK(Table &gtable)
{
    gtable.print();
    TopKComputer tkc(7, OP_MIN, OP_ADD, "annot");

    std::cout << "--- Table Join on Cols {0} and Cols {0}, Add Col 1 and Col 1, Select 7---" << std::endl;
    auto left = copy(gtable, 1);
    auto right = copy(gtable, 2);
    tkc.setLeft(&left, {0}, 1);
    tkc.setRight(&right, {0}, 1);
    tkc.productK().print();

    std::cout << "--- Table Join on Cols {0} and Cols {1}, Add Col 2 and Col 2, Select 7---" << std::endl;
    left = copy(gtable, 1);
    right = copy(gtable, 2);
    tkc.setLeft(&left, {0}, 2);
    tkc.setRight(&right, {1}, 2);
    tkc.productK().print();
    return 0;
}

int test_levelK(Table &gtable)
{
    gtable.print();
    TopKComputer tkc(7, OP_MIN, OP_ADD, "annot");

    std::cout << "--- Table Join on Cols {0} and Cols {0}, Add Col 1 and Col 1, Select 7---" << std::endl;
    auto left = copy(gtable, 1);
    auto right = copy(gtable, 2);
    tkc.setLeft(&left, {0}, 1);
    tkc.setRight(&right, {0}, 1);
    tkc.levelK().print();

    std::cout << "--- Table Join on Cols {0} and Cols {1}, Add Col 2 and Col 2, Select 7---" << std::endl;
    left = copy(gtable, 1);
    right = copy(gtable, 2);
    tkc.setLeft(&left, {0}, 2);
    tkc.setRight(&right, {1}, 2);
    tkc.levelK().print();
    return 0;
}

int test(Table &gtable)
{
    TopKComputer tkc(1024, OP_MAX, OP_MUL, "annot");
    auto left = copy(gtable, 1);
    auto right = copy(gtable, 2);
    tkc.setLeft(&left, {1}, 2);
    tkc.setRight(&right, {0}, 2);
    auto joined = tkc.productK();
    joined.sort({3});
    joined.print(100);
    std::cout << "Result hashed = " << joined.hash() << std::endl;

    left = copy(gtable, 1);
    right = copy(gtable, 2);
    tkc.setLeft(&left, {1}, 2);
    tkc.setRight(&right, {0}, 2);
    joined = tkc.levelK(32);
    joined.sort({3});
    joined.print(100);
    std::cout << "Result hashed = " << joined.hash() << std::endl;
    return 0;
}

int main(int argc, char **argv)
{
    if (argc <= 1)
    {
        std::cout << "Usage: ./test <input_file> [test]" << std::endl;
        return 1;
    }
    Table gtable(argv[1]);
    for (int i = 2; i < argc; i++)
    {
        int ret;
        std::string s(argv[i]);
        if (s == "test_productK")
            ret = test_productK(gtable);
        else if (s == "test_levelK")
            ret = test_levelK(gtable);
        else if (s == "test")
            ret = test(gtable);
        else
        {
            std::cout << "Unknown argument: " << s << std::endl;
            return 1;
        }
        if (ret != 0)
        {
            std::cout << s << " failed!" << std::endl;
            return ret;
        }
    }
    return 0;
}