#include "Table.h"
#include <iostream>
#include <vector>
#include <string>
#include <random>

int test_copy(Table &gtable)
{
    std::cout << "--- Original Table ---" << std::endl;
    gtable.print();
    auto gtable2 = gtable.copy();
    std::cout << "--- Copied Table ---" << std::endl;
    gtable2.print();
    std::cout << "--- Copied Table Sorted ---" << std::endl;
    gtable2.sort({1});
    gtable2.print();
    std::cout << "--- Original Table ---" << std::endl;
    gtable.print();
    return 0;
}

int test_sort(Table &gtable)
{
    std::cout << "--- Table Sorted by Cols {0,2} ---" << std::endl;
    gtable.sort({0, 2});
    gtable.print();
    
    std::cout << "--- Table Sorted by Cols {2,1} desc ---" << std::endl;
    gtable.sort({2, 1}, true);
    gtable.print();
    return 0;
}

int test_group_by_aggregate(Table &gtable)
{
    auto gtable2 = gtable.copy();
    gtable2.groupByAggregate({0}, 1, "SUM(dst)", OP_ADD);
    std::cout << "--- Table Sum Col 1 Group by Cols {0} ---" << std::endl;
    gtable2.print();

    auto gtable3 = gtable.copy();
    gtable3.groupByAggregate({2}, 0, "MIN(src)", OP_MIN);
    std::cout << "--- Table Min Col 0 Group by Cols {2} ---" << std::endl;
    gtable3.print();
    return 0;
}

int test_product_join(Table &gtable)
{
    gtable.print();
    gtable.cartesianJoin(gtable).print();
    return 0;
}

int test_join(Table &gtable)
{
    gtable.print();
    std::cout << "--- Table Join on Cols {0} and Cols {1} ---" << std::endl;
    gtable.join({0},gtable,{1}).print();
    std::cout << "--- Table Join on Cols {0,1} and Cols {1,0} ---" << std::endl;
    gtable.join({0,1},gtable,{1,0}).print();
    return 0;
}

int test_selectK(Table &gtable)
{
    gtable.print();
    auto gtable2 = gtable.copy();
    std::cout << "--- Table Select 10 on Cols {1} ---" << std::endl;
    gtable2.selectK(10,{1});
    gtable2.print();

    auto gtable3 = gtable.copy();
    std::cout << "--- Table Select 7 on Cols {3,1} ---" << std::endl;
    gtable3.selectK(7,{3,1});
    gtable3.print();
    return 0;
}

int main(int argc, char **argv)
{
    if (argc <= 1)
    {
        std::cout << "Usage: ./test <input_file> [test_copy] [test_sort]" << std::endl;
        return 1;
    }
    Table gtable(argv[1]);
    for (int i = 2; i < argc; i++)
    {
        int ret;
        std::string s(argv[i]);
        if (s == "test_copy")
            ret = test_copy(gtable);
        else if (s == "test_sort")
            ret = test_sort(gtable);
        else if (s == "test_group_by_aggregate")
            ret = test_group_by_aggregate(gtable);
        else if (s == "test_product_join")
            ret = test_product_join(gtable);
        else if (s == "test_join")
            ret = test_join(gtable);
        else if (s == "test_selectK")
            ret = test_selectK(gtable);
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