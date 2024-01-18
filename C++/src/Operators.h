#pragma once
#include <functional>
#include <climits>
#include <string>

struct AssociateOperator
{
    AssociateOperator(int z, std::function<int(int, int)> o, std::string _name) : zero(z), invoke(o), name(_name) {}
    int zero;
    std::function<int(int, int)> invoke;
    std::string name;
    bool operator==(AssociateOperator const &op)
    {
        return name == op.name;
    }
    bool operator!=(AssociateOperator const &op)
    {
        return name != op.name;
    }
};

static AssociateOperator OP_ADD(
    0, [](int a, int b)
    { return a + b; },
    "ADD");
static AssociateOperator OP_MUL(
    1, [](int a, int b)
    { return a * b; },
    "MUL");
static AssociateOperator OP_MAX(
    INT_MIN, [](int a, int b)
    { return a > b ? a : b; },
    "MAX");
static AssociateOperator OP_MIN(
    INT_MAX, [](int a, int b)
    { return a < b ? a : b; },
    "MIN");
