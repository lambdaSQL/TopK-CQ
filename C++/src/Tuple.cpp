#include <cassert>
#include "Tuple.h"

// std::vector<int> &Tuple::getData()
// {
//     return data;
// }

int Tuple::at(int index) const
{
    return data[index];
}

int Tuple::size() const
{
    return data.size();
}

// bool Tuple::operator==(const Tuple &t) const
// {
//     int size = (int)data.size();
//     assert(size == (int)t.data.size() && "Tuples not comparable!");
//     for (int i = 0; i < size; i++)
//         if (data[i] != t.data[i])
//             return false;
//     return true;
// }

// bool Tuple::operator<(const Tuple &t) const
// {
//     int size = (int)data.size();
//     assert(size == (int)t.data.size() && "Tuples not comparable!");
//     for (int i = 0; i < size; i++)
//     {
//         if (data[i] < t.data[i])
//             return true;
//         else if (data[i] > t.data[i])
//             return false;
//     }
//     return false;
// }

bool Tuple::compareTo(const Tuple &t, const std::vector<int> &columns, bool desc)
{
    auto size = data.size();
    assert(size == t.data.size() && "Tuples not comparable!");
    for (auto col : columns)
    {
        assert(col < size && "Index out of range!");
        if (data[col] < t.data[col])
            return !desc;
        else if (data[col] > t.data[col])
            return desc;
    }
    return false;
}

Tuple Tuple::getSubTuple(const std::vector<int> &index_list) const
{
    Tuple new_tuple;
    for (auto id : index_list)
        new_tuple.data.push_back(data[id]);
    return new_tuple;
}

long long Tuple::hash(const std::vector<int> &columns, int seed) const
{
    int sz = columns.size();
    if(sz == 1)
        return data[columns[0]];
    if(sz == 2)
        return (((long long)data[columns[0]]) << 32 ) | data[columns[1]];
    int seed2 = seed + sz;
    for (auto id : columns)
    {
        auto x = data[id];
        x = ((x >> 16) ^ x) * 0x45d9f3b;
        x = ((x >> 16) ^ x) * 0x45d9f3b;
        x = (x >> 16) ^ x;
        x += 0x9e3779b9;
        seed ^= x + (seed << 6) + (seed >> 2);
        seed2 ^= x + (seed2 << 6) + (seed2 >> 2);
    }
    long long ret = seed;
    return (ret << 32) | seed2;
}

long long Tuple::hash(int seed) const
{
    int sz = size();
    if(sz == 1)
        return data[0];
    if(sz == 2)
        return (((long long)data[0]) << 32 ) | data[1];
    int seed2 = seed + sz;
    for (auto x : data)
    {
        x = ((x >> 16) ^ x) * 0x45d9f3b;
        x = ((x >> 16) ^ x) * 0x45d9f3b;
        x = (x >> 16) ^ x;
        x += 0x9e3779b9;
        seed ^= x + (seed << 6) + (seed >> 2);
        seed2 ^= x + (seed2 << 6) + (seed2 >> 2);
    }
    long long ret = seed;
    return (ret << 32) | seed2;
}

Tuple Tuple::concat(const Tuple &t) const // concatenate
{
    Tuple ret;
    ret.data = this->data; // vector copy happens
    ret.data.insert(ret.data.end(), t.data.begin(), t.data.end());
    return ret;
}
