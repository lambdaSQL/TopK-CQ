#pragma once
#include <vector>

class Tuple
{
public:
	std::vector<int> data;
	Tuple(){}
	Tuple(std::vector<int> &data_) : data(data_) {}

	//std::vector<int>& getData();
	int at(int index) const;
	int size() const;

	// dictionary comparison
	//bool operator==(const Tuple &t) const; 
	//bool operator<(const Tuple &t) const; 
	bool compareTo(const Tuple &t, const std::vector<int> &columns, bool desc);
	
	// [a,b,c,d,e].slice([2,4])=[c,e]
	Tuple getSubTuple(const std::vector<int> &index_list) const;

	long long hash(int seed=0) const;
    long long hash(const std::vector<int> &columns, int seed=0) const;

	Tuple concat(const Tuple &t) const;
};