#include <iostream>
#include "algorithm/algorithm.h"

int main() {
    std::string dataset = "dblp"; // input dataset expected to have name dblp.csv
    int limit = 0; // this option does not do anything yet.
    run_algorithm(AlgorithmType::PATH_2_FULL, dataset, limit);
//    run_algorithm(AlgorithmType::PATH_4_FULL, dataset, limit);
    dataset = "epinions"; // input dataset expected to have name dblp.csv
    run_algorithm(AlgorithmType::PATH_2_FULL, dataset, limit);
    dataset = "twitter"; // input dataset expected to have name dblp.csv
    run_algorithm(AlgorithmType::PATH_2_FULL, dataset, limit);
    dataset = "wiki-vote"; // input dataset expected to have name dblp.csv
    run_algorithm(AlgorithmType::PATH_2_FULL, dataset, limit);
    dataset = "bitcoin"; // input dataset expected to have name dblp.csv
    run_algorithm(AlgorithmType::PATH_2_FULL, dataset, limit);



    return 0;
}

