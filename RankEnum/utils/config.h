

#ifndef RANKEDLIMIT_CONFIG_H
#define RANKEDLIMIT_CONFIG_H

enum AlgorithmType {
    BFS_3PATH,
    BFS_4PATH,
    STAR_2LEX,
    STAR_3LEX,
    STAR_2OPT, // no listmerge
    STAR_3OPT, // no listmerge
    STAR_2, // uses listmerge
    PATH_3,
    PATH_3_FULL,
    PATH_4,
    PATH_4_FULL,
    PATH_2_FULL,
    UCQ_24PATH
};


#endif //RANKEDLIMIT_CONFIG_H

