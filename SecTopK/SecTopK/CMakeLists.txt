
# project(SecTopK - secure top-k query processing system)

include_directories(${CMAKE_SOURCE_DIR}/SecTopK/)

#############################################
##                test file                ##
#############################################

add_executable(test
    test.cpp
    join.cpp
    Permutation.cpp
    PrefixSumCircuit.cpp
    SingleRelationOperator.cpp
    sort.cpp
    LevelK.cpp
    ProductK.cpp
)

target_link_libraries(test com-psi)
target_link_libraries(test aby3-ML)
target_link_libraries(test com-psi_Tests)
target_link_libraries(test aby3_Tests)
target_link_libraries(test oc::tests_cryptoTools)

#############################################
##                demo file                ##
#############################################

add_executable(demo
    demo.cpp
    join.cpp
    Permutation.cpp
    PrefixSumCircuit.cpp
    SingleRelationOperator.cpp
    sort.cpp
    ProductK.cpp
    LevelK.cpp
    CalcSHA.cpp
)

target_link_libraries(demo com-psi)
target_link_libraries(demo aby3-ML)
target_link_libraries(demo com-psi_Tests)
target_link_libraries(demo aby3_Tests)
target_link_libraries(demo oc::tests_cryptoTools)
