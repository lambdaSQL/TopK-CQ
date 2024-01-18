add_executable(TableTest TableTest.cpp)
target_link_libraries(TableTest RelationalSrc)
target_include_directories(TableTest PRIVATE ${INCLUDE_DIR})
add_test(NAME TableTest COMMAND $<TARGET_FILE:TableTest> "${DATA_DIR}/test.txt test_sort")

add_executable(TopKTest TopKTest.cpp)
target_link_libraries(TopKTest RelationalSrc)
target_include_directories(TopKTest PRIVATE ${INCLUDE_DIR})
add_test(NAME TopKTest COMMAND $<TARGET_FILE:TopKTest> "${DATA_DIR}/bitcoin_dense.txt test_productK")

add_executable(TopK TopK.cpp)
target_link_libraries(TopK RelationalSrc)
target_include_directories(TopK PRIVATE ${INCLUDE_DIR})