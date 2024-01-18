add_library(RelationalSrc
    Table.cpp
    Tuple.cpp
    TopKComputer.cpp
)

target_include_directories(RelationalSrc PRIVATE ${INCLUDE_DIR})