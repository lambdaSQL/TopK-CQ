CREATE TABLE graph (src bigint, dst bigint);
COPY graph FROM 'Data/graph.csv' ( DELIMITER '|' );

CREATE TABLE graph_t1(node1 bigint, node2 bigint, node3 bigint);
COPY graph_t1 FROM 'Data/graph_t1.csv' ( DELIMITER '|' );

CREATE TABLE graph_t2(node1 bigint, node2 bigint, node3 bigint);
COPY graph_t2 FROM 'Data/graph_t2.csv' ( DELIMITER '|' );

CREATE TABLE graph_t3(node1 bigint, node2 bigint, node3 bigint);
COPY graph_t3 FROM 'Data/graph_t3.csv' ( DELIMITER '|' );