CREATE TABLE graph (src bigint, dst bigint, rating bigint);
COPY graph FROM 'Data/#GRAPH.csv' ( DELIMITER '|' );

