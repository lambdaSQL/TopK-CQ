CREATE TABLE graph (src bigint, dst bigint, rating int, unused bigint);
COPY graph FROM '/path/to/graph' ( DELIMITER ',' );