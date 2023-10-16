create table nation
(
    n_nationkey  integer not null,
    n_name       char(25) not null,
    n_regionkey  integer not null,
    n_comment    varchar(152)
);
COPY nation FROM 'Data/nation.csv' ( DELIMITER '|' );

create table part
(
    p_partkey     bigint not null,
    p_name        varchar(55) not null,
    p_mfgr        char(25) not null,
    p_brand       char(10) not null,
    p_type        varchar(25) not null,
    p_size        integer not null,
    p_container   char(10) not null,
    p_retailprice decimal(7,2) not null,
    p_comment     varchar(23) not null
);
COPY part FROM 'Data/part.csv' ( DELIMITER '|' );

create table supplier
(
    s_suppkey     bigint not null,
    s_name        char(25) not null,
    s_address     varchar(40) not null,
    s_nationkey   integer not null,
    s_phone       char(15) not null,
    s_acctbal     decimal(7,2) not null,
    s_comment     varchar(101) not null
);
COPY supplier FROM 'Data/supplier.csv' ( DELIMITER '|' );

create table partsupp
(
    ps_partkey     bigint not null,
    ps_suppkey     bigint not null,
    ps_availqty    bigint not null,
    ps_supplycost  decimal(7,2)  not null,
    ps_comment     varchar(199) not null
);
COPY partsupp FROM 'Data/partsupp.csv' ( DELIMITER '|' );
