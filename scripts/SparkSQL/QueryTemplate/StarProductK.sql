with
R as (select src as A, dst as B, rating as Z from graph),
S as (select src as A, dst as C, rating as Z from graph),
T as (select src as A, dst as D, rating as Z from graph),
-- Compute suffix maximum annotations
T1 as (select A, D, Z, Z as ZZ from T),
T11 as (select A, max(ZZ) as ZZ from T1 group by A),
R1 as (select R.A, R.B, R.Z, R.Z+T11.ZZ as ZZ from R join T11 on R.A=T11.A),
S1 as (select A, C, Z, Z as ZZ from S),
S11 as (select A, max(ZZ) as ZZ from S1 group by A),
R2 as (select R1.A, R1.B, R1.Z, R1.ZZ, R1.ZZ+S11.ZZ as ZZZ from R1 join S11 on R1.A=S11.A order by ZZZ desc limit #K),
-- R join S
R11 as (select A, max(ZZ) as Z from R2 group by A),
S2 as (select S1.A as A, C, S1.Z as Z, ZZ from R11 join S1 on R11.A=S1.A order by R11.Z+S1.ZZ desc limit #K),
RS as (select R2.A, B as B, C, R2.Z+S2.Z as Z from R2 join S2 on R2.A=S2.A order by R2.ZZ+S2.ZZ desc limit #K),
-- then join T
RS11 as (select A, max(Z) as Z from RS group by A),
T2 as (select T1.A as A, D, T1.Z as Z, ZZ from RS11 join T1 on RS11.A=T1.A order by RS11.Z+T1.ZZ desc limit #K),
RST as (select RS.A, B, C, D, RS.Z+T2.Z as Z from RS join T2 on RS.A=T2.A order by Z desc limit #K)
select * from RST
