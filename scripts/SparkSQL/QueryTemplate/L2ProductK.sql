with
R as (select src as A, dst as B, rating as Z from graph),
S as (select src as B, dst as C, rating as Z from graph),
-- Compute suffix maximum annotations
S1 as (select B, C, Z, Z as ZZ from S),
S11 as (select B, max(ZZ) as ZZ from S1 group by B),
R1 as (select A, R.B as B, R.Z as Z from R join S11 on R.B=S11.B order by R.Z+S11.ZZ desc limit #K),
-- R join S
R11 as (select B, max(Z) as Z from R1 group by B),
S2 as (select S1.B as B, C, S1.Z as Z, ZZ from R11 join S1 on R11.B=S1.B order by R11.Z+S1.ZZ desc limit #K),
RS as (select A, R1.B as B, C, R1.Z+S2.Z as Z from R1 join S2 on R1.B=S2.B order by R1.Z+S2.ZZ desc limit #K)
select * from RS
