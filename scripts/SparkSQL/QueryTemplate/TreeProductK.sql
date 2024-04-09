with
R as (select src as B, dst as A, rating as Z from graph),
S as (select src as C, dst as B, rating as Z from graph),
T as (select src as C, dst as D, rating as Z from graph),
U as (select src as C, dst as E, rating as Z from graph),
-- Compute suffix maximum annotations
-- For U
U1 as (select C, E, Z, Z as ZZ from U),
U11 as (select C, max(ZZ) as ZZ from U1 group by C),
-- For T
T1 as (select C, D, Z, Z as ZZ from T),
T11 as (select C, max(ZZ) as ZZ from T1 group by C),
-- U to S
S1 as (select S.B, S.C, S.Z, S.Z + U11.ZZ as ZZ from S join U11 on S.C = U11.C),
-- T to S
S2 as (select S1.B, S1.C, S1.Z, S1.ZZ, S1.ZZ+T11.ZZ as ZZZ from S1 join T11 on S1.C = T11.C),
S11 as (select S2.B, max(ZZZ) as ZZ from S2 group by S2.B),
R1 as (select R.A, R.B, R.Z, R.Z+S11.ZZ as ZZ from R join S11 on R.B=S11.B order by ZZ desc limit #K),
-- R join S
R11 as (select B, max(Z) as Z from R1 group by B),
S3 as (select S2.B as B, S2.C, S2.Z as Z, S2.ZZ as ZZ, S2.ZZZ as ZZZ from R11 join S2 on R11.B=S2.B order by R11.Z+S2.ZZZ desc limit #K),
RS as (select R1.A, R1.B as B, S3.C, R1.Z+S3.Z as Z, R1.Z+S3.ZZ as ZZ from R1 join S3 on R1.B=S3.B order by R1.Z+S3.ZZZ desc limit #K),
-- then join T
RS11 as (select C, max(ZZ) as Z from RS group by C),
T2 as (select T1.C as C, D, T1.Z as Z, T1.ZZ as ZZ from RS11 join T1 on RS11.C=T1.C order by RS11.Z+T1.ZZ desc limit #K),
RST as (select RS.A as A, RS.B as B, RS.C as C, T2.D as D, RS.Z+T2.Z as Z from RS join T2 on RS.C=T2.C order by RS.ZZ+T2.ZZ desc limit #K),
-- then join U
RST11 as (select C, max(Z) as Z from RST group by C),
U2 as (select U1.C as C, U1.E, U1.Z as Z, U1.ZZ as ZZ from RST11 join U1 on RST11.C = U1.C order by RST11.Z+U1.Z desc limit #K),
RSTU as (select RST.A as A, RST.B as B, RST.C as C, RST.D as D, U2.E as E, RST.Z+U2.Z as Z from RST join U2 on RST.C = U2.C order by Z desc limit #K)
select * from RSTU
