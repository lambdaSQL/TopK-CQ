with
R as (select src as A, dst as B, rating as Z from graph),
S as (select src as B, dst as C, rating as Z from graph),
T as (select src as C, dst as D, rating as Z from graph),
U as (select src as D, dst as E, rating as Z from graph),
-- Compute suffix maximum annotations
U1 as (select D, E, Z, Z as ZZ from U),
U11 as (select D, max(ZZ) as ZZ from U1 group by D),
T1 as (select C, T.D as D, T.Z as Z, T.Z+U11.ZZ as ZZ from T join U11 on T.D = U11.D),
T11 as (select C, max(ZZ) as ZZ from T1 group by C),
S1 as (select B, S.C as C, S.Z as Z, S.Z+T11.ZZ as ZZ from S join T11 on S.C=T11.C),
S11 as (select B, max(ZZ) as ZZ from S1 group by B),
R1 as (select A, R.B as B, R.Z as Z from R join S11 on R.B=S11.B order by R.Z+S11.ZZ desc limit #K),
-- R join S
R11 as (select B, max(Z) as Z from R1 group by B),
S2 as (select S1.B as B, C, S1.Z as Z, ZZ from R11 join S1 on R11.B=S1.B order by R11.Z+S1.ZZ desc limit #K),
RS as (select A, R1.B as B, C, R1.Z+S2.Z as Z from R1 join S2 on R1.B=S2.B order by R1.Z+S2.ZZ desc limit #K),
-- then join T
RS11 as (select C, max(Z) as Z from RS group by C),
T2 as (select T1.C as C, D, T1.Z as Z, ZZ from RS11 join T1 on RS11.C=T1.C order by RS11.Z+T1.ZZ desc limit #K),
RST as (select A, B, RS.C, D, RS.Z+T2.Z as Z from RS join T2 on RS.C=T2.C order by RS.Z+T2.ZZ desc limit #K),
-- then join U
RST11 as (select D, max(Z) as Z from RST group by D),
U2 as (select U1.D as D, E, U1.Z as Z, ZZ from RST11 join U1 on RST11.D=U1.D order by RST11.Z+U1.ZZ desc limit #K),
RSTU as (select A, B, C, RST.D, E, RST.Z+U2.Z as Z from RST join U2 on RST.D=U2.D order by Z desc limit #K)
select * from RSTU
