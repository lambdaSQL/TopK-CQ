drop view if exists graph_1;
create view graph_1 as (
with graph_2_max as (
select src, max(rating) as max_accweight
from graph
group by src
)
select graph.src as src, graph.dst as dst, 
    graph.rating as rating, graph.rating+graph_2_max.max_accweight as accweight
from graph join graph_2_max on graph.dst = graph_2_max.src
);
drop view if exists graph_0;
create view graph_0 as (
with graph_1_max as (
select src, max(accweight) as max_accweight
from graph_1
group by src
)
select graph.src as src, graph.dst as dst, 
    graph.rating as rating, graph.rating+graph_1_max.max_accweight as accweight
from graph join graph_1_max on graph.dst = graph_1_max.src
order by accweight desc limit 1024
);
drop table if exists graph_1_rnk;
create table graph_1_rnk as (
    with graph_0_max as (
        select dst, max(rating) as max_weight
        from graph_0
        group by dst
    ), graph_truncated_1 as (
        select graph_1.src as src, graph_1.dst as dst, 
        graph_1.rating as rating, graph_1.accweight as accweight
        from graph_0_max join graph_1 on graph_0_max.dst = graph_1.src
        order by graph_0_max.max_weight+graph_1.accweight desc limit 1024
    )
    select src as joinkey, dst, rating, accweight, row_number() over(partition by src order by accweight desc) as rnk
    from graph_truncated_1
);
create index rnk_index on graph_1_rnk (rnk);
drop view if exists graph_acc_1;
create view graph_acc_1 as (
with levelk_right_0 as (select * from graph_1_rnk where rnk<=2),
levelk_join_0 as (select graph_0.src as src, levelk_right_0.joinkey as joinkey, levelk_right_0.dst as dst, levelk_right_0.rnk as rnk,
    graph_0.rating as left_weight, graph_0.rating+levelk_right_0.rating as rating, graph_0.rating+levelk_right_0.accweight as accweight
    from graph_0 join levelk_right_0 on graph_0.dst=levelk_right_0.joinkey
    order by accweight desc limit 1024),
levelk_left_1 as (select src, joinkey, left_weight as rating from levelk_join_0 where rnk=2),
levelk_right_1 as (select * from graph_1_rnk where rnk>2 and rnk<=4),
levelk_join_1 as (select levelk_left_1.src as src, levelk_right_1.joinkey as joinkey, levelk_right_1.dst as dst, levelk_right_1.rnk as rnk,
    levelk_left_1.rating as left_weight, levelk_left_1.rating+levelk_right_1.rating as rating, levelk_left_1.rating+levelk_right_1.accweight as accweight
    from levelk_left_1 join levelk_right_1 on levelk_left_1.joinkey=levelk_right_1.joinkey 
    union all select * from levelk_join_0
    order by accweight desc limit 1024),
levelk_left_2 as (select src, joinkey, left_weight as rating from levelk_join_1 where rnk=4),
levelk_right_2 as (select * from graph_1_rnk where rnk>4 and rnk<=8),
levelk_join_2 as (select levelk_left_2.src as src, levelk_right_2.joinkey as joinkey, levelk_right_2.dst as dst, levelk_right_2.rnk as rnk,
    levelk_left_2.rating as left_weight, levelk_left_2.rating+levelk_right_2.rating as rating, levelk_left_2.rating+levelk_right_2.accweight as accweight
    from levelk_left_2 join levelk_right_2 on levelk_left_2.joinkey=levelk_right_2.joinkey 
    union all select * from levelk_join_1
    order by accweight desc limit 1024),
levelk_left_3 as (select src, joinkey, left_weight as rating from levelk_join_2 where rnk=8),
levelk_right_3 as (select * from graph_1_rnk where rnk>8 and rnk<=16),
levelk_join_3 as (select levelk_left_3.src as src, levelk_right_3.joinkey as joinkey, levelk_right_3.dst as dst, levelk_right_3.rnk as rnk,
    levelk_left_3.rating as left_weight, levelk_left_3.rating+levelk_right_3.rating as rating, levelk_left_3.rating+levelk_right_3.accweight as accweight
    from levelk_left_3 join levelk_right_3 on levelk_left_3.joinkey=levelk_right_3.joinkey 
    union all select * from levelk_join_2
    order by accweight desc limit 1024),
levelk_left_4 as (select src, joinkey, left_weight as rating from levelk_join_3 where rnk=16),
levelk_right_4 as (select * from graph_1_rnk where rnk>16 and rnk<=32),
levelk_join_4 as (select levelk_left_4.src as src, levelk_right_4.joinkey as joinkey, levelk_right_4.dst as dst, levelk_right_4.rnk as rnk,
    levelk_left_4.rating as left_weight, levelk_left_4.rating+levelk_right_4.rating as rating, levelk_left_4.rating+levelk_right_4.accweight as accweight
    from levelk_left_4 join levelk_right_4 on levelk_left_4.joinkey=levelk_right_4.joinkey 
    union all select * from levelk_join_3
    order by accweight desc limit 1024),
levelk_left_5 as (select src, joinkey, left_weight as rating from levelk_join_4 where rnk=32),
levelk_right_5 as (select * from graph_1_rnk where rnk>32 and rnk<=64),
levelk_join_5 as (select levelk_left_5.src as src, levelk_right_5.joinkey as joinkey, levelk_right_5.dst as dst, levelk_right_5.rnk as rnk,
    levelk_left_5.rating as left_weight, levelk_left_5.rating+levelk_right_5.rating as rating, levelk_left_5.rating+levelk_right_5.accweight as accweight
    from levelk_left_5 join levelk_right_5 on levelk_left_5.joinkey=levelk_right_5.joinkey 
    union all select * from levelk_join_4
    order by accweight desc limit 1024),
levelk_left_6 as (select src, joinkey, left_weight as rating from levelk_join_5 where rnk=64),
levelk_right_6 as (select * from graph_1_rnk where rnk>64 and rnk<=128),
levelk_join_6 as (select levelk_left_6.src as src, levelk_right_6.joinkey as joinkey, levelk_right_6.dst as dst, levelk_right_6.rnk as rnk,
    levelk_left_6.rating as left_weight, levelk_left_6.rating+levelk_right_6.rating as rating, levelk_left_6.rating+levelk_right_6.accweight as accweight
    from levelk_left_6 join levelk_right_6 on levelk_left_6.joinkey=levelk_right_6.joinkey 
    union all select * from levelk_join_5
    order by accweight desc limit 1024),
levelk_left_7 as (select src, joinkey, left_weight as rating from levelk_join_6 where rnk=128),
levelk_right_7 as (select * from graph_1_rnk where rnk>128 and rnk<=256),
levelk_join_7 as (select levelk_left_7.src as src, levelk_right_7.joinkey as joinkey, levelk_right_7.dst as dst, levelk_right_7.rnk as rnk,
    levelk_left_7.rating as left_weight, levelk_left_7.rating+levelk_right_7.rating as rating, levelk_left_7.rating+levelk_right_7.accweight as accweight
    from levelk_left_7 join levelk_right_7 on levelk_left_7.joinkey=levelk_right_7.joinkey 
    union all select * from levelk_join_6
    order by accweight desc limit 1024),
levelk_left_8 as (select src, joinkey, left_weight as rating from levelk_join_7 where rnk=256),
levelk_right_8 as (select * from graph_1_rnk where rnk>256 and rnk<=512),
levelk_join_8 as (select levelk_left_8.src as src, levelk_right_8.joinkey as joinkey, levelk_right_8.dst as dst, levelk_right_8.rnk as rnk,
    levelk_left_8.rating as left_weight, levelk_left_8.rating+levelk_right_8.rating as rating, levelk_left_8.rating+levelk_right_8.accweight as accweight
    from levelk_left_8 join levelk_right_8 on levelk_left_8.joinkey=levelk_right_8.joinkey 
    union all select * from levelk_join_7
    order by accweight desc limit 1024),
levelk_left_9 as (select src, joinkey, left_weight as rating from levelk_join_8 where rnk=512),
levelk_right_9 as (select * from graph_1_rnk where rnk>512 and rnk<=1024),
levelk_join_9 as (select levelk_left_9.src as src, levelk_right_9.joinkey as joinkey, levelk_right_9.dst as dst, levelk_right_9.rnk as rnk,
    levelk_left_9.rating as left_weight, levelk_left_9.rating+levelk_right_9.rating as rating, levelk_left_9.rating+levelk_right_9.accweight as accweight
    from levelk_left_9 join levelk_right_9 on levelk_left_9.joinkey=levelk_right_9.joinkey 
    union all select * from levelk_join_8
    order by accweight desc limit 1024)
    select src, dst, rating
    from levelk_join_9
);
drop table if exists graph_2_rnk;
create table graph_2_rnk as (
    with graph_acc_1_max as (
        select dst, max(rating) as max_weight
        from graph_acc_1
        group by dst
    ), graph_truncated_2 as (
        select graph.src as src, graph.dst as dst, 
        graph.rating as rating, graph.rating as accweight
        from graph_acc_1_max join graph on graph_acc_1_max.dst = graph.src
        order by graph_acc_1_max.max_weight+graph.rating desc limit 1024
    )
    select src as joinkey, dst, rating, accweight, row_number() over(partition by src order by accweight desc) as rnk
    from graph_truncated_2
);
create index rnk_index on graph_2_rnk (rnk);
drop view if exists graph_acc_2;
create view graph_acc_2 as (
with levelk_right_0 as (select * from graph_2_rnk where rnk<=2),
levelk_join_0 as (select graph_acc_1.src as src, levelk_right_0.joinkey as joinkey, levelk_right_0.dst as dst, levelk_right_0.rnk as rnk,
    graph_acc_1.rating as left_weight, graph_acc_1.rating+levelk_right_0.rating as rating, graph_acc_1.rating+levelk_right_0.accweight as accweight
    from graph_acc_1 join levelk_right_0 on graph_acc_1.dst=levelk_right_0.joinkey
    order by accweight desc limit 1024),
levelk_left_1 as (select src, joinkey, left_weight as rating from levelk_join_0 where rnk=2),
levelk_right_1 as (select * from graph_2_rnk where rnk>2 and rnk<=4),
levelk_join_1 as (select levelk_left_1.src as src, levelk_right_1.joinkey as joinkey, levelk_right_1.dst as dst, levelk_right_1.rnk as rnk,
    levelk_left_1.rating as left_weight, levelk_left_1.rating+levelk_right_1.rating as rating, levelk_left_1.rating+levelk_right_1.accweight as accweight
    from levelk_left_1 join levelk_right_1 on levelk_left_1.joinkey=levelk_right_1.joinkey 
    union all select * from levelk_join_0
    order by accweight desc limit 1024),
levelk_left_2 as (select src, joinkey, left_weight as rating from levelk_join_1 where rnk=4),
levelk_right_2 as (select * from graph_2_rnk where rnk>4 and rnk<=8),
levelk_join_2 as (select levelk_left_2.src as src, levelk_right_2.joinkey as joinkey, levelk_right_2.dst as dst, levelk_right_2.rnk as rnk,
    levelk_left_2.rating as left_weight, levelk_left_2.rating+levelk_right_2.rating as rating, levelk_left_2.rating+levelk_right_2.accweight as accweight
    from levelk_left_2 join levelk_right_2 on levelk_left_2.joinkey=levelk_right_2.joinkey 
    union all select * from levelk_join_1
    order by accweight desc limit 1024),
levelk_left_3 as (select src, joinkey, left_weight as rating from levelk_join_2 where rnk=8),
levelk_right_3 as (select * from graph_2_rnk where rnk>8 and rnk<=16),
levelk_join_3 as (select levelk_left_3.src as src, levelk_right_3.joinkey as joinkey, levelk_right_3.dst as dst, levelk_right_3.rnk as rnk,
    levelk_left_3.rating as left_weight, levelk_left_3.rating+levelk_right_3.rating as rating, levelk_left_3.rating+levelk_right_3.accweight as accweight
    from levelk_left_3 join levelk_right_3 on levelk_left_3.joinkey=levelk_right_3.joinkey 
    union all select * from levelk_join_2
    order by accweight desc limit 1024),
levelk_left_4 as (select src, joinkey, left_weight as rating from levelk_join_3 where rnk=16),
levelk_right_4 as (select * from graph_2_rnk where rnk>16 and rnk<=32),
levelk_join_4 as (select levelk_left_4.src as src, levelk_right_4.joinkey as joinkey, levelk_right_4.dst as dst, levelk_right_4.rnk as rnk,
    levelk_left_4.rating as left_weight, levelk_left_4.rating+levelk_right_4.rating as rating, levelk_left_4.rating+levelk_right_4.accweight as accweight
    from levelk_left_4 join levelk_right_4 on levelk_left_4.joinkey=levelk_right_4.joinkey 
    union all select * from levelk_join_3
    order by accweight desc limit 1024),
levelk_left_5 as (select src, joinkey, left_weight as rating from levelk_join_4 where rnk=32),
levelk_right_5 as (select * from graph_2_rnk where rnk>32 and rnk<=64),
levelk_join_5 as (select levelk_left_5.src as src, levelk_right_5.joinkey as joinkey, levelk_right_5.dst as dst, levelk_right_5.rnk as rnk,
    levelk_left_5.rating as left_weight, levelk_left_5.rating+levelk_right_5.rating as rating, levelk_left_5.rating+levelk_right_5.accweight as accweight
    from levelk_left_5 join levelk_right_5 on levelk_left_5.joinkey=levelk_right_5.joinkey 
    union all select * from levelk_join_4
    order by accweight desc limit 1024),
levelk_left_6 as (select src, joinkey, left_weight as rating from levelk_join_5 where rnk=64),
levelk_right_6 as (select * from graph_2_rnk where rnk>64 and rnk<=128),
levelk_join_6 as (select levelk_left_6.src as src, levelk_right_6.joinkey as joinkey, levelk_right_6.dst as dst, levelk_right_6.rnk as rnk,
    levelk_left_6.rating as left_weight, levelk_left_6.rating+levelk_right_6.rating as rating, levelk_left_6.rating+levelk_right_6.accweight as accweight
    from levelk_left_6 join levelk_right_6 on levelk_left_6.joinkey=levelk_right_6.joinkey 
    union all select * from levelk_join_5
    order by accweight desc limit 1024),
levelk_left_7 as (select src, joinkey, left_weight as rating from levelk_join_6 where rnk=128),
levelk_right_7 as (select * from graph_2_rnk where rnk>128 and rnk<=256),
levelk_join_7 as (select levelk_left_7.src as src, levelk_right_7.joinkey as joinkey, levelk_right_7.dst as dst, levelk_right_7.rnk as rnk,
    levelk_left_7.rating as left_weight, levelk_left_7.rating+levelk_right_7.rating as rating, levelk_left_7.rating+levelk_right_7.accweight as accweight
    from levelk_left_7 join levelk_right_7 on levelk_left_7.joinkey=levelk_right_7.joinkey 
    union all select * from levelk_join_6
    order by accweight desc limit 1024),
levelk_left_8 as (select src, joinkey, left_weight as rating from levelk_join_7 where rnk=256),
levelk_right_8 as (select * from graph_2_rnk where rnk>256 and rnk<=512),
levelk_join_8 as (select levelk_left_8.src as src, levelk_right_8.joinkey as joinkey, levelk_right_8.dst as dst, levelk_right_8.rnk as rnk,
    levelk_left_8.rating as left_weight, levelk_left_8.rating+levelk_right_8.rating as rating, levelk_left_8.rating+levelk_right_8.accweight as accweight
    from levelk_left_8 join levelk_right_8 on levelk_left_8.joinkey=levelk_right_8.joinkey 
    union all select * from levelk_join_7
    order by accweight desc limit 1024),
levelk_left_9 as (select src, joinkey, left_weight as rating from levelk_join_8 where rnk=512),
levelk_right_9 as (select * from graph_2_rnk where rnk>512 and rnk<=1024),
levelk_join_9 as (select levelk_left_9.src as src, levelk_right_9.joinkey as joinkey, levelk_right_9.dst as dst, levelk_right_9.rnk as rnk,
    levelk_left_9.rating as left_weight, levelk_left_9.rating+levelk_right_9.rating as rating, levelk_left_9.rating+levelk_right_9.accweight as accweight
    from levelk_left_9 join levelk_right_9 on levelk_left_9.joinkey=levelk_right_9.joinkey 
    union all select * from levelk_join_8
    order by accweight desc limit 1024)
    select src, dst, rating
    from levelk_join_9
);
select count(*) from graph_acc_2
