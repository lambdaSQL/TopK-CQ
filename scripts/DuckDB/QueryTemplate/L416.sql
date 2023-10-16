drop view if exists graph_2;
create view graph_2 as (
with graph_3_max as (
select src, max(rating) as max_accweight
from graph
group by src
)
select graph.src as src, graph.dst as dst, 
    graph.rating as rating, graph.rating+graph_3_max.max_accweight as accweight
from graph join graph_3_max on graph.dst = graph_3_max.src
);
drop view if exists graph_1;
create view graph_1 as (
with graph_2_max as (
select src, max(accweight) as max_accweight
from graph_2
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
create index rnk_index_1 on graph_1_rnk (rnk);
drop view if exists graph_acc_1;
create view graph_acc_1 as (
with levelk_right_0 as (select * from graph_1_rnk where rnk<=16),
levelk_join_0 as (select graph_0.src as src, levelk_right_0.joinkey as joinkey, levelk_right_0.dst as dst, levelk_right_0.rnk as rnk,
    graph_0.rating as left_weight, graph_0.rating+levelk_right_0.rating as rating, graph_0.rating+levelk_right_0.accweight as accweight
    from graph_0 join levelk_right_0 on graph_0.dst=levelk_right_0.joinkey
    order by accweight desc limit 1024),
levelk_left_1 as (select src, joinkey, left_weight as rating from levelk_join_0 where rnk=16),
levelk_right_1 as (select * from graph_1_rnk where rnk>16 and rnk<=256),
levelk_join_1 as (select levelk_left_1.src as src, levelk_right_1.joinkey as joinkey, levelk_right_1.dst as dst, levelk_right_1.rnk as rnk,
    levelk_left_1.rating as left_weight, levelk_left_1.rating+levelk_right_1.rating as rating, levelk_left_1.rating+levelk_right_1.accweight as accweight
    from levelk_left_1 join levelk_right_1 on levelk_left_1.joinkey=levelk_right_1.joinkey 
    union all select * from levelk_join_0
    order by accweight desc limit 1024),
levelk_left_2 as (select src, joinkey, left_weight as rating from levelk_join_1 where rnk=256),
levelk_right_2 as (select * from graph_1_rnk where rnk>256 and rnk<=4096),
levelk_join_2 as (select levelk_left_2.src as src, levelk_right_2.joinkey as joinkey, levelk_right_2.dst as dst, levelk_right_2.rnk as rnk,
    levelk_left_2.rating as left_weight, levelk_left_2.rating+levelk_right_2.rating as rating, levelk_left_2.rating+levelk_right_2.accweight as accweight
    from levelk_left_2 join levelk_right_2 on levelk_left_2.joinkey=levelk_right_2.joinkey 
    union all select * from levelk_join_1
    order by accweight desc limit 1024)
    select src, dst, rating
    from levelk_join_2
);
drop table if exists graph_2_rnk;
create table graph_2_rnk as (
    with graph_acc_1_max as (
        select dst, max(rating) as max_weight
        from graph_acc_1
        group by dst
    ), graph_truncated_2 as (
        select graph_2.src as src, graph_2.dst as dst, 
        graph_2.rating as rating, graph_2.accweight as accweight
        from graph_acc_1_max join graph_2 on graph_acc_1_max.dst = graph_2.src
        order by graph_acc_1_max.max_weight+graph_2.accweight desc limit 1024
    )
    select src as joinkey, dst, rating, accweight, row_number() over(partition by src order by accweight desc) as rnk
    from graph_truncated_2
);
create index rnk_index_2 on graph_2_rnk (rnk);
drop view if exists graph_acc_2;
create view graph_acc_2 as (
with levelk_right_0 as (select * from graph_2_rnk where rnk<=16),
levelk_join_0 as (select graph_acc_1.src as src, levelk_right_0.joinkey as joinkey, levelk_right_0.dst as dst, levelk_right_0.rnk as rnk,
    graph_acc_1.rating as left_weight, graph_acc_1.rating+levelk_right_0.rating as rating, graph_acc_1.rating+levelk_right_0.accweight as accweight
    from graph_acc_1 join levelk_right_0 on graph_acc_1.dst=levelk_right_0.joinkey
    order by accweight desc limit 1024),
levelk_left_1 as (select src, joinkey, left_weight as rating from levelk_join_0 where rnk=16),
levelk_right_1 as (select * from graph_2_rnk where rnk>16 and rnk<=256),
levelk_join_1 as (select levelk_left_1.src as src, levelk_right_1.joinkey as joinkey, levelk_right_1.dst as dst, levelk_right_1.rnk as rnk,
    levelk_left_1.rating as left_weight, levelk_left_1.rating+levelk_right_1.rating as rating, levelk_left_1.rating+levelk_right_1.accweight as accweight
    from levelk_left_1 join levelk_right_1 on levelk_left_1.joinkey=levelk_right_1.joinkey 
    union all select * from levelk_join_0
    order by accweight desc limit 1024),
levelk_left_2 as (select src, joinkey, left_weight as rating from levelk_join_1 where rnk=256),
levelk_right_2 as (select * from graph_2_rnk where rnk>256 and rnk<=4096),
levelk_join_2 as (select levelk_left_2.src as src, levelk_right_2.joinkey as joinkey, levelk_right_2.dst as dst, levelk_right_2.rnk as rnk,
    levelk_left_2.rating as left_weight, levelk_left_2.rating+levelk_right_2.rating as rating, levelk_left_2.rating+levelk_right_2.accweight as accweight
    from levelk_left_2 join levelk_right_2 on levelk_left_2.joinkey=levelk_right_2.joinkey 
    union all select * from levelk_join_1
    order by accweight desc limit 1024)
    select src, dst, rating
    from levelk_join_2
);
drop table if exists graph_3_rnk;
create table graph_3_rnk as (
    with graph_acc_2_max as (
        select dst, max(rating) as max_weight
        from graph_acc_2
        group by dst
    ), graph_truncated_3 as (
        select graph.src as src, graph.dst as dst, 
        graph.rating as rating, graph.rating as accweight
        from graph_acc_2_max join graph on graph_acc_2_max.dst = graph.src
        order by graph_acc_2_max.max_weight+graph.rating desc limit 1024
    )
    select src as joinkey, dst, rating, accweight, row_number() over(partition by src order by accweight desc) as rnk
    from graph_truncated_3
);
create index rnk_index_3 on graph_3_rnk (rnk);
drop view if exists graph_acc_3;
create view graph_acc_3 as (
with levelk_right_0 as (select * from graph_3_rnk where rnk<=16),
levelk_join_0 as (select graph_acc_2.src as src, levelk_right_0.joinkey as joinkey, levelk_right_0.dst as dst, levelk_right_0.rnk as rnk,
    graph_acc_2.rating as left_weight, graph_acc_2.rating+levelk_right_0.rating as rating, graph_acc_2.rating+levelk_right_0.accweight as accweight
    from graph_acc_2 join levelk_right_0 on graph_acc_2.dst=levelk_right_0.joinkey
    order by accweight desc limit 1024),
levelk_left_1 as (select src, joinkey, left_weight as rating from levelk_join_0 where rnk=16),
levelk_right_1 as (select * from graph_3_rnk where rnk>16 and rnk<=256),
levelk_join_1 as (select levelk_left_1.src as src, levelk_right_1.joinkey as joinkey, levelk_right_1.dst as dst, levelk_right_1.rnk as rnk,
    levelk_left_1.rating as left_weight, levelk_left_1.rating+levelk_right_1.rating as rating, levelk_left_1.rating+levelk_right_1.accweight as accweight
    from levelk_left_1 join levelk_right_1 on levelk_left_1.joinkey=levelk_right_1.joinkey 
    union all select * from levelk_join_0
    order by accweight desc limit 1024),
levelk_left_2 as (select src, joinkey, left_weight as rating from levelk_join_1 where rnk=256),
levelk_right_2 as (select * from graph_3_rnk where rnk>256 and rnk<=4096),
levelk_join_2 as (select levelk_left_2.src as src, levelk_right_2.joinkey as joinkey, levelk_right_2.dst as dst, levelk_right_2.rnk as rnk,
    levelk_left_2.rating as left_weight, levelk_left_2.rating+levelk_right_2.rating as rating, levelk_left_2.rating+levelk_right_2.accweight as accweight
    from levelk_left_2 join levelk_right_2 on levelk_left_2.joinkey=levelk_right_2.joinkey 
    union all select * from levelk_join_1
    order by accweight desc limit 1024)
    select src, dst, rating
    from levelk_join_2
);
select sum(src), sum(dst), sum(rating) from graph_acc_3;