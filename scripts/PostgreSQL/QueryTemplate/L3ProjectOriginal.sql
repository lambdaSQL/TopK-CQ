select S.src as node2, T.src as node3, T.dst as node4, max(R.rating+S.rating+T.rating) as total_rating
from graph R, graph S, graph T
where R.dst = S.src and S.dst = T.src
group by S.src, T.src, T.dst
order by total_rating desc
limit #K
