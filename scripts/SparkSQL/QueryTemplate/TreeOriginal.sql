select R.src as node1, S.src as node2, S.dst as node3, T.dst as node4, U.dst as node5, R.rating+S.rating+T.rating+U.rating as total_rating
from graph R, graph S, graph T, graph U
where R.dst = S.src and R.src = T.src and R.src = U.src
order by total_rating desc
limit #K
