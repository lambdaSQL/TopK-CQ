from string import Template
import sql_file
import math

def prepare_dict(id, dictionary):
    d = dictionary.copy()
    num_tables = d["num_tables"]
    d["lefttable"] = d["table"] + "_0" if id==0 else "%s_acc_%d" % (d["table"], id)
    d["righttable"] = d["table"] if id==num_tables-2 else "%s_%d" % (d["table"], id+1)
    d["leftid"] = id
    d["rightid"] = id+1
    d["accweight"] = d["weight"] if id==num_tables-2 else "accweight"
    src_i = Template("$lefttable.${src}${srcid} as ${src}${srcid}")
    def rewrite(i):
        d["srcid"] = "" if i==0 else i
        return src_i.substitute(d)
    d["sources"] = ", ".join(map(rewrite, range(id+1)))
    d["withtruncate"] = truncate_right_table(d)
    return d

def truncate_right_table(dictionary):
    query_template = """with ${lefttable}_max as (
        select $dst, max($weight) as max_weight
        from ${lefttable}
        group by $dst
    ), ${table}_truncated_${rightid} as (
        select ${righttable}.$src as $src, ${righttable}.$dst as $dst, 
        ${righttable}.$weight as $weight, ${righttable}.$accweight as accweight
        from ${lefttable}_max join ${righttable} on ${lefttable}_max.$dst = ${righttable}.$src
        order by ${lefttable}_max.max_weight+${righttable}.$accweight desc limit $K
    )"""
    return Template(query_template).substitute(dictionary)

def productK(dictionary):
    query_template = """drop view if exists ${table}_acc_${rightid};
create view ${table}_acc_${rightid} as (
    $withtruncate
    select $sources, ${table}_truncated_${rightid}.$src as ${src}${rightid}, ${table}_truncated_${rightid}.$dst as $dst, ${lefttable}.$weight+${table}_truncated_${rightid}.$weight as $weight
    from ${lefttable} join ${table}_truncated_${rightid} on ${lefttable}.$dst=${table}_truncated_${rightid}.$src
    order by ${lefttable}.$weight+${table}_truncated_${rightid}.$accweight desc limit $K
);
"""
    return Template(query_template).substitute(dictionary)

def onelevel(dictionary):
    levelid = dictionary["levelid"]
    base = dictionary["base"]
    left_bound = base ** levelid
    right_bound = base * left_bound
    query_template = """,
levelk_left_${levelid} as (select $src, joinkey, left_weight as $weight from levelk_join_%d where rnk=%d),
levelk_right_${levelid} as (select * from ${table}_${rightid}_rnk where rnk>%d and rnk<=%d),
levelk_join_${levelid} as (select levelk_left_${levelid}.$src as $src, levelk_right_${levelid}.joinkey as joinkey, levelk_right_${levelid}.$dst as $dst, levelk_right_${levelid}.rnk as rnk,
    levelk_left_${levelid}.$weight as left_weight, levelk_left_${levelid}.$weight+levelk_right_${levelid}.$weight as $weight, levelk_left_${levelid}.$weight+levelk_right_${levelid}.accweight as accweight
    from levelk_left_${levelid} join levelk_right_${levelid} on levelk_left_${levelid}.joinkey=levelk_right_${levelid}.joinkey 
    union all select * from levelk_join_%d
    order by accweight desc limit $K)""" % (levelid-1, left_bound, left_bound, right_bound, levelid-1)
    return Template(query_template).substitute(dictionary)

def levelK(dictionary):
    base = dictionary["base"]
    k = dictionary["K"]
    num_levels = math.ceil(math.log(k, base))
    query_template = """drop table if exists ${table}_${rightid}_rnk;
create table ${table}_${rightid}_rnk as (
    $withtruncate
    select $src as joinkey, $dst, $weight, accweight, row_number() over(partition by $src order by accweight desc) as rnk
    from ${table}_truncated_${rightid}
);
create index rnk_index_${rightid} on ${table}_${rightid}_rnk (rnk);
drop view if exists ${table}_acc_${rightid};
create view ${table}_acc_${rightid} as (
with levelk_right_0 as (select * from ${table}_${rightid}_rnk where rnk<=$base),
levelk_join_0 as (select ${lefttable}.$src as $src, levelk_right_0.joinkey as joinkey, levelk_right_0.$dst as $dst, levelk_right_0.rnk as rnk,
    ${lefttable}.$weight as left_weight, ${lefttable}.$weight+levelk_right_0.$weight as $weight, ${lefttable}.$weight+levelk_right_0.accweight as accweight
    from ${lefttable} join levelk_right_0 on ${lefttable}.$dst=levelk_right_0.joinkey
    order by accweight desc limit $K)"""
    sql = Template(query_template).substitute(dictionary)
    d = dictionary.copy()
    for i in range(1, num_levels):
        d["levelid"] = i
        sql += onelevel(d)
    query_template = """
    select ${src}, $dst, $weight
    from levelk_join_${levelid}
);
"""    
    sql += Template(query_template).substitute(d)
    return sql

def generate(dictionary, func_name):
    query_template = "select sum($src), sum($dst), sum($weight) from ${table}_acc_${rightid};"
    s = Template(query_template)
    sql = ""
    num_tables = dictionary["num_tables"]
    func = productK if func_name == "productK" else levelK
    for i in range(num_tables-1):
        d = prepare_dict(i, dictionary)
        sql += func(d)
    sql += s.substitute(d)
    sql_file.write(func_name+".sql", sql)
        

# for test
if __name__ == "__main__":
    d = {}
    d["table"] = "test"
    d["src"] = "src"
    d["dst"] = "dst"
    d["weight"] = "rating"
    d["K"] = 1024
    d["num_tables"] = 3
    d["base"] = 3
    generate(d, "levelK")
