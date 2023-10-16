import sql_file
from string import Template

def get_with_max_sql(id, dictionary):
    table_with_template = """with ${table}_${id}_max as (
select $src, max(${accweight}) as max_accweight
from ${tablewithid}
group by $src
)"""
    is_last_table = dictionary["num_tables"] == id+1
    d = dictionary.copy()
    d["id"] = id
    d["accweight"] = d["weight"] if is_last_table else "accweight"
    d["tablewithid"] = d["table"] 
    if not is_last_table:
        d["tablewithid"] += "_%d" % id
    s = Template(table_with_template)
    return s.substitute(d)
    

def compute_acc_sql(id, dictionary):
    query_template = """drop view if exists ${table}_${id};
create view ${table}_${id} as (
$withnext
select ${table}.$src as $src, ${table}.$dst as $dst, 
    ${table}.$weight as $weight, ${table}.$weight+${table}_${idnext}_max.max_accweight as accweight
from ${table} join ${table}_${idnext}_max on ${table}.$dst = ${table}_${idnext}_max.$src
${orderby});
"""
    d = dictionary.copy()
    d["id"] = id
    d["idnext"] = id+1
    d["withnext"] = get_with_max_sql(id+1, d)
    d["orderby"] = "order by accweight desc limit %d\n" % dictionary["K"] if id==0 else ""
    s = Template(query_template)
    return s.substitute(d)

def generate(dictionary):
    sql = ""
    num_tables = dictionary["num_tables"]
    K = dictionary["K"]
    for i in range(num_tables-2, -1, -1):
        sql += compute_acc_sql(i, dictionary)
    sql_file.write("bottom_up.sql", sql)


# for test
if __name__ == "__main__":
    d = {}
    d["table"] = "graph"
    d["src"] = "src"
    d["dst"] = "dst"
    d["weight"] = "rating"
    d["K"] = 1024
    d["num_tables"] = 3
    generate(d)
