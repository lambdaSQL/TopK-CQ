from string import Template
import sql_file


def generate(dictionary):
    query_template = """with query_result as (
    select $firsttable.$src as $src, $lasttable.$dst as $dst, $sumweight as $weight
    from $joins 
    order by $weight desc
    limit $K
)
select sum($src), sum($dst), sum($weight) from query_result;
"""
    s = Template(query_template)
    sql = ""
    num_tables = dictionary["num_tables"]
    d = dictionary.copy()
    table_names = ["%s_%d"%(d["table"],i) for i in range(num_tables)]
    d["sumweight"] = "+".join("%s.%s" % (i, d["weight"]) for i in table_names)
    d["lasttable"] = table_names[-1]
    d["firsttable"] = table_names[0]
    join_template = "$table as %s" %  table_names[0]
    for i in range(1, num_tables):
        join_template += "\njoin $table as %s on %s.$dst = %s.$src" % (table_names[i], table_names[i-1], table_names[i])
    d["joins"] = Template(join_template).substitute(d)
    sql += s.substitute(d)
    sql_file.write("non_optimized.sql", sql)
        

# for test
if __name__ == "__main__":
    d = {}
    d["table"] = "test"
    d["src"] = "src"
    d["dst"] = "dst"
    d["weight"] = "rating"
    d["K"] = 1024
    d["num_tables"] = 3
    generate(d)