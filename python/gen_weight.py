from string import Template
import sql_file

def generate(dictionary):
    query_template = """drop table $table if exists;
create table $table as (
select $src, $dst, mod(172475*$src+8203*$dst+308407,400009) as $weight 
from $original 
);"""
    s = Template(query_template)
    sql = s.substitute(dictionary)
    sql_file.write("gen_weight.sql", sql)

# for test
if __name__ == "__main__":
    d = {}
    d["table"] = "bitcoin"
    d["original"] = "bitcoin_origin"
    d["src"] = "src"
    d["dst"] = "dst"
    d["weight"] = "rating"
    generate(d)