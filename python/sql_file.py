import os
dirname = os.path.dirname(__file__)
query_path = os.path.join(dirname, 'query')

def write(filename, sql):
    file_path = os.path.join(query_path, filename)
    f = open(file_path, "w")
    f.write(sql)
    f.close()