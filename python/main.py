import bottom_up, line_join, non_optimized, argparse

parser = argparse.ArgumentParser(
                    prog='topk_sqlgen',
                    description='A python script to generate top-k sql for line join')
parser.add_argument('-K', '--K', type=int, default=1024) 
parser.add_argument('-s', '--src', default="src")     
parser.add_argument('-d', '--dst', default='dst')  
parser.add_argument('-w', '--weight', default='rating')  
parser.add_argument('-t', '--table', default='bitcoin') 
parser.add_argument('-n', '--num_tables', type=int, default=3) 
parser.add_argument('-b', '--base', type=int, default=2) 

if __name__ == "__main__":
    d = vars(parser.parse_args())
    non_optimized.generate(d)
    bottom_up.generate(d)
    line_join.generate(d, "productK")
    line_join.generate(d, "levelK")
