import argparse
import pickle
import os
import os.path
def parse():
    parser = argparse.ArgumentParser(description='set variables for csv parsing')
    parser.add_argument('--SQL', action="store_true", default=False, required=False,
    help="Output to SQL database, default is CSV")
    parser.add_argument('--o', action="store", dest="output_file", required=True,
    type=str, help="Specify output file path")
    parser.add_argument('--i', action="store", dest="input_path", required=True,
    type=str, help="Specify input file path")
    args = parser.parse_args()
    print(args)
    output_file = args.output_file
    input_path = args.input_path
    sql = args.SQL
    with open('variables.pickle', 'wb') as f:
        pickle.dump([sql, output_file, input_path], f)

parse()
    


    


