import pickle
import sqlalchemy
import pandas as pd
from pandas import DataFrame

def write():
    j = 1
    with open('variables.pickle', 'rb') as f:
         sql, output, output_path, output_filename, input_path = pickle.load(f)
         f.close()
    output_path_dbcsv = output_path + '.csv'
    database_path = 'sqlite:///' + output_path
    csv_database = sqlalchemy.create_engine(database_path)

    #for each chunk in the CSV using chunk size from earlier
    for chunk in pd.read_csv(output_path_dbcsv, chunksize=1, iterator=True):
        #Read in what byte to start at
        chunk.index += j
        #Append chunk to table, creates if not exist
        chunk.to_sql('master_table', csv_database, if_exists='append')
        #Increments bytes read
        j = chunk.index[-1] + 1
