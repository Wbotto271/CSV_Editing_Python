from __future__ import print_function
import pickle
import csv
import re
import os
import os.path
from variables_V2 import *
import pandas as pd
from pandas import DataFrame

import functools
import random
import sys
import time

import progressbar

write_count = 0
                    
#makes 'final_df'

def animated_marker():
    bar = progressbar.ProgressBar(
    widgets=['Working: ', progressbar.AnimatedMarker()])
    for i in bar((i for i in range(5))):
        time.sleep(0.01)

def make_final_df(fileNameList, clean_df, sql, output, output_path, output_filename, input_path):
    global write_count
    #with open('variables.pickle', 'rb') as f:
        #sql, output, output_path, output_filename, input_path = pickle.load(f)
        #f.close()
    final_df = pd.DataFrame()
    filename_df = pd.DataFrame()
    filename_df = pd.DataFrame(fileNameList)
    filename_df.transpose()
    filename_df.columns = ['filename']
    final_df = clean_df.assign(filename=filename_df['filename'].values)
    #checks if the '--SQL' arguement was used and writes final_df to the appropriate location
    if sql == False:
        #create and append the specified output file
        with open(output_path, 'a', newline = '') as csvfile:
            if(write_count == 0):
                write_count = write_count + 1
                final_df.to_csv(output_path, index = False)                
            else:
                final_df.to_csv(output_path, mode = 'a', index = False, header = False)
    #if '--SQL' was used,call the function 'write_database' and pass 'final_df'
    elif sql == True:
        output_path_dbcsv = output_path + ".csv"
        with open(output_path_dbcsv, 'a', newline = '') as csvfile:
            if(write_count == 0):
                write_count = write_count + 1
                final_df.to_csv(output_path_dbcsv, index = False)                
            else:
                final_df.to_csv(output_path_dbcsv, mode = 'a', index = False, header = False)
    else:
        print("error")
                
    final_df.drop(columns=column_list_appended)
    filename_df.drop(columns='filename')
#seperates file names from file path cell    
def filename_seperator(clean_df, sql, output, output_path, output_filename, input_path):
    filename = ""
    filePathList = []
    filePathList = []
    fileNameList = []
    splitPath = []
    filePathList = clean_df['filepath'].astype(str).values.tolist()
    #for every value in the list, split it, and append the last entry to a new list
    for i in filePathList:
        filepath = i
        splitPath = filepath.split('/')
        filename = splitPath[-1]
        fileNameList.append(filename)
        #call the function that makes 'final_df' and pass necessecary variables
        make_final_df(fileNameList, clean_df, sql, output, output_path, output_filename, input_path)

    filePathList.clear()
    fileNameList.clear()
    splitPath.clear()
    del filename
#function to create 'clean_df'. this is equal to 'master_df minus' thumbnail data  
def make_clean_df(lighterExtra, master_df, sql, output, output_path, output_filename, input_path):
    clean_df = pd.DataFrame()
    extra_df = pd.DataFrame()
    extra_df = pd.DataFrame(lighterExtra)
    extra_df.transpose()
    extra_df.columns = ['extra']
    #create 'clean_df' and assign it the values of master_df and extra_df
    clean_df = master_df.assign(extra=extra_df['extra'].values)
    filename_seperator(clean_df, sql, output, output_path, output_filename, input_path)

    clean_df.drop(columns=column_list_altered)
    extra_df.drop(columns='extra')

def thumbnail_remover(master_df, sql, output, output_path, output_filename, input_path):
    fullExtraList = []
    lighterExtra = []
    splitExtra = []
    master_df.dropna()
    
    fullExtraList = master_df['extra'].astype(str).values.tolist()
    master_df.drop(columns = 'extra')
    for i in fullExtraList:
        #do the below commands if the cell does not return 'NaN' (not a number)
        if i !='nan':
            extra = i
            #check to see if the 'extra' column of the csv contains thumbnail data
            extra_test = '; thumbnail: ' in extra
            #empty_test = '' in extra
            #if the cell contains data, do the do the following
            if (extra_test == True):
                #split the string at '; thumbnail: '
                splitExtra = extra.split('; thumbnail: ')
                #add the first string to lighter.extra... thumbnail data is always at the end
                lighterExtra.append(splitExtra[0])
                splitExtra.clear()
                #call the function 'male_clean_df'
                make_clean_df(lighterExtra, master_df, sql, output, output_path, output_filename, input_path)
            #if the string does not contain thumbnail data, append it to lighterExtra
            elif (extra_test == False):
                lighterExtra.append(extra)
                #call the function 'make_clean_df'
                make_clean_df(lighterExtra, master_df, sql, output, output_path, output_filename, input_path)
            #handling unexpected issues. file output is weird and this is prettyer than a debug diag
            else:
                print("unknown Issue parsing Extra column")
                kill_switch = True
                #updayes 'kill.pickles' sp exit the code
                with open('kill.pickle', 'wb') as f:
                    pickle.dump([kill_switch], f)
        #if the input is not valid or detected as a specific invalid type, skip it
        else:
            return()
        #clear variables and lists for reuse. lighten memory usage
        del extra
        lighterExtra.clear()
        fullExtraList.clear()
def date_cleaner(master_df, sql, output, output_path, output_filename, input_path):
    fullDateList = []
    fullDateList = master_df['date'].astype(str).values.tolist()
    regex = re.compile('[@!#$%^&*()<>?\|}{~;":_-]')
    for i in fullDateList:
        date = i
        #print(regex.search(date))
        if (regex.search(date) == None):
            fullDateList.clear()
            thumbnail_remover(master_df, sql, output, output_path, output_filename, input_path)
        elif (regex.search(date) != None):
            fullDateList.clear()
            #del fullExtraList[:]
            return()           
        else:
            print("unknown Issue parsing Extra column")
            kill_switch = True
            with open('kill.pickle', 'wb') as f:
                pickle.dump([kill_switch], f)
        del date

def read():
    with open('variables.pickle', 'rb') as f:
        sql, output, output_path, output_filename, input_path = pickle.load(f)
        f.close()
    row_count = sum(1 for row in csv.reader( open(input_path) ) )
    with open(input_path) as PF:
        chunk_iter = pd.read_csv(PF, usecols=column_list, chunksize = 1)
        for chunk in chunk_iter:
            animated_marker()
            master_df = pd.DataFrame()
            master_df = pd.concat([master_df,chunk])
            master_df.rename(columns=({'filename':'filepath'}), inplace=True)
            date_cleaner(master_df, sql, output, output_path, output_filename, input_path)
            master_df.drop(columns=column_list_altered)

