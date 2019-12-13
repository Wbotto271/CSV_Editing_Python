import os
import os.path
import pickle

def update_pickle(sql, output, output_path, output_filename, input_path):
    print(sql, output, output_path, output_filename, input_path)

    #create/open variables.pickleand write to it in bytes
    with open('variables.pickle', 'wb') as f:
        #store the specified variables in 'variables.pickle'
        pickle.dump([sql, output, output_path, output_filename, input_path], f)
        f.close()
        #close the file

#verifies all command arguments are valid
def verify(sql, output, output_file, input_path):
    #check of output_file ends with '.db'
    db_test = output_file.endswith('.db')
    #check of output_file ends with '.csv'
    csv_test = output_file.endswith('.csv')
    if sql == True and db_test != True:
        #remove the incorrect file extension from the specified document
        output_filename, file_extension = os.path.splitext(output_file)
        #add the correct file type
        output_filename = output_filename + '.db'
        #print(output_filename)
        print("Renaming", output_file, "to", output_filename, "because it ended in... something other than it should have")
        output_path = output +'\\'+ output_filename
        clean_csv = output_path + '.csv'
        #call the function 'update_pickle'
        update_pickle(sql, output, output_path, output_filename, input_path)
        
    elif sql == False and csv_test != True:
        #remove the incorrect file extension from the specified document
        output_filename, file_extension = os.path.splitext(output_file)
        #add the correct file type
        output_filename = output_filename + '.csv'
        print("Renaming", output_file, "to", output_filename, "because it ended in... something other than it should have")
        output_path = output +'\\'+ output_filename
        #call the function 'update_pickle'
        clean_csv =  'N/A'
        update_pickle(sql, output, output_path, output_filename, input_path)
        
    elif (sql == True and db_test == True) or (sql == False and csv_test == True):
        #check if the file path given exists
        if os.path.exists(output) == True:
            output_filename = output_file
            #join_path(output, output_filename)
            output_path = output +'\\'+ output_filename
            #call the function 'update_pickle'
            clean_csv =  'N/A'
            update_pickle(sql, output, output_path, output_filename, input_path)
            
        else:
            #updates 'kill.pickle so the program can be exited
            print("File path for output file does not exist")
            print("Please try again")
            kill_switch = True
            with open('kill.pickle', 'wb') as f:
                pickle.dump([kill_switch], f)        
    else:
        #updates 'kill.pickle so the program can be exited
        print("unknown exception in outputfilename")
        kill_switch = True
        with open('kill.pickle', 'wb') as f:
            pickle.dump([kill_switch], f)
                
def splitpath():
    #splits output filename from its directory path so its existance can be verified
    with open('variables.pickle', 'rb') as f:
        sql, output_file, input_path = pickle.load(f)
        f.close()
    if output_file == input_path:
        #updates 'kill.pickle so the program can be exited
        print("input and output file paths cannot be the same")
        kill_switch = True
        with open('kill.pickle', 'wb') as f:
            pickle.dump([kill_switch], f)
    #check if the input file exists
    elif os.path.isfile(input_path) == True:
        #if so do the below commands
        output = output_file.split('\\')
        output_file = output[-1]
        del output[-1]
        output = "\\".join(output)
        verify(sql, output, output_file, input_path)
    #if not, updates 'kill.pickle so the program can be exited
    else:
        print("The input file path you have specified:", input_path, "does not exist")
        print("Please try again")
        kill_switch = True
        with open('kill.pickle', 'wb') as f:
            pickle.dump([kill_switch], f)
                               

    
