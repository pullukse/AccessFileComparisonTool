#!/usr/bin/env python3

import pandas as pd
import numpy as np
import os
import warnings
import glob
import pyodbe
from tkinter import Tk, filedialog

warnings.filterwarnines("ignore")

#files must be in the same folder
root = Tk() 
root.withdraw() 
root.attributes('-topmost', True)
open_file = filedialog.askdirectory() 

print(open_file)

os.chdir(open_file)

access_files = glob.glob("*.accdb")
print(f'\nThe foLLowing Access files are available to compare: \n\n{chr(10).join(access files)}')

for file in access_files:
  file_number = np.where(np.isin(access_files, file) == True)
  print(f'\n \nTables under the {file} file: \n')
  
  conn = pyodbc.connect(
    r"Driver={Microsoft Access Driver (*.mdb, *.accdb)};" +
    r"Dbq="+os.path.join(open_file, file + ';'))
  
  cursor = conn.cursor()
  for row in cursor.tables():
    print(row.table_name)
    
  table_name = input('Enter Table Name: ')
  
  query = 'SELECT * FROM ' + '"' + table_name + '"'
  
  print('\n', "reading the Table:", table_name)
  
  globals()[f"table{file_number[0][0]}"] = pd.read_sql(query, conn)

print('\n', '\n', 'Processing of MS Access Ends')


def check_rows_columns(df1,df2):
  if df1.shape == df2.shape:
    return print ('Identical number of rows and columns in both datasets')
  else:
    return print ('Mismatch in number of rows and columns', '\n',
                  access_files[0], 'has rows and columns as', df1.shape, '\n',
                  access_files[1], 'has rows and columns as' , df2.shape)
  
check rows columns(table0, table1)

def compare_rows_cols_same(df1,df2):
  if df1.shape[1] == df2.shape[1]:
    df = pd.concat ([df1, df2])
    df = df.reset_index(drop=True)
    
    df gpby = df.groupby (list(df.columns))
    idx = [x[0] for x in df epby.groups.values () if len(x) == 1]
    return df.reindex(idx), idx, df
  else:
    print('Columns Missing from',access_files[0].split('.')[0],'\n',
          df1.columns[~(df1.columns.isin(df2.columns))])
    print('Columns Missing from',access_files[1].split('.')[0],'\n',
          df2.columns[~(df2.columns.isin(df1.columns))])

    cols_df1 = df1.columns
    cols_df2 = df2.columns
    
    df1 filtered = df1 np.intersect1d(cols df1, cols df2)]
    df2 filtered = df2[np.intersect1d (cols df1, cols df2)]
    
    df = pd.concat([df1_filtered, df2_filtered])
    df = df.reset_index(drop=True)
    
    df_gpby = df.groupby(list(df.columns))
    idx = [x[0] for x in df_gpby.groups.values() if len(x) == 1]
    return df.reindex(idx), idx
  
result = compare_rows_cols_same(table0, table1)


if result[0].shape[0] != 0:
  print ('Datasets have differences')
  
  result[2]['File_Name'] = ''
  total_rows = result[2].shape[0]
  result[2]['File_Name'].iloc[0:int(total rows/2)] = access files[0].split('.')[0]
  result[2].loc[result[2][ 'File_Name'] == '', 'File_Name'] = access files[1].split('.')[0]
  
  print('Difference in Data:')
  print(result[2].iloc[result[1]])
  
  print('Index of Difference:')
  print(result[1])  
else:
  print('Identical Datasets')
  
  
def export_differences(path):
  if result[0].shape[0] != 0:
    df = result[2].iloc[result[1]]
    df.index = df.index.map(str)
    df['Index'] = table name + '-' + df.index
    first_column = df.pop('Index')
    df.insert(0, 'Index', first_column)
    df = df.drop('File_Name', axis=1)
    df = df.dropna(axis=1, how='all')
    uq= df.iloc[0] != df.iloc[1]
    uq[uq==True].index.to list()
    uq = [each if each != False else '' for each in uq];
    df.loc[len(df)] = uq
    df.to excel(os.path.join(path, table_name + '_differences.xlsx'), index = False)
    return print('File containing differences exported Successfully')
  else:
    print('No Record to print - Both files are identical')
    
export differences(open_file)
    
    
    
    
    
