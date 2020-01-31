# -*- coding: utf-8 -*-
"""
Created on Thu Oct 17 17:02:37 2019

@author: jboyce

NOTES ON GOAL OF SCRIPT:
    + Grab loan list for trendix values
    + Use SQL query to create Trendix csv
    + export Trendix csv to \\pnmac.com\cfs\Finance\Trendix\LoanPerformanceIn
    + pause x seconds
    + import Trendix output from \\pnmac.com\cfs\Finance\Trendix\LoanPerformanceOut
    
USER REQUIREMENTS:
    + User must update loan number list in the 1) property value query and 2) Trendix query  
    
    
REVISION HISTORY:
    20191213 to 20191213
    20191206:
        + Discovered issue with df_final and df_trendix_out not merging properly
        + Trendix df had all NaN values
        + Identified the root cause as df indexes with different data types (prevented matching)
        + Had to test dtypes of indexes, value columns. Trendix df had values but all columns imported as NaN when merging on left and right index
        + That's when focus shifted to index data type (after countless websearches)
"""

import pandas as pd
import numpy as np
import pyodbc
import time

#clock starts to time how long the df import takes
start_tm = time.clock()

#############################################
#SETTINGS - SQL CONNECTIONS, SQL QUERY FILENAMES AND SQL PATHS
#############################################
#set file location of sql query we want to import
sql_filename ='PropertyValues_Leadgen_waterfall.sql' 
sql_filename_trendix ='TrendixSQLCompleted.sql' 
sql_path = r'M:\Capital Markets\GNMA EBO Project\SQL Queries'
sql_fileandpath = sql_path+"\\"+sql_filename
sql_fileandpath_trendix = sql_path+"\\"+sql_filename_trendix

#SQL connection configuration settings; connection == the connection to your database
sql_conn = pyodbc.connect('DRIVER={SQL Server};SERVER={w08-vm-sql-3};DATABASE=portfolio_strategy;UID=jboyce;Trusted_Connection=yes')

#############################################
#SETTINGS - Set Trendix file paths (for csv export)
#############################################
#Trendix In folder
trendix_path_in = r'\\pnmac.com\cfs\Finance\Trendix\LoanPerformanceIn'
trendix_filename_in = '\waterfall_trendix_in.csv'
trendix_fileandpath_in = trendix_path_in + trendix_filename_in

#Trendix Out folder
trendix_path_out = r'\\pnmac.com\cfs\Finance\Trendix\LoanPerformanceOut'
trendix_filename_out = '\waterfall_trendix_in_output.csv'
trendix_fileandpath_out = trendix_path_out + trendix_filename_out

#Trendix Export to project folder
trendix_export_path = r'M:\Capital Markets\GNMA EBO Project\20191231 cutoff\Python Files'
trendix_export_filename = '\waterfall_trendix_export.csv' 
trendix_export_fileandpath = trendix_export_path + trendix_export_filename


#############################################
#SETTINGS - Set the export filename and location for property value dataframes
#############################################
#df export info (BEFORE df updates)
df_export_path = r'M:\Capital Markets\GNMA EBO Project\20191231 cutoff\Python Files'
df_export_filename = '\df_waterfall_orig.xlsx' 
df_export_fileandpath = df_export_path + df_export_filename

#df export info (AFTER df updates)
df_final_export_path = r'M:\Capital Markets\GNMA EBO Project\20191231 cutoff\Python Files'
df_final_export_filename = '\df_waterfall_final.xlsx' 
df_final_export_fileandpath = df_final_export_path + df_final_export_filename

#############################################
#set the SQL connection, query path, and query file name
#############################################


#############################################
#RUN THE SQL QUERIES AND CREATE DATAFRAMES
#############################################
#Read the SQL query results into a pandas dataframe
#define the opening of the filename and path of the sql file
query = open(sql_fileandpath)
query_trendix = open(sql_fileandpath_trendix)

#Read the sql file results into dataframes
#Make sure any loan lists in the query are updated prior to running
df = pd.read_sql_query(query.read(),sql_conn).set_index('LoanNumber') #, chunksize=1000,index_col='LoanNumber') #.set_index('LoanNumber')) #ADDING CHUNKSIZE TEST HERE!!!! #can add chunksize for df
#, chunksize=1000
#^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

#NOTE ON DF INDEX BELOW
#changing df index dtype to int not needed...keeping it as object to match trendix index dtype...too many issues with int for loannumber

df_trendix = pd.read_sql_query(query_trendix.read(),sql_conn).set_index('LOAN_NO')
sql_conn.close()

#------------------------------------------------------------------------------------------------------------
#Export the Trendix csv template to the Trendix In folder
df_trendix.to_csv(trendix_fileandpath_in) #index=False

#print some statistics do console for user color
print('\n')
print('Waterfall shape (rows):',df.shape[0])
print('Trendix shape (rows):',df_trendix.shape[0])
print('\n')
print('Trendix csv exported to: \n',trendix_path_in, '\n')

#countdown while waiting for Trendix output...could probably refactor script to watch for output file posting to folder 
for i in range(5):
    print(str(5-i))
    time.sleep(1)    

print('\n')
print('Grabbing Trendix Output File \n')

time.sleep(1)

df_trendix_out = pd.read_csv(trendix_fileandpath_out).set_index('LOAN_NO')

#change index to object / string dtype (to match df_final index dtype)
df_trendix_out.index = df_trendix_out.index.astype(np.str) #12/13/2019 THIS INDEX DTYPE CHANGE TO OBJECT OR STRING APPEARS TO WORK


print('Trendix Output shape (rows):',df_trendix_out.shape[0], '\n')
print("Trendix Output (first 5 rows):\n",df_trendix_out.head())
print("\n", "CUR_HOUSE_PRICE datatype is:",df_trendix_out.CUR_HOUSE_PRICE.dtypes)
print("Trendix Index datatype is:",df_trendix_out.index.dtype) #or can do type(df_trendix_out.index)
print("DF Index datatype is:",df.index.dtype) #or can do type(df.index)
print("\n")
print("CUR_HOUSE_PRICE in Trendix DF:\n",df_trendix_out['CUR_HOUSE_PRICE'].head(), '\n')


#------------------------------------------------------------------------------------------------------------
#Export df prior to updating Trendix value
df.to_excel(df_export_fileandpath) #,index=False


df_final = df

#add the CUR_HOUSE_PRICE column to df_final
df_final['CUR_HOUSE_PRICE'] = df_trendix_out['CUR_HOUSE_PRICE']

#update FINAL_WATERFALL_VALUE with CUR_HOUSE_PRICE
df_final.loc[df_final['FINAL_WATERFALL_APPROACH'] == 'Pull Trendix', 'FINAL_WATERFALL_VALUE'] = df_final.CUR_HOUSE_PRICE

#MAYBE ADD TO STEP THAT ADDS THE TRENDIX CSV TO THE FINAL OUTPUT

print('\n', 'Trendix Output df (five rows): \n', df_trendix_out.head()) #also printed out above...consider labeling the printout
print('\n', 'df_final (five rows): \n', df_final.head())
print('\n', 'df_final property values (five rows): \n', df_final[['FINAL_WATERFALL_VALUE','CUR_HOUSE_PRICE']].head())

#------------------------------------------------------------------------------------------------------------
#Export df after updating Trendix value
df_final.to_excel(df_final_export_fileandpath) #,index=False
df_trendix_out.to_csv(trendix_export_fileandpath) #,index=False

print("\n")
print("Import Time (Using Time): " + str(round((time.clock()-start_tm),2)) +  " seconds...\n")
print('\nSee the following path for xlsx versions of the dataframes: \n \n')
print(df_final_export_path, '\n \nAll Done!\n')
print("Process time (Using Process Time): " + str(time.process_time) + " seconds...\n")


#------------------------------------------------------------------------------------------------------------
#junk leftover code

#set df index to int64 or won't merge with Trendix (index datatypes have to match) trying column dtype change above
#df.LoanNumber.astype('Int64')
#df.index.astype('Int64')
#df['LoanNumber'].astype(np.int64) #this works. dtyp is now int64
#print('df v1 (after astype to to int64) \n', df.head())
#print("DF Index datatype is:",df.index.dtype, '\n') #or can do type(df.index)
#set LoanNumber to index (since we changed data type to int64)
#df.set_index('LoanNumber', inplace=True, drop=True) #producing error
#print('df v2 (after set_index inplace) \n', df.head(), '\n')


#stackoverflow example below
#df.loc[df.Code=='Dummy', 'Code'] = df.merge(df2, on='Market', how='left')['Code(New)']
#another stackoverflow solution below

#print out the run time, df shape, and df first five rows
#print("\n")
#print("Import Time (Using Time): " +str(time.clock()-start_tm))
#print("Import Time (Using process_time): " +str(time.process_time()))
#print("Import Time  (Using perf_counter): " +str(time.perf_counter()))
#print("\n")
#print("Top 5 rows\n" + str(df.head()) +"\n")
#print("\n Number of rows: {}, Number of columns: {}".format(df.shape[0], df.shape[1]))

#example from ibm class
#df_del_na.loc[df_del_na['Neighbourhood'] == "Not assigned", 'Neighbourhood'] = "Queen's Park"
#df_del_na