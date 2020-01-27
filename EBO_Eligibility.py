# -*- coding: utf-8 -*-
"""
Created on Thu Oct 17 17:02:37 2019

@author: jboyce

NOTES ON GOAL OF SCRIPT:
    + Run EBO eligibility for ongoing intra-month tracking 
    + Use SQL query to create loan list of eligible EBOs for the current month
    + export list (xlsx or csv format) to \\pnmac.com\cfs\Finance\Trendix\LoanPerformanceIn
    + pause x seconds
    + import Trendix output from \\pnmac.com\cfs\Finance\Trendix\LoanPerformanceOut
    
USER REQUIREMENTS:
    + User must update loan number list in the 1) property value query and 2) Trendix query  
    
    
REVISION HISTORY:
    20200123:
        + Updated eligiblity script to integrate VA Refi and NA Housing queries
"""

import pandas as pd
import numpy as np
import pyodbc
import time

#clock starts to time how long the df import takes
#start_tm = time.clock()
result_time = time.localtime()
time_string = time.strftime("%Y%m%d", result_time)
t = time.process_time()

#############################################
#SETTINGS - SQL CONNECTIONS, SQL QUERY FILENAMES AND SQL PATHS
#############################################
#set file location of sql query we want to import
sql_filename ='EBO Sale Eligibility Scrub_Simultaneous_summers_v2_CalcCutoff.sql'  #CHANGE THIS AFTER FINALIZED
sql_path = r'M:\Capital Markets\GNMA EBO Project\SQL Queries' #CHANGE THIS AFTER FINALIZED
sql_fileandpath = sql_path+"\\"+sql_filename

#SQL connection configuration settings; connection == the connection to your database
sql_conn = pyodbc.connect('DRIVER={SQL Server};SERVER={w08-vm-sql-3};DATABASE=portfolio_strategy;UID=jboyce;Trusted_Connection=yes')

#############################################
#SETTINGS - Set file paths (for xlsx/csv export)
#############################################

#Set path, filename for export to folder
ebo_eligibility_export_path = r'M:\Capital Markets\GNMA EBO Project\Python'
ebo_eligibility_export_filename = '\ebo_eligibility.xlsx' 
ebo_eligibility_export_filename_withTimeStamp = '\ebo_eligibility_' + time_string +  '.xlsx' 
ebo_eligibility_export_fileandpath = ebo_eligibility_export_path + ebo_eligibility_export_filename
ebo_eligibility_export_filename_loannums = '\ebo_eligibility_list.csv' 
ebo_eligibility_export_fileandpath_loannums = ebo_eligibility_export_path + ebo_eligibility_export_filename_loannums
ebo_eligibility_export_fileandpath_withTimeStamp = ebo_eligibility_export_path + ebo_eligibility_export_filename_withTimeStamp

#############################################
#RUN THE SQL QUERIES AND CREATE DATAFRAMES
#############################################
#Read the SQL query results into a pandas dataframe
#define the opening of the filename and path of the sql file
query = open(sql_fileandpath)


#Read the sql file results into dataframes
#Make sure any loan lists in the query are updated prior to running
df = pd.read_sql_query(query.read(),sql_conn).set_index('LoanId')
sql_conn.close()

#------------------------------------------------------------------------------------------------------------
#Export the Trendix csv template to the Trendix In folder
#BE SURE TO CHANGE THIS TO EXPORT THE LOAN LIST AND THE ELIBILITY OUTPUT
#STILL NEED TO ADD REMOVALS OF LOANNUMBERS THAT ARE NOT ELIGIBLE
#DONE - STILL NEED TO ADD DATESTAMP TO EXPORT LIST

#^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

df_eligible = df.loc[df['EligibilityScrub'] == ''][['CurrentPrincipalBalanceAmt','EligibilityScrub']]

df.to_excel(ebo_eligibility_export_fileandpath) #index=False
df.to_excel(ebo_eligibility_export_fileandpath_withTimeStamp) #index=False, export one with date
df_eligible.to_csv(ebo_eligibility_export_fileandpath_loannums) #index=False #, columns=['CurrentPrincipalBalanceAmt','EligibilityScrub']

#result_time2 = time.localtime()
#time_diff = time.asctime(result_time2) - time.asctime(result_time)
#time_string2 = time.strftime("%H:%M:%S", time_diff)
elapsed_time = time.process_time() - t

print("Runtime was: ", elapsed_time, " seconds\n") #add seconds formatting on to-do list
print('DF Eligible (head):\n\n', df_eligible.head())

#print some statistics do console for user color
print('\n')
print('Eligible 60+ Count (rows):',df.shape[0])
print('Eligible not kicked count (rows):',df_eligible.shape[0])
print('Eligible population exported to: \n',ebo_eligibility_export_fileandpath, '\n', 'and\n', ebo_eligibility_export_fileandpath_withTimeStamp)



#MAYBE ADD TO STEP THAT ADDS THE TRENDIX CSV TO THE FINAL OUTPUT

#------------------------------------------------------------------------------------------------------------
#junk leftover code

#update FINAL_WATERFALL_VALUE with CUR_HOUSE_PRICE
#df_final.loc[df_final['FINAL_WATERFALL_APPROACH'] == 'Pull Trendix', 'FINAL_WATERFALL_VALUE'] = df_final.CUR_HOUSE_PRICE

#add the CUR_HOUSE_PRICE column to df_final
#df_final['CUR_HOUSE_PRICE'] = df_trendix_out['CUR_HOUSE_PRICE']

#print('\n', 'Trendix Output df (five rows): \n', df_trendix_out.head()) #also printed out above...consider labeling the printout
#print('\n', 'df_final (five rows): \n', df_final.head())
#print('\n', 'df_final property values (five rows): \n', df_final[['FINAL_WATERFALL_VALUE','CUR_HOUSE_PRICE']].head())

#Export df after updating Trendix value
#df_final.to_excel(df_final_export_fileandpath) #,index=False
#df_trendix_out.to_csv(trendix_export_fileandpath) #,index=False

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