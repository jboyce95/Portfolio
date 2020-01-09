# -*- coding: utf-8 -*-
"""
Created on Thu Oct 17 17:02:37 2019

@author: jboyce

NOTES ON GOAL OF SCRIPT:
    + create ebo investor tape to send to investors
    
USER REQUIREMENTS:
    + User must update loan number list in the 1) ebo investor tape query
    + Presently, this python script references the actual SQL query. Future refactoring, which aims to pull in loan number lists
        located on internal folders, will shift to passing the text string inside of the SQL script in order to automatically 
        insert the loan number list into script
    + Future refactoring can include the following:
        ++ Import loan numbers for population (rather than updating SQL scripts)
        ++ Create Summary statistics report based on the loan population
    
    
REVISION HISTORY:
    2020XXXX:
        + Discovered issue with df_final and df_trendix_out not merging properly
"""

import pandas as pd
import numpy as np
import pyodbc
import time

#clock starts to time how long the df import takes
#start_tm = time.clock()

def make_tape():
        
        #SETTINGS - SQL CONNECTIONS, FILENAMES AND PATHS
        #------------------------------------------------------------------------------------------------------------
        #------------------------------------------------------------------------------------------------------------
        #set file location of sql query we want to import
        sql_filename ='EBO Investor Tape.sql' 
        sql_path = r'M:\Capital Markets\GNMA EBO Project\SQL Queries'
        sql_fileandpath = sql_path+"\\"+sql_filename
        
        
        #------------------------------------------------------------------------------------------------------------
        #Set the export filename and location for property value dataframes
        df_export_path = r'M:\Capital Markets\GNMA EBO Project\SQL Queries\Tests'
        df_export_filename = '\df_ebo_investor_tape.xlsx' 
        df_export_fileandpath = df_export_path + df_export_filename
        
        
        #------------------------------------------------------------------------------------------------------------
        #set the SQL connection, query path, and query file name
        #SQL connection configuration settings; connection == the connection to your database
        sql_conn = pyodbc.connect('DRIVER={SQL Server};SERVER={w08-vm-sql-3};DATABASE=SMD;UID=jboyce;Trusted_Connection=yes')
        #------------------------------------------------------------------------------------------------------------
        #------------------------------------------------------------------------------------------------------------
        
        
        #RUN THE SQL QUERIES AND CREATE DATAFRAMES
        #------------------------------------------------------------------------------------------------------------
        #------------------------------------------------------------------------------------------------------------
        #Read the SQL query results into a pandas dataframe
        #define the opening of the filename and path of the sql file
        query = open(sql_fileandpath)
        
        #Make sure any loan lists in the query are updated prior to running
        df = pd.read_sql_query(query.read(),sql_conn).set_index('LoanNumber') #, chunksize=1000,index_col='LoanNumber') #.set_index('LoanNumber')) #ADDING CHUNKSIZE TEST HERE!!!! #can add chunksize for df
        sql_conn.close()
        
        #------------------------------------------------------------------------------------------------------------
        #Export the query results to the Trendix In folder
        df.to_excel(df_export_fileandpath) #,index=False
        
        #print some statistics to console for user color
        print('\n')
        print('EBO Population shape (rows):',df.shape[0])
        print('\n')
        print('EBO Investor Tape exported to: \n',df_export_fileandpath, '\n')
        
        
        print('\n', 'df_EBO_Investor_Tape (five rows): \n', df.head())
        
        #------------------------------------------------------------------------------------------------------------
        print('\nSee the following path for xlsx versions of the dataframes:\n')
        print(df_export_fileandpath)