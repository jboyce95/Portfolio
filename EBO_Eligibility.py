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
    + Note that 
    
REVISION HISTORY:
    20200311:
        + PENDING - Adding placeholder kick columns for pre-dd phase of marketing process
        + Note that this update there are errors thrown when there is only one loan in any kick file
    20200123:
        + Added to the dataframe the tracking of dd kicks, port strat kicks, pricing kicks from loan lists
            housed in the network drive
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
sql_path = r'M:\Capital Markets\GNMA EBO Project\SQL Queries' #CHANGE THIS AFTER FINALIZED
sql_filename ='EBO Sale Eligibility Scrub_Simultaneous.sql'  #CHANGE THIS AFTER FINALIZED
sql_fileandpath = sql_path+"\\"+sql_filename

#SQL connection configuration settings; connection == the connection to your database
sql_conn = pyodbc.connect('DRIVER={SQL Server};SERVER={w08-vm-sql-3};DATABASE=portfolio_strategy;UID=jboyce;Trusted_Connection=yes')

#############################################
#SETTINGS - Set file paths (for xlsx/csv export)
#CONSIDER CHANGING PATHS TO INDIVIDUAL FOLDER TYPES (E.G., ELIGIBILITY, PRICING, DD KICKS, ETC.)
#############################################
#
#EXPORTS - Set path, filename for exporting Eligibility Population Flags 
#EBO ELIGIBILITY
#------------------------------------------------------------------------------------------------------------
ebo_eligibility_export_path = r'M:\Capital Markets\GNMA EBO Project\Python'
ebo_eligibility_export_filename = '\ebo_eligibility.xlsx' 
#------------------------------------------------------------------------------------------------------------



#define filename that includes timestap (for archiving)
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
#print(df.shape[0])

#############################################
#ADD KICK COLUMNS TO DATAFRAME
#############################################
# df['FLAG_ALLOCATION'] = ''
# df['FLAG_DD_KICKS'] = ''
# df['FLAG_PCG_KICKS'] = ''
# df['FLAG_PRICIING_KICKS'] = ''
df['FLAG_PS_KICKS'] = ''

#Port Strat
#------------------------------------------------------------------------------------------------------------
ps_kicklist_import_path = r'M:\Capital Markets\GNMA EBO Project\Python\Import' ##### r'M:\Capital Markets\GNMA EBO Project\Python'
ps_kicklist_import_filename = '\PS_Kicks_20200401_settle.csv' 
ps_kicklist_import_pathandfile = ps_kicklist_import_path + ps_kicklist_import_filename
df_PS_kicks = pd.read_csv(ps_kicklist_import_pathandfile).set_index('LoanId')

#update flag column to make sure it's populated
df_PS_kicks['FLAG_PS_KICKS'] = 'PS_KICK'
#------------------------------------------------------------------------------------------------------------
"""
#??????????????????????????????????????????????????????????????????????????????????????
#TEMPORARY COMMENTING OUT FOR KICK LISTS AND ALLOCATION LISTS
#WORKING TO ADD SCRIPT THAT TRIES TO PULL FILES BUT GIVES EXCEPTION ERRORS IF DOESN'T EXIST OR FILES ARE EMPTY
#

#############################################
#IMPORTS (KICK LISTS AND ALLOCATIONS) - Set path, filename for importing Pricing Kick List
#   then import into df
#############################################
#Port Strat
#------------------------------------------------------------------------------------------------------------
ps_kicklist_import_path = r'M:\Capital Markets\GNMA EBO Project\20200131 cutoff\Servicing Investments' # PROBABLY CHANGE THIS TO CENTRALIZED FOLDER (BUT IT RISKS NOT BEING UPDATED...COULD ADD FILENAME CHANGER TO END OF SCRIPT) ##### r'M:\Capital Markets\GNMA EBO Project\Python'
ps_kicklist_import_filename = '\PS_Kicks.csv' 
ps_kicklist_import_pathandfile = ps_kicklist_import_path + ps_kicklist_import_filename
df_PS_kicks = pd.read_csv(ps_kicklist_import_pathandfile).set_index('LoanId')
#------------------------------------------------------------------------------------------------------------
#Allocation
#------------------------------------------------------------------------------------------------------------
allocation_import_path = r'M:\Capital Markets\GNMA EBO Project\20200131 cutoff\Servicing Investments' 
allocation_import_filename = '\Allocation_20200302.csv' 
allocation_import_pathandfile = allocation_import_path + allocation_import_filename
df_allocation = pd.read_csv(allocation_import_pathandfile).set_index('LoanId')
#------------------------------------------------------------------------------------------------------------
#Pricing
#------------------------------------------------------------------------------------------------------------
pricing_kicklist_import_path = r'M:\Capital Markets\GNMA EBO Project\20200131 cutoff\Servicing Investments' 
pricing_kicklist_import_filename = '\Pricing_Kicks.csv' 
pricing_kicklist_import_pathandfile = pricing_kicklist_import_path + pricing_kicklist_import_filename
df_pricing_kicks = pd.read_csv(pricing_kicklist_import_pathandfile).set_index('LoanId')
#------------------------------------------------------------------------------------------------------------
#DD Kicks
#------------------------------------------------------------------------------------------------------------
dd_kicklist_import_path = r'M:\Capital Markets\GNMA EBO Project\20200131 cutoff\DD' 
dd_kicklist_import_filename = '\DD_Kicks_20200302_settle.csv' 
dd_kicklist_import_pathandfile = dd_kicklist_import_path + dd_kicklist_import_filename
df_DD_kicks = pd.read_csv(dd_kicklist_import_pathandfile).set_index('LoanId')
#------------------------------------------------------------------------------------------------------------
#^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
"""




#df_eligible = df.loc[(df['EligibilityScrub'] == '') & (df.FLAG_PS_KICKS.isnull())][['CurrentPrincipalBalanceAmt','EligibilityScrub','FLAG_PS_KICKS']] #& (df.Flag_pricing_kicks.isnull()) & (df.Flag_dd_kicks.isnull()) ]


#############################################
#UPDATE
#############################################
df.update(df_PS_kicks)



#TEMP COMMENT OUT
#DELETE MERGE SECTION ONCE CLEANED UP (USING UPDATE METHOD NOW)
"""
#??????????????????????????????????????????????????????????????????????????????????????
#############################################
#ADD PRICING, ALLOCATION, PORT STRAT, AND DD COLUMNS TO DF
#############################################
#merge dataframes of kick lists and allocation lists
df = pd.merge(df, df_PS_kicks, how='left', suffixes=('','_PS'), left_index=True, right_index=True)
df = pd.merge(df, df_pricing_kicks, how='left', suffixes=('','_pricing_kicks'), left_index=True, right_index=True)
df = pd.merge(df, df_DD_kicks, how='left', suffixes=('','_dd_kicks'), left_index=True, right_index=True)
df = pd.merge(df, df_allocation, how='left', suffixes=('','_Allocation'), left_index=True, right_index=True)

#^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
"""


#------------------------------------------------------------------------------------------------------------
#Export list of eligible loans after filtering out Port Strat kicks, due diligence kicks, and pricing kicks
#------------------------------------------------------------------------------------------------------------

#BELOW VERSION IS WHEN THERE ARE KICK FLAGS DEFINED
#df_eligible = df.loc[(df['EligibilityScrub'] == '') & (df.Flag.isnull()) & (df.Flag_pricing_kicks.isnull()) & (df.Flag_dd_kicks.isnull()) ][['CurrentPrincipalBalanceAmt','EligibilityScrub','Flag','Flag_pricing_kicks','Flag_dd_kicks','Flag_Allocation']]


"""
#??????????????????????????????????????????????????????????????????????????????????????
#USE THIS FOR NOW INTRA MONTH
#BELOW VERSION IS WHEN THERE ARE NO KICK FLAGS YET - USE THIS FOR NOW INTRA MONTH
#df_eligible = df.loc[(df['EligibilityScrub'] == '')][['CurrentPrincipalBalanceAmt','EligibilityScrub']]
#^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
"""
df_eligible = df.loc[(df['EligibilityScrub'] == '') & (df.FLAG_PS_KICKS == '')][['CurrentPrincipalBalanceAmt','EligibilityScrub','FLAG_PS_KICKS']] #& (df.Flag_pricing_kicks.isnull()) & (df.Flag_dd_kicks.isnull()) ]

#df_eligible = df.loc[(df['EligibilityScrub'] == '')][['CurrentPrincipalBalanceAmt','EligibilityScrub']]
#------------------------------------------------------------------------------------------------------------

df.to_excel(ebo_eligibility_export_fileandpath) #index=False
df.to_excel(ebo_eligibility_export_fileandpath_withTimeStamp) #index=False, export one with date
df_eligible.to_csv(ebo_eligibility_export_fileandpath_loannums) #index=False #, columns=['CurrentPrincipalBalanceAmt','EligibilityScrub']

#result_time2 = time.localtime()
#time_diff = time.asctime(result_time2) - time.asctime(result_time)
#time_string2 = time.strftime("%H:%M:%S", time_diff)
elapsed_time = round((time.process_time() - t),1)

print("Runtime was: ", elapsed_time, " seconds\n") #add seconds formatting on to-do list
print('DF Eligible (head):\n\n', df_eligible.head())

#print some statistics to console for user color
print('\n')
print('Initial EBO Count (rows):',df.shape[0])
print('Initial EBO Total UPB: $'+str(round(df.CurrentPrincipalBalanceAmt.sum()/1000000,1))+'mm')
print('Eligible not kicked count (rows):', df_eligible.shape[0])
print('Eligible not kicked Total UPB: $'+str(round(df_eligible.CurrentPrincipalBalanceAmt.sum()/1000000, 1))+'mm\n')
#print('Eligible Allocated the Mass Mutual (Total UPB and loan count): $'+str(round(df_eligible.loc[df_eligible.Flag_Allocation == 'Mass Mutual'].CurrentPrincipalBalanceAmt.sum()/1000000, 1))+'mm, ' + str(df_eligible.loc[df_eligible.Flag_Allocation == 'Mass Mutual'].CurrentPrincipalBalanceAmt.count()) + ' loans' )
print('\nEligible population exported to: \n',ebo_eligibility_export_fileandpath, '\n', 'and\n', ebo_eligibility_export_fileandpath_withTimeStamp)



#MAYBE ADD TO STEP THAT ADDS THE TRENDIX CSV TO THE FINAL OUTPUT

#------------------------------------------------------------------------------------------------------------
#junk leftover code

#update FINAL_WATERFALL_VALUE with CUR_HOUSE_PRICE
#df_final.loc[df_final['FINAL_WATERFALL_APPROACH'] == 'Pull Trendix', 'FINAL_WATERFALL_VALUE'] = df_final.CUR_HOUSE_PRICE
#FOR REFERENCE
#df_final.loc[df_final['FINAL_WATERFALL_APPROACH'] == 'Pull Trendix', 'FINAL_WATERFALL_VALUE'] = df_final.CUR_HOUSE_PRICE


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


#example from ibm class
#df_del_na.loc[df_del_na['Neighbourhood'] == "Not assigned", 'Neighbourhood'] = "Queen's Park"
#df_del_na

#TESTING ERRORS - DELETE FROM HERE DOWN ONCE CLEANED UP
#df = pd.merge(df, df_PS_kicks, how='left', left_index=True, right_index=True)
#or use concatenate
#df = pd.concat([df, df_PS_kicks], axis=1, join='outer') #ValueError: Only can inner (intersect) or outer (union) join the other axis
#print(df.loc[[df_PS_kicks.index]].head())  #KeyError: "None of [Index([(1004304742,)], dtype='object', name='LoanId')] are in the [index]"
#print(df.columns)
#print(df.shape[0])
#print(df.iloc[[df.index==df_PS_kicks.index][:]].head()) #ValueError: Lengths must match to compare
#ENCOURAGING...GOT DIFFERENT ERROR WHICH MEANS THIS WORKED
#print(df.loc[[df_PS_kicks.index]].head()) #KeyError: "None of [Index([(1234567890,)], dtype='object', name='LoanId')] are in the [index]"
#TEMP WORKING TO ADD DUMMY KICK LOANS, COLUMNS, MERGE AND ELIGIBILITY LIST
#"""
#??????????????????????????????????????????????????????????????????????????????????????
#############################################
#ADD PRICING, ALLOCATION, PORT STRAT, AND DD COLUMNS TO DF
#############################################
#merge dataframes of kick lists and allocation lists
# df = pd.merge(df, df_PS_kicks, how='left', left_index=True, right_index=True)
# df = pd.merge(df, df_pricing_kicks, how='left', suffixes=('','_pricing_kicks'), left_index=True, right_index=True)
# df = pd.merge(df, df_DD_kicks, how='left', suffixes=('','_dd_kicks'), left_index=True, right_index=True)
# df = pd.merge(df, df_allocation, how='left', suffixes=('','_Allocation'), left_index=True, right_index=True)

#^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
#"""