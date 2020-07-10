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
    
REVISION HISTORY:
    20200527:
        + Added days360 calculation and accrued interest calculation
    20200320:
        + PENDING - Adding reference to starting population (column, etc.) for marketing process
            ++ "ValueError: cannot reindex from a duplicate axis" caused by duplicate loannumbers
            ++ utf-8 unicode error caused by extra spaces at the end of loan numbers
    20200311:
        + COMPLETED Adding placeholder kick columns for pre-dd phase of marketing process
        + Note that this update there are errors thrown when there is only one loan in any kick file
    20200123:
        + Added to the dataframe the tracking of dd kicks, port strat kicks, pricing kicks from loan lists
            housed in the network drive
        + Updated eligiblity script to integrate VA Refi and NA Housing queries

##############################################
USER REQUIREMENTS: NOTES TO USER - IMPORTANT:
    + UPDATE FILENAMES FOR LOAN LEVELS
    + UPDATE SETTLE DATE (WILL ADD AUTO CALC DOWN THE ROAD)
    + 

    

##############################################

FIX DF_ELIGIBLE EXPORT WHEN STARTING POP NOT IDENTIFIED YET (ALL = 0, NO 1s)
"""

import pandas as pd
import numpy as np
import pyodbc
import time
from days360 import days360Calc
from datetime import date #f for any date 
import calendar # for days360 function

#clock starts to time how long the df import takes
#start_tm = time.clock()
result_time = time.localtime()
time_string = time.strftime("%Y%m%d", result_time)
t = time.process_time()

#############################################
# SETTINGS - TRADE TARGETS AND DETAILS
#############################################

settle_date = date(2020, 8, 3)
#add settle date calc 
#if YYYYMM01 not in (weekend or US Bank Holiday), then  YYYYMM01  
#elseif  YYYYMM02 not in (weekend or US Bank Holiday), then YYYYMM02
#elseif  YYYYMM03 not in (weekend or US Bank Holiday), then YYYYMM03
#else  YYYYMM04

mm_trade_size = 2e6
pbo_trade_size = 135e6



#############################################
# SETTINGS - SQL CONNECTIONS, SQL QUERY FILENAMES AND SQL PATHS
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
ebo_eligibility_export_filename_loannums_withTimeStamp = '\ebo_eligibility_list_' + time_string +  '.csv' 
ebo_eligibility_export_fileandpath_loannums_withTimeStamp = ebo_eligibility_export_path + ebo_eligibility_export_filename_loannums_withTimeStamp


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

# Set field to datetime for future use of time fields in days360 calculations
df['InterestPaidToDt'] = pd.to_datetime(df['InterestPaidToDt'])
df['SETTLE_DATE'] = settle_date

#print(df.shape[0])

#############################################
#ADD KICK AND ALLOCATION COLUMNS TO DATAFRAME
#############################################
df['FLAG_ALLOCATION'] = 0
df['FLAG_DD_KICKS'] = 0
df['FLAG_PCG_KICKS'] = 0
df['FLAG_PRICING_KICKS'] = 0
df['FLAG_PS_KICKS'] = 0
df['FLAG_STARTING_POP'] = 0
df['FLAG_ELIGIBILITY_FINAL'] = 0 # FOR USE LATER DURING ALLOCATION OPTIMIZATION
df['FINAL_WATERFALL_VALUE'] = 0
df['FLAG_COVID'] = 0

#############################################
#IMPORTS (KICK LISTS AND ALLOCATIONS) - Set path, filename for importing Pricing Kick List
#   then import into df
#############################################
#------------------------------------------------------------------------------------------------------------
#Port Strat
#------------------------------------------------------------------------------------------------------------
ps_kicklist_import_path = r'M:\Capital Markets\GNMA EBO Project\Python\Import'
ps_kicklist_import_filename = '\PS_Kicks_20200401_settle.csv' 
ps_kicklist_import_pathandfile = ps_kicklist_import_path + ps_kicklist_import_filename
df_PS_kicks = pd.read_csv(ps_kicklist_import_pathandfile).set_index('LoanId')

#update flag column to make sure it's populated
df_PS_kicks['FLAG_PS_KICKS'] = 1 # 'PS_KICK'
#------------------------------------------------------------------------------------------------------------
#Allocation
#------------------------------------------------------------------------------------------------------------
allocation_import_path = r'M:\Capital Markets\GNMA EBO Project\Python\Import'
allocation_import_filename = '\Allocation_20200401_settle.csv' 
allocation_import_pathandfile = allocation_import_path + allocation_import_filename
df_allocation = pd.read_csv(allocation_import_pathandfile).set_index('LoanId')
#------------------------------------------------------------------------------------------------------------
#Pricing
#------------------------------------------------------------------------------------------------------------
pricing_kicklist_import_path = r'M:\Capital Markets\GNMA EBO Project\Python\Import'
pricing_kicklist_import_filename = '\Pricing_Kicks_20200401_settle.csv' 
pricing_kicklist_import_pathandfile = pricing_kicklist_import_path + pricing_kicklist_import_filename
df_pricing_kicks = pd.read_csv(pricing_kicklist_import_pathandfile).set_index('LoanId')

#update flag column to make sure it's populated
df_pricing_kicks['FLAG_PRICING_KICKS'] = 1 # 'PRICING_KICK'
#------------------------------------------------------------------------------------------------------------
#DD Kicks
#------------------------------------------------------------------------------------------------------------
dd_kicklist_import_path = r'M:\Capital Markets\GNMA EBO Project\Python\Import'
dd_kicklist_import_filename = '\DD_Kicks_20200401_settle.csv' 
dd_kicklist_import_pathandfile = dd_kicklist_import_path + dd_kicklist_import_filename
df_DD_kicks = pd.read_csv(dd_kicklist_import_pathandfile).set_index('LoanId')

#update flag column to make sure it's populated
df_DD_kicks['FLAG_DD_KICKS'] = 1 # 'DD_KICK'
#------------------------------------------------------------------------------------------------------------
#PCG Kicks
#------------------------------------------------------------------------------------------------------------
pcg_kicklist_import_path = r'M:\Capital Markets\GNMA EBO Project\Python\Import'
pcg_kicklist_import_filename = '\PCG_Kicks_20200401_settle.csv' 
pcg_kicklist_import_pathandfile = pcg_kicklist_import_path + pcg_kicklist_import_filename
df_pcg_kicks = pd.read_csv(pcg_kicklist_import_pathandfile).set_index('LoanId')

#update flag column to make sure it's populated
df_pcg_kicks['FLAG_PCG_KICKS'] = 1 # 'PCG_KICK'
#------------------------------------------------------------------------------------------------------------
#Starting pop - This is the starting population that will roll down from here (no new loans added)
#------------------------------------------------------------------------------------------------------------
starting_pop_import_path = r'M:\Capital Markets\GNMA EBO Project\Python\Import'
starting_pop_import_filename = '\Starting_Pop_20200401_settle.csv' 
starting_pop_import_pathandfile = starting_pop_import_path + starting_pop_import_filename
df_starting_pop = pd.read_csv(starting_pop_import_pathandfile).set_index('LoanId')

#update flag column to make sure it's populated
df_starting_pop['FLAG_STARTING_POP'] = 1
#------------------------------------------------------------------------------------------------------------


#############################################
#UPDATE THE MAIN DATAFRAME WITH THE KICKS IN EACH CATEGORY
#############################################
df.update(df_PS_kicks)
df.update(df_pricing_kicks)
df.update(df_DD_kicks)
df.update(df_pcg_kicks)
df.update(df_starting_pop)
#STILL NEED TO ADD UPDATES FOR ALLOCATION

#############################################
# CALCULATE THE ACCRUED INTEREST FOR THE TRADES
#############################################

# Calculate the accrued interest using hte days360 function (make sure it's saved to the project folder)
df['DAYS360_DAYS'] = df.apply(lambda row: days360Calc(row['InterestPaidToDt'], settle_date), axis=1)

df['ACCRUED_INTEREST'] = df['DAYS360_DAYS'] * df['CurrentPrincipalBalanceAmt'] * df['PT at Buyout'] / 360


#############################################
# IMPORT THE PROPERTY VALUE FROM WATERFALL
#############################################

def grab_property_value():
    import_path = r'M:\Capital Markets\GNMA EBO Project\Python'
    import_filename = 'df_waterfall_final.xlsx'
    import_path_and_filename = import_path + '\\' + import_filename
    columns = ['LoanNumber','FINAL_WATERFALL_VALUE','FINAL_WATERFALL_APPROACH']
    df_PV = pd.read_excel(import_path_and_filename, usecols=columns).set_index('LoanNumber')
    
    return df_PV

df.update(grab_property_value())


#############################################
# ADD COVID FLAG TO QUERY IMPORT THE PROPERTY VALUE
#############################################





#############################################
#FILTER FINAL LIST OF ELIGIBLE LOANS AFTER KICKS
#############################################
#------------------------------------------------------------------------------------------------------------
#Filter out Port Strat kicks, due diligence kicks, PCG kicks, and pricing kicks
#------------------------------------------------------------------------------------------------------------

if df.FLAG_STARTING_POP.sum() >=100:
    df['FLAG_ELIGIBILITY_FINAL'] = 1
    df_eligible = df.loc[(df['EligibilityScrub'] == '') & (df.FLAG_DD_KICKS == 0) & (df.FLAG_PCG_KICKS == 0) & (df.FLAG_PRICING_KICKS == 0) & (df.FLAG_PS_KICKS == 0) & (df.FLAG_COVID == 0) & (df.FLAG_STARTING_POP == 1)][['CurrentPrincipalBalanceAmt','FINAL_WATERFALL_VALUE','EligibilityScrub','FLAG_DD_KICKS','FLAG_PCG_KICKS','FLAG_PRICING_KICKS','FLAG_PS_KICKS','FLAG_ALLOCATION','FLAG_STARTING_POP','FLAG_COVID','FLAG_ELIGIBILITY_FINAL']]
else:
    df_eligible = df.loc[(df['EligibilityScrub'] == '') & (df.FLAG_DD_KICKS == 0) & (df.FLAG_PCG_KICKS == 0) & (df.FLAG_PRICING_KICKS == 0) & (df.FLAG_PS_KICKS == 0) & (df.FLAG_COVID == 0) ][['CurrentPrincipalBalanceAmt','FINAL_WATERFALL_VALUE','EligibilityScrub','FLAG_DD_KICKS','FLAG_PCG_KICKS','FLAG_PRICING_KICKS','FLAG_PS_KICKS','FLAG_ALLOCATION','FLAG_STARTING_POP','FLAG_ELIGIBILITY_FINAL','FLAG_COVID']]
    


#^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

#FLAG_STARTING_POP

#------------------------------------------------------------------------------------------------------------
#Export list of eligible loans after filtering out Port Strat kicks, due diligence kicks, and pricing kicks
#------------------------------------------------------------------------------------------------------------
df.to_excel(ebo_eligibility_export_fileandpath) #index=False
df.to_excel(ebo_eligibility_export_fileandpath_withTimeStamp) #index=False, export one with date
df_eligible.to_csv(ebo_eligibility_export_fileandpath_loannums) #index=False #, columns=['CurrentPrincipalBalanceAmt','EligibilityScrub']
df_eligible.to_csv(ebo_eligibility_export_fileandpath_loannums_withTimeStamp) #index=False #, columns=['CurrentPrincipalBalanceAmt','EligibilityScrub']
#ebo_eligibility_export_fileandpath_loannums_withTimeStamp 

#############################################
#GET THE COUNTS OF EACH KICK...OR JUST CREATE A PIVOT...NEXT PART OF PROJECT
#############################################
#------------------------------------------------------------------------------------------------------------
#Count of DD, Port Strat, Pricing and PCG Repurchase Kicks
#------------------------------------------------------------------------------------------------------------

#NEED TO CHANGE THIS TO == 1
count_dd_kicks = df.loc[df['FLAG_DD_KICKS'] == 1]['FLAG_DD_KICKS'].count()  #.isin(['DD_KICK'])]['FLAG_DD_KICKS'].count() 
count_ps_kicks = df.loc[df['FLAG_PS_KICKS'] == 1]['FLAG_DD_KICKS'].count() #.isin(['PS_KICK'])]['FLAG_DD_KICKS'].count() 
count_pricing_kicks = df.loc[df['FLAG_PRICING_KICKS'] == 1]['FLAG_PRICING_KICKS'].count() #.isin(['PRICING_KICK'])]['FLAG_PRICING_KICKS'].count() 
count_pcg_kicks = df.loc[df['FLAG_PCG_KICKS'] == 1]['FLAG_PCG_KICKS'].count()  #.isin(['PCG_KICK'])]['FLAG_PCG_KICKS'].count() 
#------------------------------------------------------------------------------------------------------------

#result_time2 = time.localtime()
#time_diff = time.asctime(result_time2) - time.asctime(result_time)
#time_string2 = time.strftime("%H:%M:%S", time_diff)
elapsed_time = round((time.process_time() - t),1)

print("Runtime was: ", elapsed_time, " seconds\n") #add seconds formatting on to-do list
print('DF Eligible (head):\n\n', df_eligible.head())

#print some statistics to console for user color
print('\n')
print(f'Initial EBO Count (rows): {df.shape[0]:,.0f}') # ,df.shape[0])
print(f'Initial EBO Total UPB: ${(round(df.CurrentPrincipalBalanceAmt.sum()/1000000,1)):,.1f}mm')
print(f'Remaining (after kicks) count (rows): {df_eligible.shape[0]:,.0f}') #, df_eligible.shape[0]'
print(f'Remaining (after kicks) Total UPB: ${(round(df_eligible.CurrentPrincipalBalanceAmt.sum()/1000000,1)):,.1f}mm\n')
print('Kicks (not mutually exclusive):')
print(f'\tDue Diligence: {count_dd_kicks:,.0f}') #.format(count_dd_kicks))
print(f'\tPort Strat: {count_ps_kicks:,.0f}') #.format(count_ps_kicks)) #FLAG_PS_KICKS
print(f'\tPricing: {count_pricing_kicks:,.0f}') #.format(count_pricing_kicks))
print(f'\tPCG Repurchases: {count_pcg_kicks:,.0f}') #.format(count_pcg_kicks))
#print('Eligible Allocated the Mass Mutual (Total UPB and loan count): $'+str(round(df_eligible.loc[df_eligible.Flag_Allocation == 'Mass Mutual'].CurrentPrincipalBalanceAmt.sum()/1000000, 1))+'mm, ' + str(df_eligible.loc[df_eligible.Flag_Allocation == 'Mass Mutual'].CurrentPrincipalBalanceAmt.count()) + ' loans' )
print('\nEligible population exported to: \n\t',ebo_eligibility_export_fileandpath, '\n\t', ebo_eligibility_export_fileandpath_withTimeStamp)



#^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
#MAYBE ADD TO STEP THAT ADDS THE TRENDIX CSV TO THE FINAL OUTPUT

#------------------------------------------------------------------------------------------------------------
#junk leftover code

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
