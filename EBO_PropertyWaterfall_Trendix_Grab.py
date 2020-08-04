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
    20200323:
        + Script results sometimes had different rows between Trendix and final waterfall output (Trendix>)
        + Added append function to add Trendix entries not included in the final waterfall output
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
import math
import os


#clock starts to time how long the df import takes
#start_tm = time.clock()


#############################################
#START THE TIMER TO SEE HOW LONG PROCESS TAKES
#############################################
result_time = time.localtime()
time_string = time.strftime("%Y%m%d", result_time)
t = time.process_time()


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
trendix_export_path = r'M:\Capital Markets\GNMA EBO Project\Python'
trendix_export_filename = '\waterfall_trendix_export.csv' #CONSIDER ADDING WATERFALL FOLDER FOR OUTPUT
trendix_export_fileandpath = trendix_export_path + trendix_export_filename


#############################################
#SETTINGS - Set the export filename and location for property value dataframes
#############################################
#df export info (BEFORE df updates)
df_export_path = r'M:\Capital Markets\GNMA EBO Project\Python'
df_export_filename = '\df_waterfall_orig.xlsx' #CONSIDER ADDING WATERFALL FOLDER FOR OUTPUT
df_export_fileandpath = df_export_path + df_export_filename

#df export info (AFTER df updates)
#CONSIDER ADDING WATERFALL FOLDER FOR OUTPUT
df_final_export_path = r'M:\Capital Markets\GNMA EBO Project\Python'
df_final_export_filename = '\df_waterfall_final.xlsx' 
df_final_export_fileandpath = df_final_export_path + df_final_export_filename
df_final_export_filename_withTimeStamp = '\df_waterfall_final_' + time_string + '.xlsx'
df_final_export_fileandpath_withTimeStamp = df_final_export_path + df_final_export_filename_withTimeStamp


#############################################
#SETTINGS - Set file paths (for xlsx/csv import)
#############################################
loan_list_path = r'M:\Capital Markets\GNMA EBO Project\Python' 
loan_list_filename = '\ebo_eligibility_list.csv'  #ADDED 1, 2 DUE TO LARGE SCRIPT SIZING #CONSIDER ADDING WATERFALL FOLDER FOR OUTPUT
#loan_list_filename = '\EBO_LOAN_LIST.csv'  #ADDED 1, 2 DUE TO LARGE SCRIPT SIZING #CONSIDER ADDING WATERFALL FOLDER FOR OUTPUT
loan_list_fileandpath = loan_list_path + loan_list_filename


#Read the loan list from excel/csv into into a dataframe
df_loannums = pd.read_csv(loan_list_fileandpath)


#############################################
#GRAB LOAN NUMBERS FROM ELIGIBILITY FILE
#USING JOINED_LIST FUNCTION
#############################################

#set the list of potential column names for loan number
column_names = ['LoanId', 'LoanNumber','LOAN_NO', 'LoanNum']

for i in range(len(column_names)):
    if column_names[i] in df_loannums.columns:
        df_loannums.set_index(column_names[i], inplace=True)
        df_loannums.index = df_loannums.index.map(str) #convert loan numbers to string
        my_list = df_loannums.index #create list of loan numbers (for joining later)

for i in range(len(my_list)):
    if i == 0:
        joined_list = "'"+str(my_list[i])+"'"
    else:
        joined_list = joined_list+",'"+str(my_list[i])+"'"



#############################################
#set the SQL connection, query path, and query file name
#############################################


#############################################
#RUN THE SQL QUERIES AND CREATE DATAFRAMES
#############################################
#Read the SQL query results into a pandas dataframe
#define the opening of the filename and path of the sql file
#query = open(sql_fileandpath) #when referencing SQL file (not text)
#query_trendix = open(sql_fileandpath_trendix) #when referencing SQL file (not text)
        
query = """
--This section automatically calculates the MEPeriod for the end of the previous month
Declare @MEPeriod INT
SET @MEPeriod = (SELECT LEFT(convert(varchar, DATEADD(MONTH, -1, GETDATE()), 112),6))

--This section automatically calculates the DateKey for the end of the previous month
DECLARE @DateKey INT
SET @DateKey = (select CONVERT(varchar,dateadd(d,-(day(getdate())),getdate()),112))


SELECT 

pspv.LoanNumber
,CASE
	WHEN pspv.CA_Value = 0 THEN NULL
	ELSE pspv.CA_Value
END AS CA_Value 
,CA_Conf_Score 
,pspv.CA_FSD
,pspv.CA_High_Value
,pspv.CA_Low_Value
,cast(convert(decimal(12,2),pspv.Trendix_Value) as money) AS Trendix_Value
,pspv.Leadgen_Value
,pspv.Leadgen_Source
,pspv.RunDate
,lmme.LoanClosingDt AS Closing_Date
,convert(varchar(10),lmme.DocumentationType) AS DocType 
,cast(convert(decimal(12,2),lf.CurrentUPB) as money) as CurrentUPB 
,cast(convert(decimal(12,2),lf.CurrentCombinedUPB) as money) as CurrentCombinedUPB 
,cast(convert(decimal(12,2),lmme.OriginalPrincipalBalanceAmt) as money) as OriginalPrincipalBalanceAmt
,cast(convert(decimal(12,2),pm.OriginalAppraisalAmt) as money) as OriginalAppraisalAmt 
,CASE
	WHEN CA_Value = 0 THEN NULL
	ELSE ROUND((lf.CurrentCombinedUPB / pspv.CA_Value), 8)
END AS CA_Value_LTV 
,GETDATE() as Today
,datediff(month,(lmme.LoanClosingDt),(pspv.RunDate)) as Date_Diff_Months
,Case
	when ld.OriginalLTVRatio = 0 then lmme.OriginalLoanToValueRatio
	when LEFT(lf.LoanNumber,1) = '7' Then ROUND((ed.OriginationPILoanAmountWithPMIMIPAmount / ed.PropertyAppraisedValue), 8)
	Else ROUND(ld.OriginalLTVRatio, 8)
End As OrigLTV
,@MEPeriod as MEPeriod
,@DateKey as DateKey
,CASE
	WHEN lmme.DocumentationType IS NULL THEN  CASE
													WHEN OriginalAppraisalAmt > 0 THEN cast(OriginalAppraisalAmt as numeric(16,2))
													ELSE
														CASE
															WHEN ld.OriginalLTVRatio = 0 THEN	CASE
																									WHEN lmme.OriginalLoanToValueRatio > 0 THEN (lmme.OriginalPrincipalBalanceAmt / lmme.OriginalLoanToValueRatio)
																									ELSE lmme.OriginalPrincipalBalanceAmt 
																								END
															WHEN LEFT(lf.LoanNumber,1) = '7' THEN (lmme.OriginalPrincipalBalanceAmt / (ed.OriginationPILoanAmountWithPMIMIPAmount / ed.PropertyAppraisedValue)) 
															ELSE (lmme.OriginalPrincipalBalanceAmt / ld.OriginalLTVRatio) 
														END
												END
	WHEN ISNUMERIC(lmme.DocumentationType) = 1 AND convert(varchar,lmme.DocumentationType) = '1' THEN	CASE
																											WHEN OriginalAppraisalAmt > 0 THEN cast(OriginalAppraisalAmt as numeric(16,2))
																											ELSE
																												CASE
																													WHEN ld.OriginalLTVRatio = 0 THEN	CASE  --SUMMERS--> IF OLTV = 0, THEN USE ORIGBAL
																																							WHEN lmme.OriginalLoanToValueRatio > 0 THEN (lmme.OriginalPrincipalBalanceAmt / lmme.OriginalLoanToValueRatio)
																																							ELSE lmme.OriginalPrincipalBalanceAmt --SUMMERS INPUT 1/10-> USE CA_VALUE IN THIS CASE
																																						END
																													WHEN LEFT(lf.LoanNumber,1) = '7' THEN (lmme.OriginalPrincipalBalanceAmt / (ed.OriginationPILoanAmountWithPMIMIPAmount / ed.PropertyAppraisedValue)) 
																													ELSE (lmme.OriginalPrincipalBalanceAmt / ld.OriginalLTVRatio) 
																												END
																										END
	WHEN ISNUMERIC(lmme.DocumentationType) = 0 AND convert(varchar,lmme.DocumentationType) = 'B' THEN	CASE 
																											WHEN OriginalAppraisalAmt > 0 THEN cast(OriginalAppraisalAmt as numeric(16,2))
																											ELSE
																												CASE
																													WHEN ld.OriginalLTVRatio = 0 THEN	CASE  --ADD CASE HERE FOR ZERO LMME.OLTV
																																							WHEN lmme.OriginalLoanToValueRatio > 0 THEN (lmme.OriginalPrincipalBalanceAmt / lmme.OriginalLoanToValueRatio)
																																							ELSE lmme.OriginalPrincipalBalanceAmt --SUMMERS INPUT 1/10-> USE ORIGBAL IN THIS CASE
																																						END 
																													WHEN LEFT(lf.LoanNumber,1) = '7' THEN (lmme.OriginalPrincipalBalanceAmt / (ed.OriginationPILoanAmountWithPMIMIPAmount / ed.PropertyAppraisedValue)) 
																													ELSE (lmme.OriginalPrincipalBalanceAmt / ld.OriginalLTVRatio) 
																												END
																										END
 	WHEN CA_Conf_Score = 50 THEN	CASE 
										WHEN ISNUMERIC(lmme.DocumentationType) = 1 AND convert(varchar,lmme.DocumentationType) = '2' THEN 999 --FIXED VARCHAR TO FLOAT ERROR IN DOCTYPE SECTION
										WHEN OriginalAppraisalAmt > 0 THEN cast(OriginalAppraisalAmt as numeric(16,2))
										ELSE
											CASE
												WHEN ld.OriginalLTVRatio = 0 THEN	CASE  --ADD CASE HERE FOR ZERO LMME.OLTV
																						WHEN lmme.OriginalLoanToValueRatio > 0 THEN (lmme.OriginalPrincipalBalanceAmt / lmme.OriginalLoanToValueRatio)
																						ELSE 999
																					END 
												WHEN LEFT(lf.LoanNumber,1) = '7' THEN (lmme.OriginalPrincipalBalanceAmt / (ed.OriginationPILoanAmountWithPMIMIPAmount / ed.PropertyAppraisedValue)) 
												ELSE (lmme.OriginalPrincipalBalanceAmt / ld.OriginalLTVRatio) 
											END 
									END 
	WHEN ISNUMERIC(lmme.DocumentationType) = 1 AND convert(varchar,lmme.DocumentationType) = '2' AND ( datediff(month,(lmme.LoanClosingDt),(GETDATE())) > 48 ) THEN	CASE
																											WHEN CA_Value > 0 THEN CA_Value --SUMMERS INPUT 1/10-> USE CA_VALUE IN THIS CASE 
																											ELSE 999
																										END
	WHEN CA_Conf_Score > 50 AND ( datediff(month,(lmme.LoanClosingDt),(GETDATE())) > 48 ) THEN	CASE --ADD CASE FOR ORIG APPRAISAL OR OLTV CALC
																									WHEN (ISNUMERIC(lmme.DocumentationType) = 0 AND convert(varchar,lmme.DocumentationType) = 'NULL') 
																										OR (ISNUMERIC(lmme.DocumentationType) = 0 AND lmme.DocumentationType IS NULL) THEN 
																										CASE --FIXED VARCHAR TO FLOAT ERROR IN DOCTYPE SECTION
																											WHEN OriginalAppraisalAmt > 0 THEN cast(OriginalAppraisalAmt as numeric(16,2))
																											ELSE
																												CASE
																													WHEN ld.OriginalLTVRatio = 0 THEN	CASE  --ADD CASE HERE FOR ZERO LMME.OLTV
																																							WHEN lmme.OriginalLoanToValueRatio > 0 THEN (lmme.OriginalPrincipalBalanceAmt / lmme.OriginalLoanToValueRatio)
																																							ELSE 999
																																						END 
																													WHEN LEFT(lf.LoanNumber,1) = '7' THEN (lmme.OriginalPrincipalBalanceAmt / (ed.OriginationPILoanAmountWithPMIMIPAmount / ed.PropertyAppraisedValue)) 
																													ELSE (lmme.OriginalPrincipalBalanceAmt / ld.OriginalLTVRatio) 
																												END 
																										END  --CA_Value  --ADD CASE FOR ORIG APPRAISAL OR OLTV CALC
																									WHEN ISNUMERIC(lmme.DocumentationType) = 1 AND convert(varchar,lmme.DocumentationType) = '2' THEN CA_Value
																									--ADD ELSE CASE FOR THE REST THAT DON'T HAVE NULL DOC TYPES?
																								END 
	WHEN ((CA_Conf_Score BETWEEN 51 AND 84) AND ( datediff(month,(lmme.LoanClosingDt),(GETDATE())) <= 48 ) and ((lf.CurrentCombinedUPB / pspv.CA_Value) > 1)) THEN 999 
	WHEN ((CA_Conf_Score BETWEEN 51 AND 84) AND ( datediff(month,(lmme.LoanClosingDt),(GETDATE())) <= 48 ) and ((lf.CurrentCombinedUPB / pspv.CA_Value) <= 1)) THEN CASE
																											WHEN CA_Value > 0 THEN CA_Value --SUMMERS INPUT 1/10-> USE CA_VALUE IN THIS CASE 
																											ELSE 999
																										END
	WHEN ((CA_Conf_Score > 84) AND ( datediff(month,(lmme.LoanClosingDt),(GETDATE())) <= 48 ) and ((lf.CurrentCombinedUPB / pspv.CA_Value) > 1.2)) THEN	CASE 
																																							WHEN OriginalAppraisalAmt > 0 THEN cast(OriginalAppraisalAmt as numeric(16,2))
																																							ELSE 
																																								CASE
																																									WHEN ld.OriginalLTVRatio = 0 THEN	CASE  --ADD CASE HERE FOR ZERO LMME.OLTV
																																																			WHEN lmme.OriginalLoanToValueRatio > 0 THEN (lmme.OriginalPrincipalBalanceAmt / lmme.OriginalLoanToValueRatio)
																																																			ELSE 999
																																																		END 
																																									WHEN LEFT(lf.LoanNumber,1) = '7' THEN (OriginalPrincipalBalanceAmt / (ed.OriginationPILoanAmountWithPMIMIPAmount / ed.PropertyAppraisedValue)) 
																																									ELSE (OriginalPrincipalBalanceAmt / ld.OriginalLTVRatio) 
																																								END 

																																						END  
	WHEN ((CA_Conf_Score > 84) AND ( datediff(month,(lmme.LoanClosingDt),(GETDATE())) <= 48 ) and ((lf.CurrentCombinedUPB / pspv.CA_Value) <= 1.2)) THEN CA_Value 
	ELSE 999
END as FINAL_WATERFALL_VALUE

,CASE
	WHEN lmme.DocumentationType IS NULL THEN	CASE
													WHEN OriginalAppraisalAmt > 0 THEN 'OrigAppraisal'
													ELSE
														CASE
															WHEN ld.OriginalLTVRatio = 0 THEN	CASE  --ADD CASE HERE FOR ZERO LMME.OLTV
																									WHEN lmme.OriginalLoanToValueRatio > 0 THEN 'Calc Using OrigLTV'
																									ELSE 'Use OrigBal'
																								END
															WHEN LEFT(lf.LoanNumber,1) = '7' THEN 'Calc Using OrigLTV' 
															ELSE 'Calc Using OrigLTV' 
														END
												END
	WHEN ISNUMERIC(lmme.DocumentationType) = 1 AND convert(varchar,lmme.DocumentationType) = '1' THEN	CASE
																											WHEN OriginalAppraisalAmt > 0 THEN 'OrigAppraisal'
																											ELSE
																												CASE
																													WHEN ld.OriginalLTVRatio = 0 THEN	CASE  --ADD CASE HERE FOR ZERO LMME.OLTV
																																							WHEN lmme.OriginalLoanToValueRatio > 0 THEN 'Calc Using OrigLTV'
																																							ELSE 'Use OrigBal'
																																						END
																													WHEN LEFT(lf.LoanNumber,1) = '7' THEN 'Calc Using OrigLTV' 
																													ELSE 'Calc Using OrigLTV' 
																												END
																										END
	WHEN ISNUMERIC(lmme.DocumentationType) = 0 AND convert(varchar,lmme.DocumentationType) = 'B' THEN	CASE
																											WHEN OriginalAppraisalAmt > 0 THEN 'OrigAppraisal'
																											ELSE
																												CASE
																													WHEN ld.OriginalLTVRatio = 0 THEN	CASE  --ADD CASE HERE FOR ZERO LMME.OLTV
																																							WHEN lmme.OriginalLoanToValueRatio > 0 THEN 'Calc Using OrigLTV'
																																							ELSE 'Use OrigBal'
																																						END
																													WHEN LEFT(lf.LoanNumber,1) = '7' THEN 'Calc Using OrigLTV' 
																													ELSE 'Calc Using OrigLTV' 
																												END
																										END
 	WHEN CA_Conf_Score = 50 THEN	CASE 
										WHEN ISNUMERIC(lmme.DocumentationType) = 1 AND convert(varchar,lmme.DocumentationType) = '2' THEN 'Pull Trendix'
										WHEN OriginalAppraisalAmt > 0 THEN 'OrigAppraisal'
										ELSE
											CASE
												WHEN ld.OriginalLTVRatio = 0 THEN	CASE  --ADD CASE HERE FOR ZERO LMME.OLTV
																						WHEN lmme.OriginalLoanToValueRatio > 0 THEN 'Calc Using OrigLTV'
																						ELSE 'Pull Trendix'
																					END 
												WHEN LEFT(lf.LoanNumber,1) = '7' THEN 'Calc Using OrigLTV' 
												ELSE 'Calc Using OrigLTV'
											END 
									END 
	WHEN ISNUMERIC(lmme.DocumentationType) = 1 AND convert(varchar,lmme.DocumentationType) = '2' AND ( datediff(month,(lmme.LoanClosingDt),(GETDATE())) > 48 ) THEN	CASE
																											WHEN CA_Value > 0 THEN 'Collateral Analytics' --SUMMERS INPUT 1/10-> USE CA_VALUE IN THIS CASE 
																											ELSE 'Pull Trendix'
																										END
	WHEN CA_Conf_Score > 50 AND ( datediff(month,(lmme.LoanClosingDt),(GETDATE())) > 48 ) THEN	CASE --ADD CASE FOR ORIG APPRAISAL OR OLTV CALC
																									WHEN (ISNUMERIC(lmme.DocumentationType) = 0 AND convert(varchar,lmme.DocumentationType) = 'NULL') 
																										OR (ISNUMERIC(lmme.DocumentationType) = 0 AND lmme.DocumentationType IS NULL) THEN 
																										CASE --FIXED VARCHAR TO FLOAT ERROR IN DOCTYPE SECTION
																											WHEN OriginalAppraisalAmt > 0 THEN 'OrigAppraisal'
																											ELSE
																												CASE
																													WHEN ld.OriginalLTVRatio = 0 THEN	CASE  --ADD CASE HERE FOR ZERO LMME.OLTV
																																							WHEN lmme.OriginalLoanToValueRatio > 0 THEN 'Calc Using OrigLTV'
																																							ELSE 'Pull Trendix'
																																						END 
																													WHEN LEFT(lf.LoanNumber,1) = '7' THEN 'Calc Using OrigLTV'
																													ELSE 'Calc Using OrigLTV'
																												END 
																										END  --CA_Value  --ADD CASE FOR ORIG APPRAISAL OR OLTV CALC
																									WHEN ISNUMERIC(lmme.DocumentationType) = 1 AND convert(varchar,lmme.DocumentationType) = '2' THEN 'Collateral Analytics'
																								END  
	WHEN ((CA_Conf_Score BETWEEN 51 AND 84) AND ( datediff(month,(lmme.LoanClosingDt),(GETDATE())) <= 48 ) and ((lf.CurrentCombinedUPB / pspv.CA_Value) > 1)) THEN 'Pull Trendix'
	WHEN ((CA_Conf_Score BETWEEN 51 AND 84) AND ( datediff(month,(lmme.LoanClosingDt),(GETDATE())) <= 48 ) and ((lf.CurrentCombinedUPB / pspv.CA_Value) <= 1)) THEN CASE
																											WHEN CA_Value > 0 THEN 'Collateral Analytics' --SUMMERS INPUT 1/10-> USE CA_VALUE IN THIS CASE 
																											ELSE 'Pull Trendix'
																										END
	WHEN ((CA_Conf_Score > 84) AND ( datediff(month,(lmme.LoanClosingDt),(GETDATE())) <= 48 ) and ((lf.CurrentCombinedUPB / pspv.CA_Value) > 1.2)) THEN	CASE 
																																							WHEN OriginalAppraisalAmt > 0 THEN 'OrigAppraisal'
																																							ELSE 
																																								CASE
																																									WHEN ld.OriginalLTVRatio = 0 THEN	CASE  --ADD CASE HERE FOR ZERO LMME.OLTV
																																																			WHEN lmme.OriginalLoanToValueRatio > 0 THEN 'Calc Using OrigLTV'
																																																			ELSE 'Pull Trendix'
																																																		END 
																																									WHEN LEFT(lf.LoanNumber,1) = '7' THEN 'Calc Using OrigLTV'
																																									ELSE 'Calc Using OrigLTV'
																																								END 
																																						END
	WHEN ((CA_Conf_Score > 84) AND ( datediff(month,(lmme.LoanClosingDt),(GETDATE())) <= 48 ) and ((lf.CurrentCombinedUPB / pspv.CA_Value) <= 1.2)) THEN 'Collateral Analytics'
	ELSE 'Pull Trendix'
END as FINAL_WATERFALL_APPROACH


FROM portfolio_strategy.megatron.LeadModel_Combined_PropertyValue_Hist PSPV 
	LEFT JOIN [w08-vm-sql-3].dw.dbo.loan_fact lf ON pspv.LoanNumber = lf.LoanNumber and lf.DateKey = @DateKey
	LEFT JOIN smd..Loan_Master_ME lmme ON lmme.LoanId = pspv.LoanNumber AND lmme.meperiod = (SELECT LEFT(convert(varchar, @MEPeriod, 112),6))
	left join [w08-vm-sql-3].dw.dbo.loan_dim ld on ld.LoanNumber = pspv.LoanNumber
	left join dw_retail..loan ED (Nolock) on ED.LoanNumber = pspv.LoanNumber
	left join smd..property_master_me pm on lmme.loanid = pm.loanid and pm.meperiod = @MEPeriod

INNER JOIN	
	(
	SELECT LoanNumber, MAX(RunDate) AS MaxRunDate FROM portfolio_strategy.megatron.LeadModel_Combined_PropertyValue_Hist
	GROUP BY LoanNumber
	) grouped_pspv
		ON pspv.LoanNumber = grouped_pspv.LoanNumber
		and pspv.RunDate = grouped_pspv.MaxRunDate

WHERE pspv.LoanNumber in (%s)


AND

lf.DateKey = @DateKey

AND lmme.meperiod = @MEPeriod
""" % (joined_list)

query_trendix = """
--This section automatically calculates the DateKey for the end of the previous month
DECLARE @DateKey_NextMonth date
SET @DateKey_NextMonth = (DATEADD(MONTH, 1, GETDATE())) --(select CONVERT(varchar,dateadd(d,(day(getdate())),getdate()),112))



select
LM.LoanId as LOAN_NO,
CASE
	when PM.AppraisalAmt>0 then (PM.AppraisalAmt)
	when LM.OriginalLoanToValueRatio>0 then (LM.OriginalPrincipalBalanceAmt/LM.OriginalLoanToValueRatio)
	when LM.OriginalLoanToValueRatio=0 and PM.AppraisalAmt=0 then (LM.OriginalPrincipalBalanceAmt)
end as APP_VALUE,
CASE
	when PM.AppraisalDt>'1/1/1900' then (PM.AppraisalDt)
	when LM.OriginalPrincipalBalanceAmt>0 then (NoteDt)
	when LM.OriginalLoanToValueRatio>0 then (NoteDt)
end as APPRAISAL_DATE,
Case
	when PM.PropertyTypeId in (000) then '1'
	when PM.PropertyTypeId in (001) then '1'
	when PM.PropertyTypeId in (002) then '5'
	when PM.PropertyTypeId in (003) then '2'
	when PM.PropertyTypeId in (004) then '4'
	when PM.PropertyTypeId in (005) then '9'
	when PM.PropertyTypeId in (006) then '9'
	when PM.PropertyTypeId in (007) then '9'
	when PM.PropertyTypeId in (008) then '6'
	when PM.PropertyTypeId in (009) then '9'
	when PM.PropertyTypeId in (010) then '3'
	when PM.PropertyTypeId in ('00A') then '9'
	when PM.PropertyTypeId in ('00B') then '9'
	when PM.PropertyTypeId in ('00C') then '9'
	when PM.PropertyTypeId in ('00D') then '9'
	else '9'
End as PROP_TYPE,
Case
	when LM.LoanPurposeCodeId in (1) then '1'
	when LM.LoanPurposeCodeId in (3) then '1'
	when LM.LoanPurposeCodeId in (4) then '1'
	when LM.LoanPurposeCodeId in (5) then '2'
	when LM.LoanPurposeCodeId in (6) then '2'
	when LM.LoanPurposeCodeId in (7) then '2'
	when LM.LoanPurposeCodeId in (8) then '3'
	when LM.LoanPurposeCodeId in (9) then '9'
	else '9'
end as PURPOSE,
PM.PropertyState as STATE,
PM.PropertyZip as ZIP,
convert(varchar(10),cast(@DateKey_NextMonth as Date),101) as TARGET_DATE --cast((@DateKey_NextMonth as Date),101)


from SMD..Loan_Master LM

inner join SMD..Property_Master PM
on PM.LoanId=LM.Loanid


where LM.LoanId in (%s)

""" % (joined_list)

#Read the sql file results into dataframes
#Make sure any loan lists in the query are updated prior to running
df = pd.read_sql_query(query,sql_conn).set_index('LoanNumber') #changed query.read() to just query #, chunksize=1000,index_col='LoanNumber') #.set_index('LoanNumber'))


#NOTE ON DF INDEX BELOW
#changing df index dtype to int not needed...keeping it as object to match trendix index dtype...too many issues with int for loannumber

df_trendix = pd.read_sql_query(query_trendix,sql_conn).set_index('LOAN_NO') #changed query_trendix.read() to just query_trendix 
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
for i in range(60):
    print(str(60-i))
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

#############################################
#APPEND TRENDIX DF VALUES TO DF_FINAL
#############################################

#------------------------------------------------------------------------------------------------------------
#CREATE TEMP TRENDIX DF FOR APPEND
#------------------------------------------------------------------------------------------------------------
df_trendix_append = df_trendix_out[['CUR_HOUSE_PRICE']]
df_trendix_append.index.astype(np.str)
df_trendix_append = df_trendix_append[~df_trendix_append.index.isin(df_final.index)] #subset of trendix not in df_final
df_trendix_append['FINAL_WATERFALL_APPROACH'] = 'Pull Trendix'
df_trendix_append['FINAL_WATERFALL_VALUE'] = df_trendix_append.CUR_HOUSE_PRICE
df_trendix_append.rename(index={df_trendix_append.index.name: df_final.index.name})
# df3 = pd.concat([df1,df2_append.rename(columns={'Cur_House_Px':'waterfall'})], ignore_index=False, sort=False)
# df3 = pd.concat([df1,df2_append.rename(columns={'Cur_House_Px':'waterfall'})], ignore_index=False, sort=False)
# df_trendix_out

#------------------------------------------------------------------------------------------------------------
#APPEND TRENDIX DF TO DF_FINAL
#------------------------------------------------------------------------------------------------------------
#df_final.append(df_trendix_append, ignore_index=False, sort=False)
df_final = pd.concat([df_final,df_trendix_append], ignore_index=False, sort=False)
df_final.index.name = 'LoanNumber' #this fixes the xlsx export not having a column nmae for loan number
#^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

print('df_trendix_append', df_trendix_append.head())
print('\n', 'df_trendix_append # of rows: \n', df_trendix_append.shape[0]) 
print('\n', 'df_final # of rows: \n', df_final.shape[0]) 
print('\n', 'Trendix # of rows: \n', df_trendix_out.shape[0]) 
print('\n', 'Trendix Output df (five rows): \n', df_trendix_out.head()) 
print('\n', 'df_final (five rows): \n', df_final.head())
print('\n', 'df_final property values (five rows): \n', df_final[['FINAL_WATERFALL_VALUE','CUR_HOUSE_PRICE']].head())


#------------------------------------------------------------------------------------------------------------
#Export df after updating Trendix value
#------------------------------------------------------------------------------------------------------------
df_final.to_excel(df_final_export_fileandpath) #,index=False
df_final.to_excel(df_final_export_fileandpath_withTimeStamp) #,index=False
df_trendix_out.to_csv(trendix_export_fileandpath) #,index=False
#------------------------------------------------------------------------------------------------------------

print("\n")
print('\nSee the following path for xlsx versions of the dataframes:\n\t',df_final_export_path)
print('\n \nAll Done!\n')

#############################################
#REPORT HOW LONG THE PROCESS TAKES
#############################################
elapsed_time = round((time.process_time() - t),1)
print("Runtime was: ", elapsed_time, " seconds\n") #add seconds formatting on to-do list

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