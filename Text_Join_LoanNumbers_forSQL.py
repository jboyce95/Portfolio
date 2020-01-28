# -*- coding: utf-8 -*-
"""
Created on Fri Jan  3 14:47:57 2020

@author: jboyce
"""


import pandas as pd
import pyodbc
import numpy as np
import time

#############################################
#SETTINGS - Set file paths (for xlsx/csv import)
#############################################
loan_list_path = r'M:\Capital Markets\GNMA EBO Project\Python'
loan_list_filename = '\ebo_eligibility_list_test.csv'  
loan_list_fileandpath = loan_list_path + loan_list_filename



#Read the loan list from excel/csv into into a dataframe
df = pd.read_csv(loan_list_fileandpath)

#set the list of potential column names for loan number
column_names = ['LoanId', 'LoanNumber','LOAN_NO', 'LoanNum']

for i in range(len(column_names)):
    if column_names[i] in df.columns:
        #print(column_names[i]) 
        #print(df[column_names[i]][:2])
        df.set_index(column_names[i], inplace=True)
        df.index = df.index.map(str) #convert loan numbers to string
        my_list = df.index #create list of loan numbers (for joining later)

#print(my_list)

#my_list = ['1001867441','1001874313','1001874706']
print(len(my_list))

for i in range(len(my_list)):
    if i == 0:
        joined_list = "'"+str(my_list[i])+"'"
        print(joined_list)
    else:
        joined_list = joined_list+",'"+str(my_list[i])+"'"

print(joined_list)