# -*- coding: utf-8 -*-
"""
Created on Wed Apr 29 10:57:59 2020

@author: jboyce
"""

import pulp
import pandas as pd
import numpy as np


# import the file to be optimized
optimizer_path = r'M:\Capital Markets\GNMA EBO Project\Python\Optimizer'
optimizer_filename = 'Optimize_no_negative.xlsx' 
optimizer_pathandfile = optimizer_path + '\\' + optimizer_filename
df = pd.read_excel(optimizer_pathandfile).set_index('LoanId')

# ADD CLEANING OF NULLS/NANS FOR FIELDS

print(df.head())

# Create starting loan list
loan_list = list(df.index.values)
#print(loan_list[0:5]) # prints first 5 loannums


#SET DICTIONARIES
# Set total funding target for MM (including dlq interest)
TOTAL_MM = dict(zip(loan_list,df['(MM_px X UPB) + Accrued Interest']))

# Set dictionary for total target for PBO
TOTAL_PBO = dict(zip(loan_list,df['PBO_Total']))

# Create dictionary for NEC of MM
NEC_MM = dict(zip(loan_list,df['NEC_MM']))

# Create dictionary for NEC of PBO
NEC_PBO = dict(zip(loan_list,df['NEC_PBO']))

# Create dictionary for PBO flag
FLAG_PBO = dict(zip(loan_list,df['PBO_Flag']))

# Create dictionary for MM flag
FLAG_MM = dict(zip(loan_list,df['MM_Flag']))


# SET DECISION VARIABLES
# SET VARIABLES
mm_integer = pulp.LpVariable.dicts("MM_Selector",loan_list,0,1,pulp.LpBinary) # binary indicator for allocation to MM
pbo_integer = pulp.LpVariable.dicts("PBO_Selector",loan_list,0,1,pulp.LpBinary) # binary indicator for allocation to MM


# SET THE PULP PROBLEM
prob = pulp.LpProblem('Maximum_Profit',pulp.LpMaximize)


total_NEC = pulp.lpSum([NEC_MM[i] * mm_integer[i] + NEC_PBO[i] * pbo_integer[i] for i in loan_list]) # for v3
#total_NEC = pulp.lpSum([NEC_MM[i] * mm_integer[i] - NEC_PBO[i] * mm_integer[i] for i in loan_list]) + pulp.lpSum([NEC_PBO[i] * pbo_integer[i] - NEC_MM[i] * pbo_integer[i] for i in loan_list]) #for v4 nec diff

# OBJECTIVE to maximize
prob += total_NEC
#print(prob)

# CONSTRAINTS
# Constraints for MM trade size
prob += pulp.lpSum(TOTAL_MM[i] * mm_integer[i] for i in TOTAL_MM) <= 200.0e6 #, "MM_Maximum"
#prob += pulp.lpSum(TOTAL_MM[i] * mm_integer[i] for i in TOTAL_MM) >= 199.0e6 #, "MM_Minimum"


# Constraints for PBO trade size
prob += pulp.lpSum(TOTAL_PBO[i] * pbo_integer[i] for i in TOTAL_PBO) <= 135.0e6 #, "PBO_Maximum" 
#prob += pulp.lpSum(TOTAL_PBO[i] * pbo_integer[i] for i in TOTAL_PBO) >= 134.0e6 #, "PBO_Minimum"


# Constraints for not double-allocating 
for i in loan_list:
    prob += mm_integer[i] + pbo_integer[i] <= 1


# Constraints for profit for each investor  
for i in loan_list:
    prob += NEC_MM[i]*mm_integer[i] >=0
    prob += NEC_PBO[i]*pbo_integer[i] >=0

    
# EXPORT THE PULP OUTPUT
# WRITE LP PROBLEM TO A FILE # The problem data is written to an .lp file
prob.writeLP(optimizer_path + '\\Optimize_prob_data.lp')

# SOLVE THE OPTIMIZATION
prob.solve()

print("Status: ",pulp.LpStatus[prob.status]) #prints Infeasible


# POPULATE THE RESULTS FOR EACH VARIABLE INTO LISTS
mylist = []
myvalue = []
myvalue_orig_var = []


for variable in prob.variables():
    mylist.append(variable.name[-10:])
    myvalue.append(variable.varValue)
    myvalue_orig_var.append(variable.name)


# INSERT THE VARIABLE LIST RESULTS INTO DATAFRAMES
# import pandas as pd # already imported the pandas library above

df_sol = pd.DataFrame({'LoanId': mylist,
                              'Flag': myvalue,
                              'Original_Variable': myvalue_orig_var})

df_sol.set_index('LoanId', inplace=True)
df_sol.to_csv(optimizer_path + '\\Optimizer_sol_loannums.csv') #index=False

# MAYBE ADD SEPARATE DF FOR EACH INVESTOR (SINCE LOAN NUMBERS ARE DUPLICATED FOR EACH INVESTOR)
# MAYBE ADD UPDATE TO ELIGIBILITY DATAFRAME
