# -*- coding: utf-8 -*-
"""
Created on Thu Oct 17 17:02:37 2019

@author: jboyce

NOTES ON GOAL OF SCRIPT:
    
USER REQUIREMENTS:
    
    
REVISION HISTORY:

    """

import pandas as pd
import numpy as np
import pyodbc
import time

#clock starts to time how long the df import takes
#start_tm = time.clock()

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
#SETTINGS - Set path of file to import
#############################################
#Path and filename of file to import
filepath_import = r'M:\Capital Markets\Users\Johnathan Boyce\Misc\Programming\Python\EBO ML-AI-Probability Project\FAV (Gang) Data'
filename_import = '\EBO.EBOMaster.csv'
pathandfile_import = filepath_import + filename_import

#############################################
#SETTINGS - Set path of file to export
#############################################
#Path and filename of file to export
filepath_export = r'M:\Capital Markets\Users\Johnathan Boyce\Misc\Programming\Python\EBO ML-AI-Probability Project\FAV (Gang) Data'
filename_export_decision_tree = '\decision-tree_probabilities' #'.xlsx' need to add xls
pathandfile_export_decision_tree = filepath_export + filename_export_decision_tree

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

#Import the file into a dataframe
df = pd.read_csv(pathandfile_import).set_index('LoanId')


#NOTE ON DF INDEX BELOW
#sql_conn.close()

#------------------------------------------------------------------------------------------------------------
#change index to object / string dtype (to match df_final index dtype)
#df_trendix_out.index = df_trendix_out.index.astype(np.str) #12/13/2019 THIS INDEX DTYPE CHANGE TO OBJECT OR STRING APPEARS TO WORK

print("Top 5 rows\n" + str(df.head()) +"\n")
print("\nNumber of rows: {}, Number of columns: {}".format(df.shape[0], df.shape[1]))
print("DF Index datatype is:",df.index.dtype) #or can do type(df.index)
print("\n")

#Create scrub version of initial df (so we can go back to it if necessary)
df_scrub = df

#ADD GROUPBY PIVOT HERE TO SHOW LoanType counts

#check for number of nulls for each column
print("Total Count of Null Records \n" + str(df_scrub.isna().count()) + "\n")


#check the proportion of records that are missing
print("\nTotal Percentage of Null Records \n" + str(df_scrub.isna().mean()) + "\n") #need to fix this to count or % of total using count or upb

#df datatype info summary
print("\nMiscellaneous Info on the Dataframe (Scrub):")
df_scrub.info()

#Describe certain column fields
print("\nAverage Original Borrower Score\n"+ str(df_scrub.OriginalBorrowerCreditScore.describe()))

#Print the average FICO by LoTypeId
print("\nAverage Score (by LoTypeId)\n" + str(df_scrub.groupby(by="LoTypeId").mean()["OriginalBorrowerCreditScore"]))

#add categorical and non-categorical dfs
#df_scrub_cat=df_scrub[df_scrub[]]

#############################################
#SET LIST OF COLUMNS TO DROP, REMOVE NULLS, AND SET DATATYPES
#############################################
#create list of features/columns to drop
drop_list=['LoTypeId'
    ,'PropertyTypeId'
    ,'LoanPurposeCodeId'
    ,'OriginalBorrowerCreditScore'
    ,'BeginningLossMitStatus'
    ,'FicoGroup'
    ,'proptype'
    ,'Occ'] #just added removal of CRTFlag and ClientID

#drop features / columns not needed
df_scrub_drpd = df_scrub.drop(drop_list, axis=1)

#confirm dropping list worked
print(df_scrub_drpd.head())


#check remaining shape
print("DROPPED SHAPE: ", df_scrub_drpd.shape)


#check remaining list of columns
print("COLUMNS (AFTER DROPPED): ", df_scrub_drpd.columns)


#remove NULL rows
df_scrub_drpd = df_scrub_drpd.dropna()

#double check what's left
print("DROPPED NA SHAPE: ", df_scrub_drpd.shape) #might have to filter out datetime rows

#check df size before and after dtype change
print("Dataframe filesize (before astype):", df_scrub_drpd.memory_usage()) #.sum() #mem_usage(column_name)?

#set datatypes...#category, float32, int32, to_datetime
df_scrub_drpd = df_scrub_drpd.astype({'MEPeriod':'int32'
    ,'PropertyState': 'category'
    ,'Units': 'category'
    ,'ProductLineCodeId': 'category'
    ,'InvestorId': 'category'
    ,'CurrentPrincipalBalanceAmt': 'float32'
    ,'CurrentInterestRate': 'float32'
    ,'Rate_at_D90': 'category'
    ,'OriginalInterestRt': 'float32'
    ,'OriginalLoanToValueRatio': 'float32'
    ,'OriginalPrincipalBalanceAmt': 'int32'
    ,'PropertyTypeDesc': 'category'
    ,'ProductLineCodeDesc': 'category'
    ,'InvestorName': 'category'
    ,'LotypeDesc': 'category'
    ,'LoanPurposeCodeDesc': 'category'
    ,'OccupancyCodeDesc': 'category'
    ,'cltv_trendix': 'float32'
    ,'cltv_avm': 'float32'
    ,'Dt': 'int32'
    ,'Flow': 'category'
    ,'OrigFlow': 'category'
    ,'OrigDelinquentPaymentCount': 'int32'
    ,'LastDelinquentPaymentCount': 'int32'
    ,'PaymentMade': 'int32'
    ,'upb': 'category'
    ,'CLTV_Trendix_Grp': 'category'
    ,'CLTV_AVM_Grp': 'category'
    ,'LTV': 'category'
    ,'OrigDQ': 'category'
    ,'Loan_Purpose': 'category'
    ,'trial': 'category'
    ,'maxdq': 'int32'
    ,'MaxDQgrp': 'category'
    ,'DLQ_diff': 'int32'
    ,'DLQ_Diff_Grp': 'category'})

print("Dataframe filesize (after astype):", df_scrub_drpd.memory_usage()) #.sum() #mem_usage(column_name)?

#set datatypes in order to reduce df size
#convert to datetime and confirm converted field
#stackoverflow solution: data.iloc[:, 7:12] = data.iloc[:, 7:12].apply(pd.to_datetime, errors='coerce')
#waterfall solution: df_final.loc[df_final['FINAL_WATERFALL_APPROACH'] == 'Pull Trendix', 'FINAL_WATERFALL_VALUE'] = df_final.CUR_HOUSE_PRICE
# OLD - df_scrub_drpd = pd.to_datetime(df_scrub_drpd[df_scrub_drpd['LoanClosingDt','OrigNextPaymentDueDt','LastNextPaymentDueDt']])

#DOESN'T WORK: df_scrub_drpd.loc[df_scrub_drpd['LoanClosingDt','OrigNextPaymentDueDt','LastNextPaymentDueDt']] = df_scrub_drpd.loc[df_scrub_drpd['LoanClosingDt','OrigNextPaymentDueDt','LastNextPaymentDueDt']].apply(pd.to_datetime, errors='coerce') #stackoverflow solution
#df_scrub_drpd = pd.to_datetime(df_scrub_drpd.loc[df_scrub_drpd[['LoanClosingDt','OrigNextPaymentDueDt','LastNextPaymentDueDt']]],yearFirst=True,errors='coerce') #.apply(pd.to_datetime, errors='coerce')
#STILL NEED TO CONVERT TO DATETIME...CAN MOVE FORWARD FOR NOW

#print("Dataframe filesize (after to_datetime):", df_scrub_drpd.memory_usage()) #.sum()

#############################################
#SET TARGET VALUE COLUMNS
#############################################
#df3.loc[df3['b'] == 7, 'test'] = 1
df_scrub_drpd.loc[df_scrub_drpd['Flow'] == '0-DILSS', 'Target_y']       = 0
df_scrub_drpd.loc[df_scrub_drpd['Flow'] == '1-Prepay', 'Target_y']      = 1
df_scrub_drpd.loc[df_scrub_drpd['Flow'] == '2-Mod', 'Target_y']         = 2
df_scrub_drpd.loc[df_scrub_drpd['Flow'] == '3-Cure', 'Target_y']        = 3
df_scrub_drpd.loc[df_scrub_drpd['Flow'] == '4-Dlq-Pay', 'Target_y']     = 4
df_scrub_drpd.loc[df_scrub_drpd['Flow'] == '5-Dlq-Nopay', 'Target_y']   = 5

df_target = pd.DataFrame(df_scrub_drpd['Target_y'].astype('int32')) #.set_index('LoanId')
#print(df_target.head())

#############################################
#SET LIST OF COLUMNS TO DROP, REMOVE NULLS, AND SET DATATYPES
#############################################
#perform onehot encoding
#df_onehot = df_scrub_drpd.select_dtypes()
df_scrub_drpd_onehot = pd.get_dummies(df_scrub_drpd[['PropertyState','ProductLineCodeId','PropertyTypeDesc','ProductLineCodeDesc','LotypeDesc','LoanPurposeCodeDesc','OccupancyCodeDesc','OrigDQ','Loan_Purpose','MaxDQgrp','DLQ_Diff_Grp','trial']]
                                      ,prefix=['PropertyState','ProductLineCodeId','PropertyTypeDesc','ProductLineCodeDesc','LotypeDesc','LoanPurposeCodeDesc','OccupancyCodeDesc','OrigDQ','Loan_Purpose','MaxDQgrp','DLQ_Diff_Grp','trial']
                                      ,prefix_sep="_")
#'Flow','OrigFlow', 'Flow','OrigFlow',
print(df_scrub_drpd_onehot.shape)
print(df_scrub_drpd_onehot.head())
print(df_scrub_drpd_onehot.columns)


#delete the original columns for one-hot (so only binary is left)
post_onehot_drop_list = ['PropertyState','ProductLineCodeId','PropertyTypeDesc','ProductLineCodeDesc','LotypeDesc','LoanPurposeCodeDesc','OccupancyCodeDesc','Flow','OrigFlow','OrigDQ','Loan_Purpose','MaxDQgrp','DLQ_Diff_Grp','trial']

#drop original onehot columns
df_scrub_drpd_after_onehot = df_scrub_drpd.drop(post_onehot_drop_list, axis=1)

#drop other object dtype columns
post_onehot_drop_list2 = ['Units'	,'InvestorId'	,'Rate_at_D90'	,'InvestorName'	,'upb'	,'CLTV_Trendix_Grp'	,'CLTV_AVM_Grp'	,'LTV']
df_scrub_drpd_after_onehot = df_scrub_drpd_after_onehot.drop(post_onehot_drop_list2, axis=1)

#drop misc remaining object dtypes as cleanup (these are actually date-related fields)
post_onehot_drop_list3 = ['LoanClosingDt'	,'OrigNextPaymentDueDt'	,'LastNextPaymentDueDt']
df_scrub_drpd_after_onehot = df_scrub_drpd_after_onehot.drop(post_onehot_drop_list3, axis=1)

#drop target column (not scaling this field)
post_onehot_drop_list4 = ['Target_y']
df_scrub_drpd_after_onehot = df_scrub_drpd_after_onehot.drop(post_onehot_drop_list4, axis=1)

print(df_scrub_drpd_after_onehot.shape) #AFTER COLUMN CLEANUP: float32(6), int32(8), uint8(117) #26 columns remaining ties to 47 orig columns - 13onehot -8null = 26



#############################################
#MERGE DATAFRAMES USING INDEX (LOAN NUMBERS)
#############################################
#add one-hot to original dataframe
df_post_onehot = pd.merge(df_scrub_drpd_after_onehot, df_scrub_drpd_onehot, left_index=True,right_index=True,how='left')

#check head and shape
#print("Post-onehot shape:",df_post_onehot.shape)
#print(df_post_onehot.head())

print(df_post_onehot.info()) #dtypes: category(9), float32(6), int32(8), object(3), uint8(113)...might  have to remove categorical columns that remain...upb/ltv buckets


#############################################
#SCALE / NORMALIZE
#############################################

#scale columns (including onehot) and preview a couple of rows
#import numpy as np
from sklearn.preprocessing import StandardScaler

X1 = df_post_onehot.values #[:,1:]  skipped BidDate since values cannot be timestamp
X1 = np.nan_to_num(X1)
cluster_dataset = StandardScaler().fit_transform(X1)


print("Sample of scaled data:", cluster_dataset[:2])


#############################################
#RECREATE DATAFRAME USING SCALED / NORMALIZED VALUES
#############################################
#df_post_onehot
#re-create the df (without bid date) to include the scaled/transformed data
df_post_onehot_scaled = pd.DataFrame(
    cluster_dataset,
     index=list(df_post_onehot.index),
     columns=df_post_onehot.columns)

df_post_onehot_scaled.index.rename('LoanId', inplace=True)
print(df_post_onehot_scaled.head()) #WORKED...NOW NEED TO ADD ONE HOT AFTER TRANSFORMED



#############################################
#MODELING
#############################################
# ---
# # Modeling
# ---

# #### Let's define our X and y for training and testing

#############################################
#SET Y (NOT NORMALIZED)
#############################################

y = df_target['Target_y']
#print(y [0:5])

#############################################
#SET VALUES FROM X (SCALED/NORMALIZED)
#############################################

X = df_post_onehot_scaled #.values
print(X[:2])

#set indexes list prior to train test splits
indices = X.index

#############################################
#SET TRAIN AND TEST SPLITS
#############################################
from sklearn.model_selection import train_test_split

X_train, X_test, y_train, y_test, indices_train, indices_test = train_test_split(X, y, indices, test_size=0.3, random_state=4)
print ('Train set:', X_train.shape,  y_train.shape)
print ('Train set (indices):', indices_train)
print ('Test set:', X_test.shape,  y_test.shape)
print ('Test set (indices): ', indices_test)


#############################################
#MODELING - DECISION TREE
#############################################
# ---
# ### Decision Tree
# 

# #### Modeling
#from sklearn.tree import DecisionTreeClassifier

#print(dTree) # it shows the default parameters

#############################################
#MODELING - ITERATE THROUGH MAX_DEPTHS FOR DECISION TREES
#############################################
#iterate through max_depth to find optimal depth to minimize entropy
#first

### Create Function to iterate through Decision Tree max_depth to obtain best accuracy
def dtree_max_depth_iterator():
    depth_list=[4,6,8,12]
    dtree_criterion="entropy"
    dtree_acc_score=[]
    dtree_acc_dict={}
    #ADDED
    predTree_max=[]
    
    #import decision tree libraries
    from sklearn.tree import DecisionTreeClassifier
    from sklearn import metrics
    import matplotlib.pyplot as plt
    import operator
    
    
    for i in range(0,len(depth_list)):

        #set decision tree
        dTree = DecisionTreeClassifier(criterion=dtree_criterion, max_depth = depth_list[i])
        print("i = "+str(i)) 
        #print("Length of list is: "+str(len(depth_list))) #used this to print the iteration number during testing
        
        #fit the training set
        dTree.fit(X_train,y_train)
        
        
        #prediction
        predTree = dTree.predict(X_test)
        
        
        #get the accuracy score
        dtree_acc_score.append(round(metrics.accuracy_score(y_test, predTree),6))
        dtree_acc_dict[depth_list[i]] = dtree_acc_score[i]
        #ADDED
        predTree_max = DecisionTreeClassifier(criterion=dtree_criterion, max_depth = max(dtree_acc_dict.items(), key=operator.itemgetter(1))[0]).fit(X_train,y_train).predict(X_test)
                
        print("Max Depth is: {}; Accuracy Score is: {}".format(depth_list[i], dtree_acc_dict[depth_list[i]]))
        print(dtree_acc_dict)
        print("Completed {} runs".format(i+1))
        print("\n")
        print("Max_Depth for Best Accuracy Is:"+str(max(dtree_acc_dict.items(), key=operator.itemgetter(1))[0])) #ADDED THIS LATER FOR MAX ACCURACY - CHANGE ITEMGETTER TO ALSO GET ACCURACY
        print("predTree_max is:",predTree_max)
        print("DecisionTrees's Accuracy: ", round(metrics.accuracy_score(y_test, predTree_max),4))
        
    return dtree_acc_score
    return dtree_acc_dict
    return predTree_max


    
dtree_max_depth_iterator()


#############################################
#RUN DECISION TREE USING SKLEARN
#############################################
from sklearn.tree import DecisionTreeClassifier
from sklearn import metrics
import matplotlib.pyplot as plt
#import operator
from sklearn.metrics import classification_report, confusion_matrix

#Decision Tree with max_depth =4
dTree4 = DecisionTreeClassifier(criterion="entropy", max_depth = 4)
dTree4.fit(X_train,y_train)
predTree4 = dTree4.predict(X_test)
dt4_probabilities = dTree4.predict_proba(X_test)


#Decision Tree with max_depth =12
dTree12 = DecisionTreeClassifier(criterion="entropy", max_depth = 12)
dTree12.fit(X_train,y_train)
predTree12 = dTree12.predict(X_test)
dt12_probabilities = dTree12.predict_proba(X_test)

print("DecisionTrees's Accuracy (post script) (4 max_depth): ", round(metrics.accuracy_score(y_test, predTree4),4))
print("Probabilities (4 max_depth):" , dTree4.predict_proba(X_test))
print("\nClassification Report - Decision Tree (4 max_depth) \n \n  {}".format(classification_report(y_test, predTree4)))

print("DecisionTrees's Accuracy (post script) (12 max_depth): ", round(metrics.accuracy_score(y_test, predTree12),4))
print("Probabilities (12 max_depth):" , dTree12.predict_proba(X_test))
print("\nClassification Report - Decision Tree (12 max_depth) \n \n  {}".format(classification_report(y_test, predTree12)))

#CREATE A DF FROM PROBABILITIES
# df_tempfix = pd.DataFrame(
#     cluster_dataset_1,
#      index=list(onehot_temp.index),
#      columns=onehot_temp.columns)
# df_tempfix.head()

#set target probabilities column names
target_probabilities_column_names = ['0-DILSS', '1-Prepay', '2-Mod', '3-Cure', '4-Dlq-Pay', '5-Dlq-Nopay']

#create dataframe of probabilities resulting from max_depth = 4
df_DT4 = pd.DataFrame(
    dt4_probabilities,
      index=indices_test,
      columns=target_probabilities_column_names)

#create dataframe of probabilities resulting from max_depth = 12
df_DT12 = pd.DataFrame(
    dt12_probabilities,
      index=indices_test,
      columns=target_probabilities_column_names)

#set target column = to 
#ADDED THIS FOR EXPORT
df_DT4['Target_y'] = df_target['Target_y']
df_DT4['Predicted_y'] = predTree4

df_DT12['Target_y'] = df_target['Target_y']
df_DT12['Predicted_y'] = predTree12

print("\nDecision Tree Head (max_depth = 4): \n{}".format(df_DT4.head()))
print("\nDecision Tree Head (max_depth = 12): \n{}".format(df_DT12.head()))


#export dataframe results to excel
df_DT4.to_excel(filepath_export + filename_export_decision_tree + "4.xlsx")
df_DT12.to_excel(filepath_export + filename_export_decision_tree + "12.xlsx")


#TEMP PRINT OF PREDTREE4
print(predTree4[:5])
print(predTree12[:5])

#dTree = DecisionTreeClassifier(criterion="entropy", max_depth = 4)
#dTree.fit(X_train,y_train)



#TO COMPARE MAX_DEPTH PROBABILITIES
#USE THIS TO COMBINE LOANNUMS, PROBABILITIES AND COLUMN HEADERS
#THEN EXPORT FOR REFERENCE
# df_tempfix = pd.DataFrame(
#     cluster_dataset_1,
#      index=list(onehot_temp.index),
#      columns=onehot_temp.columns)
# df_tempfix.head()

########################################
#predTree = dTree_max.predict(X_test)
#name 'dTree_max' is not defined


# #### Evaluation
from sklearn import metrics
import matplotlib.pyplot as plt

#print("DecisionTrees's Accuracy: ", round(metrics.accuracy_score(y_test, predTree_max),4))

#name 'dTree_max' is not defined

#Decision Tree
#DT_yhat = DT_model.predict(X_test)
#print("DT Jaccard index: %.3f" % jaccard_similarity_score(y_test, DT_yhat))
#print("DT F1-score: %.3f" % f1_score(y_test, DT_yhat, average='weighted') )

#############################################
#VISUALIZATION: DECISION TREE
#############################################
# <div id="visualization">
#     <h2>Visualization</h2>
#     Lets visualize the tree
# </div>

# Notice: You might need to uncomment and install the pydotplus and graphviz libraries if you have not installed these before
#get_ipython().system('conda install -c conda-forge pydotplus -y')
#get_ipython().system('conda install -c conda-forge python-graphviz -y')

"""
from sklearn.externals.six import StringIO
import pydotplus
import matplotlib.image as mpimg
from sklearn import tree
get_ipython().run_line_magic('matplotlib', 'inline')

#>>>>>>>>>>>>>>>>>>>>  MIGHT NEED TO CHANGE TO Y_TEST INSTEAD OF Y_TRAIN  <<<<<<<<<<<<<<<<<<<<<<<<<
dot_data = StringIO()
filename = "ebo_outcome_tree.png"
featureNames = df_post_onehot_scaled.columns #[0:5] #FIND COLUMN NAMES
targetNames = df_target['Target_y'].unique().tolist() #df_target['Target_y']
out=tree.export_graphviz(dTree,feature_names=featureNames, out_file=dot_data, class_names= str(np.unique(y_train)), filled=True,  special_characters=True,rotate=False)  #MIGHT HAVE TO CHANGE TO Y_TEST #HAD TO ADD STR FOR Y_TRAIN 
graph = pydotplus.graph_from_dot_data(dot_data.getvalue())  
graph.write_png(filename) #THIS LINE IS BLOWING UP WITH THE FOLLOWING ERROR--> AttributeError: 'NoneType' object has no attribute 'write_png' #maybe changed to write_gif
img = mpimg.imread(filename)
plt.figure(figsize=(100, 200))
plt.imshow(img,interpolation='nearest')
"""

#^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
#-------------LEFT OFF HERE-------------


#df_scrub['BidDate'] = pd.to_datetime(df_scrub['BidDate'])
#df_scrub.info()



"""
#############################################
#PLOT CORRELATION MATRIX
#############################################
#got this correlation matrix from stackoverflow (referenced in Google Sheet)
#Need to scale/normalize prior to running correlation matrix (this is just a placeholder for now)
import matplotlib.pyplot as plt

f = plt.figure(figsize=(20, 34))
plt.matshow(df_scrub.corr(), fignum=f.number)
plt.xticks(range(df_scrub.shape[1]), df_scrub.columns, fontsize=14, rotation=90)
plt.yticks(range(df_scrub.shape[1]), df_scrub.columns, fontsize=14)
cb = plt.colorbar()
cb.ax.tick_params(labelsize=14)
plt.title('Correlation Matrix', fontsize=16)
plt.show() #added this and it displayed correlation matrix
"""

#ADD ML PROJECT TEXT HERE:
# # Data Preparation and Understanding
# ---

#print(pd.pivot_table(df_scrub,'CurrentPrincipalBalanceAmt',['Units'],aggfunc=np.sum))


"""
#df_scrub_drpd.index #not needed at this point since this was to check a pop that didn't have loannums


#perform logistic regression on train and test sets
from sklearn.model_selection import train_test_split

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=4)
print ('Train set:', X_train.shape,  y_train.shape)
print ('Test set:', X_test.shape,  y_test.shape)


# # Logistic Regression

# In[35]:


from sklearn.linear_model import LogisticRegression
from sklearn.metrics import confusion_matrix
LR = LogisticRegression(C=0.01, solver='liblinear').fit(X_train,y_train)
LR


# In[36]:


yhat_LR0 = LR.predict(X_test)
yhat_LR0


# In[37]:


yhat_prob_LR0 = LR.predict_proba(X_test)
yhat_prob_LR0


# In[51]:


from sklearn.metrics import jaccard_similarity_score, log_loss, f1_score

print("Log Loss is: {}".format(log_loss(y_test, yhat_prob_LR0)))
print("Jaccard Similarity Score is: {}".format(jaccard_similarity_score(y_test, yhat_LR0)))


# #### Plot the Confusion Matrix

# In[39]:


from sklearn.metrics import classification_report, confusion_matrix
import matplotlib.pyplot as plt
import itertools
def plot_confusion_matrix(cm, classes,
                         normalize=False,
                         title='Confusion matrix',
                         cmap=plt.cm.Blues):
"""
#This function prints and plots the confusion matrix.
#Normalization can be applied by setting `normalize=True`.
"""
   if normalize:
       cm = cm.astype('float') / cm.sum(axis=1)[:, np.newaxis]
       print("Normalized confusion matrix")
   else:
       print('Confusion matrix, without normalization')

   print(cm)

   plt.imshow(cm, interpolation='nearest', cmap=cmap)
   plt.title(title)
   plt.colorbar()
   tick_marks = np.arange(len(classes))
   plt.xticks(tick_marks, classes, rotation=45)
   plt.yticks(tick_marks, classes)

   fmt = '.2f' if normalize else 'd'
   thresh = cm.max() / 2.
   for i, j in itertools.product(range(cm.shape[0]), range(cm.shape[1])):
       plt.text(j, i, format(cm[i, j], fmt),
                horizontalalignment="center",
                color="white" if cm[i, j] > thresh else "black")

   plt.tight_layout()
   plt.ylabel('True label')
   plt.xlabel('Predicted label')
print(confusion_matrix(y_test, yhat_LR0, labels=[1,0])) #COULD CHANGE YHAT TO YHAT_LR


# In[40]:


# Compute confusion matrix - Logistic Regression
cnf_matrix = confusion_matrix(y_test, yhat_LR0, labels=[1,0])
np.set_printoptions(precision=2)


# Plot non-normalized confusion matrix
plt.figure()
plot_confusion_matrix(cnf_matrix, classes=['Win_Bid=1','Win_Bid=0'],normalize= False,  title='Confusion matrix')


# In[41]:


print("Classification Report - Logistic Regression \n \n  {}".format(classification_report(y_test, yhat_LR0)))


# ---
# ### Support Vector Machine (SVM)
# #### Test each non-linear SVM kernel type (Polynomial, Radial based function (RBF), and Sigmoid)
# 

# ##### Radial based function (RBF)

# ### WORK IN PROGRESS FOR REMAINING!!! CONSIDER FUNCTION TO ITERATE THROUGH

# In[49]:


from sklearn.tree import DecisionTreeClassifier
DT_model = DecisionTreeClassifier(criterion="entropy", max_depth = 8) #work on iterating through this
DT_model.fit(X_train,y_train)
DT_model


# In[52]:


#Decision Tree
DT_yhat = DT_model.predict(X_test)
print("DT Jaccard index: %.3f" % jaccard_similarity_score(y_test, DT_yhat))
print("DT F1-score: %.3f" % f1_score(y_test, DT_yhat, average='weighted') )


# In[53]:


#Support Vector Machine
from sklearn import svm

SVM_model = svm.SVC(kernel='rbf', gamma='auto') #added 'auto' to gamma based on warning
SVM_model.fit(X_train, y_train)

SVM_yhat = SVM_model.predict(X_test) 
print("SVM Jaccard index: %.3f" % jaccard_similarity_score(y_test, SVM_yhat))
print("SVM F1-score: %.3f" % f1_score(y_test, SVM_yhat, average='weighted') )


# In[54]:


#Logistic Regression
LR_model = LogisticRegression(C=0.01, solver='liblinear').fit(X_train,y_train)

LR_yhat = LR_model.predict(X_test)
LR_yhat_prob = LR_model.predict_proba(X_test)
print("LR Jaccard index: %.3f" % jaccard_similarity_score(y_test, LR_yhat))
print("LR F1-score: %.3f" % f1_score(y_test, LR_yhat, average='weighted') )
print("LR LogLoss: %.3f" % log_loss(y_test, LR_yhat_prob))


# #### STOP HERE FOR NOW

# In[55]:


#KNN
knn_yhat = kNN_model.predict(test_X)
print("KNN Jaccard index: %.3f" % jaccard_similarity_score(test_y, knn_yhat))
print("KNN F1-score: %.3f" % f1_score(test_y, knn_yhat, average='weighted') )


# # Report
# You should be able to report the accuracy of the built model using different evaluation metrics:

# | Algorithm          | Jaccard | F1-score | LogLoss |
# |--------------------|---------|----------|---------|
# | KNN                | 0.67    | 0.63     | NA      |
# | Decision Tree      | 0.72    | 0.74     | NA      |
# | SVM                | 0.80    | 0.76     | NA      |
# | LogisticRegression | 0.74    | 0.66     | 0.57    |

# # K Nearest Neighbor (KNN)
# Notice: You should find the best k to build the model with the best accuracy.  
# **warning:** You should not use the __loan_test.csv__ for finding the best k, however, you can split your train_loan.csv into train and test to find the best __k__.

# In[36]:


# Modeling
#WORK IN PROGRESS...ITERATE THROUGH K EVENTUALLY
from sklearn.neighbors import KNeighborsClassifier
k = 3

#Train Model and Predict  
kNN_model_0 = KNeighborsClassifier(n_neighbors=k).fit(X_train,y_train)
kNN_model_0


# In[37]:


# just for sanity chaeck
yhat_KNN0 = kNN_model_0.predict(X_test)
yhat_KNN0[0:5]


# In[38]:


# Best k
Ks=15
mean_acc=np.zeros((Ks-1))
std_acc=np.zeros((Ks-1))
ConfustionMx=[];
for n in range(1,Ks):
    
    #Train Model and Predict  
    kNN_model = KNeighborsClassifier(n_neighbors=n).fit(X_train,y_train)
    yhat_kNN = kNN_model.predict(X_test)
    
    
    mean_acc[n-1]=np.mean(yhat_kNN==y_test);
    
    std_acc[n-1]=np.std(yhat_kNN==y_test)/np.sqrt(yhat_kNN.shape[0])
mean_acc


# In[39]:


# Building the model again, using k=7
from sklearn.neighbors import KNeighborsClassifier
k = 7
#Train Model and Predict  
kNN_model2 = KNeighborsClassifier(n_neighbors=k).fit(X_train,y_train)
kNN_model2


# In[70]:


#df['dayofweek'] = df['effective_date'].dt.dayofweek



"""


#print(df_scrub_desc)

#update FINAL_WATERFALL_VALUE with CUR_HOUSE_PRICE - using filter
#df_final.loc[df_final['FINAL_WATERFALL_APPROACH'] == 'Pull Trendix', 'FINAL_WATERFALL_VALUE'] = df_final.CUR_HOUSE_PRICE


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