#!/usr/bin/env python
# coding: utf-8

# # Error Analysis on 8760 profiles (22 profiles)

# In[177]:

print('start error analysis')
print()

#importing packages needed for analysis
import os
import pandas as pd
#import numpy as np
#import math
#from pandas import DataFrame

path = os.getcwd()
#print(path)

#this code creates an output directory in the parent director, if one does not exist yet
#Note: this is where all of the output files will be written, since outputs are large this saves space in git
path = os.getcwd()
parent = os.path.dirname(path)
outputs_dir = parent+'\outputs\error_analysis'
if not os.path.exists(outputs_dir):
    os.makedirs(outputs_dir)
print('output files are written out in parent directory: '+outputs_dir)

##UNCOMMENT WHICH PROFILE BEING ANALYZED 
#x = 'load'
#x2 = 'Load'

#x = 'solar'
#x2 = 'TRG_Avg'

x = 'wind'
x2 = 'TRG_Avg'

outputs_dir_x = parent+'\outputs\error_analysis/'+x
if not os.path.exists(outputs_dir_x):
    os.makedirs(outputs_dir_x)

print(x, 'setup')
print()

# In[178]:


# create DFs for error analysis
# DF to use for regional error analysis, like RMSE and number of representative hours 
long = pd.read_csv('../outputs/'+x+'_long_format.csv')
regions = long['Region'].unique()
regions = pd.DataFrame(regions).rename(columns={0:'Region'})
print(len(regions))

reg_hr = long[['Region','HOY']].copy()
reg_hr['reg_hr'] = reg_hr['Region'] + '-' + reg_hr['HOY'].astype(str)
RH_list = reg_hr['reg_hr'].unique()
RH_list = pd.DataFrame(RH_list).rename(columns={0:'reg_hr'})
print(len(RH_list)/8760)
#print(RH_list[0:3])

filelist = []
#print(parent+'/outputs/'+x)
files = os.listdir(parent+'/outputs/'+x)
for f in files:
    filelist.append(f)
#print(len(filelist))
#print(filelist)
print()


# ## Loop and create error files 

# In[179]:


rep_hours = regions
reg_RMSE = regions
profile_diff = RH_list
prof_RMSE = {}
import time;
ts = time.time()
#print(ts)

print('start of loop... wait for it...')
for i in filelist:
    y=len(x)+6
    n=i[y:-4]
    
    stat = pd.read_csv('../outputs/'+x+'/'+i)
    stat = stat[['Region','Season','Month','DOY','HOY','Hour_Tot',x2,'Avg']]
    stat['Diff'] = stat[x2] - stat['Avg'] 
    #uncomment out line below to generate individual files, significantly adds to solve time
    #stat.to_csv('../outputs/error_analysis/'+x+'/diff_'+i)
    
    #note: thihs is alt approach to above code, faster to solve, but need to update plots
    diff = stat[['Region','HOY','Diff']].copy().rename(columns={'Diff':n})
    diff['reg_hr'] = diff['Region'] + '-' + diff['HOY'].astype(str)
    diff = diff.drop(columns={'Region','HOY'})
    profile_diff = pd.merge(profile_diff, diff, on='reg_hr', how='left')
    
    # regional RMSE
    stat['Square'] = stat['Diff']**2
    stat_reg = stat.groupby('Region',as_index=False).agg({'Square' : sum})
    stat_reg = stat_reg.rename(columns={'Square':'Square_Sum'})
    stat_reg['Mean'] = stat_reg['Square_Sum'] / 8760 
    stat_reg[n] = stat_reg['Mean']**(1/2)
    stat_reg = stat_reg.drop(columns={'Square_Sum','Mean'})
    reg_RMSE = pd.merge(reg_RMSE, stat_reg, on='Region', how='left')
    
    # profile RMSE
    stat2 = stat.copy()
    stat2 = stat2.rename(columns={'Square':'RMSE'})
    stat2['RMSE'] = stat2['Diff']**2
    stat2 = stat2.agg({'RMSE' : sum})
    stat2 = stat2 / (8760*len(regions)) 
    stat2[n] = stat2**(1/2)
    stat2[n] = stat2[n].values
    prof_RMSE.update({n : (stat2[n])})

    #print(x, n, 'error calculated')

print('end of loop')
print()
tf = time.time()
#print(tf)
#print((tf-ts)/60,'minutes to solve loop')

#profile_diff.to_csv('../outputs/error_analysis/'+x+'_'+'profile_diff.csv')
reg_RMSE.to_csv('../outputs/error_analysis/'+x+'_'+'regional_RMSE.csv')
profile_df = pd.DataFrame.from_dict(prof_RMSE,orient='index').rename(columns={0:'RMSE'})
profile_df.to_csv('../outputs/error_analysis/'+x+'_'+'profile_RMSE.csv')

print('end of error analysis')


# In[ ]:




