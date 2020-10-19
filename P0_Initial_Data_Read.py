#!/usr/bin/env python
# coding: utf-8

# # Load Data Prep

# In[62]:


#importing packages needed for analysis
import os
import numpy as np
import pandas as pd
import math
from pandas import DataFrame
from itertools import cycle
pd.set_option('display.max_rows',500)

path = os.getcwd()
#print(path)

#this code creates an output directory in the parent director, if one does not exist yet
#Note: sets where all of the output files will be written, since outputs files are large
path = os.getcwd()
parent = os.path.dirname(path)
outputs_dir = parent+'\outputs'
if not os.path.exists(outputs_dir):
    os.makedirs(outputs_dir)
print('output files are written out in parent directory: '+outputs_dir)
print()
load_raw = pd.read_csv('inputs/load_duration_curves_raw_data.csv')
print(load_raw.head(2))
print()
print('number of rows in dataset =', load_raw.shape[0])


# In[63]:


#Organizing regional data

#create temporary copy to make changes on
load_org = load_raw.copy()
print('number of rows in dataset (including CN) =',load_org.shape[0])

#Regional IDs
unique_r = pd.Series(load_org['Region'].unique()).dropna()
rl = unique_r.str.split("_",n=1,expand=True)
rl[2] = unique_r
#print(rl)
print('number of regions in dataset (including CN) =',unique_r.shape[0])

#Cleaning up the empty subgroups
#print(rl[rl.isna().any(axis=1)])
rl.loc[rl[0] == 'NENGREST', 1] = 'REST'
rl.loc[rl[0] == 'FRCC', 1] = 'FRCC'

#Cleaning up the misnamed groups
#unique_g = pd.Series(rl[0].unique()).dropna()
#print(unique_g)
rl[0] = rl[0].replace('NENGREST','NENG')
rl[0] = rl[0].replace('WECC','WEC')
unique_g = pd.Series(rl[0].unique()).dropna()
print('number of regional groups in dataset (including CN) =',unique_g.shape[0])
rl.rename(columns={0 : 'R_Group', 1: 'Drop', 2:'Region'},inplace=True)
rl = rl[['R_Group','Region']]

#Creating a dummy for load dataset where r_subgroup = region
#for wind and solar this will include state data
rl['R_Subgroup']=rl['Region']
#print(rl.head())

#Merging Regional Data to DF
load_org = pd.merge(rl,load_org,on='Region',how='right')
print()
print(load_org.head(2))

#Removing Canada
load_org = load_org[load_org['R_Group']!="CN"]
print()
print('number of rows in dataset after removing CN =',load_org.shape[0])
unique_r = pd.Series(load_org['Region'].unique()).dropna()
print('number of regions in dataset (excluding CN) =',unique_r.shape[0])
unique_g = pd.Series(load_org['R_Group'].unique()).dropna()
print('number of regional groups in dataset (excluding CN) =',unique_g.shape[0])

#for testing only, otherwise comment out the lines below
#NOTE: use FRCC for one region, ERC for two regions
#load_org = load_org[load_org['R_Group']=="ERC"]
#print('number of rows in dataset for testing =',load_org.shape[0])


# In[64]:


#rename hour titles to just the value ('Hour 1' --> 1)
load_org.columns = load_org.columns.str.replace('Hour ', '')
#print(load_org.head(2))

#melt function converts values in wide format to long format
load_dur = pd.melt(load_org,id_vars=['R_Group','R_Subgroup','Region','Month','Day'],
                    var_name='Hour',value_name='Load')

#print(load_dur.dtypes)

#turn hour values to numeric 
load_dur['Hour'] = pd.to_numeric(load_dur['Hour'],errors='coerce')
unique_h = pd.Series(load_dur['Hour'].unique()).dropna()
#print(unique_h.tail(2))

#turn load values to numeric 
load_dur['Load'] = pd.to_numeric(load_dur['Load'].str.replace(",",""),errors='coerce')
#print(load_dur.head(2))

season_month = pd.read_csv('inputs/season_months.csv')
load_dur = pd.merge(load_dur,season_month, on='Month', how='left')

#days are counted 1 to 365, not 1 to 31, will rename in code below to DOY=day of year
unique_d = pd.Series(load_dur['Day'].unique()).dropna()
#print(unique_d.tail(2))

#Scale Load as a function of the peak hour in each region
#Do not have to scale solar and wind since they are CFs
aggregations = {'Load':max}
peak = load_dur.groupby(['Region'],as_index=False).agg(aggregations)
peak = peak.rename(columns={'Load':'Load_Peak'})
#print(peak.head())
load_dur = pd.merge(load_dur,peak, on='Region', how='left')

load_dur = load_dur.rename(columns={'Load':'Load_Act','Day':'DOY'})
load_dur['Load'] = load_dur['Load_Act'] / load_dur['Load_Peak']

#add an hour counter
load_dur['HOY'] = (load_dur['Hour']) + (load_dur['DOY'] - 1) * 24
load_dur = load_dur.sort_values(by=['Region','HOY'])
unique_hc = pd.Series(load_dur['HOY'].unique()).dropna()
#print(unique_hc.tail(2))

#organized long format data to new csv file
load_dur = load_dur[['Region','R_Group','R_Subgroup','Season','Month','DOY','Hour','HOY','Load_Act','Load']]
load_dur.to_csv('../outputs/load_long_format.csv')
print(load_dur.tail(2))
print()
print('number of rows in dataset =',load_dur.shape[0])
print('number of regs in dataset =',load_dur.shape[0]/8760)


# # Solar Data Prep

# In[65]:


solar_raw = pd.read_csv('inputs/solar_generation.csv')
print(solar_raw.head(2))
print()
print('number of rows in dataset =', solar_raw.shape[0])


# In[66]:


#Organizing regional data

#create temporary copy to make changes on
solar_org = solar_raw.copy()
print('number of rows in dataset (including CN) =',solar_org.shape[0])

#Regional IDs
unique_r = pd.Series(solar_org['Region'].unique()).dropna()
rl = unique_r.str.split("_",n=1,expand=True)
rl[2] = unique_r
#print(rl)
print('number of regions in dataset (including CN) =',unique_r.shape[0])

#Cleaning up the empty subgroups
#print(rl[rl.isna().any(axis=1)])
rl.loc[rl[0] == 'NENGREST', 1] = 'REST'
rl.loc[rl[0] == 'FRCC', 1] = 'FRCC'

#Cleaning up the misnamed groups
#unique_g = pd.Series(rl[0].unique()).dropna()
#print(unique_g)
rl[0] = rl[0].replace('NENGREST','NENG')
rl[0] = rl[0].replace('WECC','WEC')
unique_g = pd.Series(rl[0].unique()).dropna()
print('number of regional groups in dataset (including CN) =',unique_g.shape[0])
rl.rename(columns={0 : 'R_Group', 1: 'Drop', 2:'Region'},inplace=True)
rl = rl[['R_Group','Region']]
rl['R_Subgroup']=rl['Region']
#print(rl.head())

#Merging Regional Data to DF
solar_org = pd.merge(rl,solar_org,on='Region',how='right')
solar_org['Region'] = solar_org['State'] + "_" + solar_org['R_Subgroup'] 
print()
print(solar_org.head(2))

#Creating State+Region column

#Removing Canada
solar_org = solar_org[solar_org['R_Group']!="CN"]
print()
print('number of rows in dataset after removing CN =',solar_org.shape[0])
unique_r = pd.Series(solar_org['Region'].unique()).dropna()
print('number of regions in dataset (excluding CN) =',unique_r.shape[0])
unique_g = pd.Series(solar_org['R_Group'].unique()).dropna()
print('number of regional groups in dataset (excluding CN) =',unique_g.shape[0])


# In[67]:


#melt function converts values in wide format to long format
solar_dur = pd.melt(solar_org,id_vars=['R_Group','Region','State','R_Subgroup',
                                        'Season','Month','Day','Resource Class'],
                     var_name='Hour',value_name='Solar_Gen')
#print(solar_dur)
#print(solar_dur.dtypes)

#turn hour values to numeric 
solar_dur['Hour'] = pd.to_numeric(solar_dur['Hour'],errors='coerce')
unique_h = pd.Series(solar_dur['Hour'].unique()).dropna()
#print(unique_h.tail(2))

#edit columns
solar_dur = solar_dur.drop(columns={'Season'})
solar_dur = solar_dur.rename(columns={'Resource Class':'TRG'})
#print(solar_dur)

#change generation value to capacity factor
solar_dur['Solar_Gen']=solar_dur['Solar_Gen']/1000

#create new seasons column
season_month = pd.read_csv('inputs/season_months.csv')
solar_dur = pd.merge(solar_dur,season_month, on='Month', how='left')
solar_dur = solar_dur.sort_values(['Region','Month','Day'])
print(solar_dur.head())
#print(solar_dur.shape)


# In[68]:


#create DF that only has the labels, easier to merge onto
solar_dur2 = solar_dur[['Region','R_Group','R_Subgroup','State','Month','Day','Hour','Season']].copy()
solar_dur2 = solar_dur2.drop_duplicates(['Region','R_Group','R_Subgroup','State','Month','Day','Hour'])
#print(solar_dur2.head())

#loops through the TRGs, creates a column for each one, decreases the number of rows in DF
#Note: It would be better to set this up to find the TRG numbers and then do the loop 
        #instead of hard coding in the TRG numbers here
for n in range(3,9):
    #print(n)
    solar_subset = solar_dur.loc[solar_dur['TRG'] == n].reset_index(drop=True)
    solar_subset = solar_subset.rename(columns={'Solar_Gen':'TRG'+str(n)}).drop(columns={'TRG'})
    solar_dur2 = pd.merge(solar_dur2,solar_subset,on=['R_Group','R_Subgroup','Region','State',
                                                      'Month','Day','Hour','Season'],how='left')

#creates an average TRG column
solar_dur2['TRG_Avg'] = solar_dur2[['TRG3','TRG4','TRG5','TRG6','TRG7','TRG8']].mean(axis=1)
print(solar_dur2.head())
print()
print('number of rows in dataset =',solar_dur2.shape[0])
print('number of regs in dataset =',solar_dur2.shape[0]/8760)   


# In[69]:


#matches the month and day-of-month to the day-of-year (e.g.365) value
days = pd.read_csv('inputs/days_365.csv')
#print(days.head())
solar_dur3 = pd.merge(days,solar_dur2,on=['Month','Day'],how='right')
#print(solar_dur3.head())

#add an hour counter
solar_dur3['HOY'] = (solar_dur3['Hour']) + (solar_dur3['DOY'] - 1) * 24
solar_dur3 = solar_dur3.sort_values(by=['Region','HOY'])
unique_hc = pd.Series(solar_dur3['HOY'].unique()).dropna()
#print(unique_hc.tail(2))

#add load data column
l_col = load_dur[['R_Subgroup','HOY','Load_Act']]
solar_dur4 = pd.merge(solar_dur3,l_col,on=['R_Subgroup','HOY'],how='left')
#print(solar_dur4.shape[0]/8760)

#Remove regions without load data (only removes ERC_PHDL)
solar_dur4 = solar_dur4.dropna(subset=['Load_Act'])
#print(solar_dur4.shape[0]/8760)

#organized long format data to new csv file
solar_dur4 = solar_dur4[['Region','R_Group','R_Subgroup','State','Season','Month','DOY','Hour','HOY',
                         'Load_Act','TRG3','TRG4','TRG5','TRG6','TRG7','TRG8','TRG_Avg']]
solar_dur4.to_csv('../outputs/solar_long_format.csv')
print(solar_dur4.tail())
print('number of rows in dataset =',solar_dur4.shape[0])
print('number of regs in dataset =',solar_dur4.shape[0]/8760)


# # Wind Data Prep

# In[70]:


wind_raw = pd.read_csv('inputs/onshore_wind_gen.csv')
print(wind_raw.head(2))
print()
print('number of rows in dataset =', wind_raw.shape[0])


# In[71]:


#Organizing regional data

#create temporary copy to make changes on
wind_org = wind_raw.copy()
print('number of rows in dataset (including CN) =',wind_org.shape[0])

#Regional IDs
unique_r = pd.Series(wind_org['Region'].unique()).dropna()
rl = unique_r.str.split("_",n=1,expand=True)
rl[2] = unique_r
#print(rl)
print('number of regions in dataset (including CN) =',unique_r.shape[0])

#Cleaning up the empty subgroups
#print(rl[rl.isna().any(axis=1)])
rl.loc[rl[0] == 'NENGREST', 1] = 'REST'
rl.loc[rl[0] == 'FRCC', 1] = 'FRCC'

#Cleaning up the misnamed groups
#unique_g = pd.Series(rl[0].unique()).dropna()
#print(unique_g)
rl[0] = rl[0].replace('NENGREST','NENG')
rl[0] = rl[0].replace('WECC','WEC')
unique_g = pd.Series(rl[0].unique()).dropna()
print('number of regional groups in dataset (including CN) =',unique_g.shape[0])
rl.rename(columns={0 : 'R_Group', 1: 'Drop', 2:'Region'},inplace=True)
rl = rl[['R_Group','Region']]
rl['R_Subgroup']=rl['Region']
#print(rl.head())

#Merging Regional Data to DF
wind_org = pd.merge(rl,wind_org,on='Region',how='right')
wind_org['Region'] = wind_org['State'] + "_" + wind_org['R_Subgroup'] 
print()
print(wind_org.head(2))

#used to see if its day or DOY value, if DOY then rename
unique_d = pd.Series(wind_org['Day'].unique()).dropna()
#print(unique_d.tail(2))

#Removing Canada
wind_org = wind_org[wind_org['R_Group']!="CN"]
print()
print('number of rows in dataset after removing CN =',wind_org.shape[0])
unique_r = pd.Series(wind_org['Region'].unique()).dropna()
print('number of regions in dataset (excluding CN) =',unique_r.shape[0])
unique_g = pd.Series(wind_org['R_Group'].unique()).dropna()
print('number of regional groups in dataset (excluding CN) =',unique_g.shape[0])


# In[72]:


#create an empty set to merge to, the loop copies regional/day IDs for each hour
wind_dur=[]
for n in range(1,25):
    #print(n)
    rdh = wind_org[['Region','R_Group','R_Subgroup','State','Month','Day']].copy()
    rdh = rdh.drop_duplicates(['Region','R_Group','R_Subgroup','State','Month','Day'])
    rdh['Hour']=n
    wind_dur.append(rdh)
wind_dur = pd.concat(wind_dur)
#wind_dur.describe()


# In[73]:


#loops through the TRGs, creates a column for each one, decreases the number of rows in DF
#Note: It would be better to set this up to find the TRG numbers and then do the loop 
        #instead of hard coding in the TRG numbers here

for n in range(1,7):
    #print(n)
    
    wind_subset = wind_org.loc[wind_org['TRG'] == n].reset_index(drop=True)
    
    #melt function converts values in wide format to long format
    wind_subset = pd.melt(wind_subset,id_vars=['R_Group','R_Subgroup','Region','State','Month','Day','TRG'],
                          var_name='Hour',value_name='Wind_Gen')
    
    #edit columns 
    wind_subset['Wind_Gen'] = wind_subset['Wind_Gen']/1000
    wind_subset['Hour'] = pd.to_numeric(wind_subset['Hour'],errors='coerce')
    wind_subset = wind_subset.rename(columns={'Wind_Gen':'TRG'+str(n)}).drop(columns={'TRG'})
    
    wind_dur = pd.merge(wind_dur,wind_subset,on=['Region','R_Group','R_Subgroup','State',
                                                 'Month','Day','Hour'],how='left')


#create new seasons column
print(wind_dur.head())
print()
print('number of rows in dataset =',wind_dur.shape[0])


# In[77]:


#Averages TRG data
wind_dur['TRG_Avg'] = wind_dur[['TRG1','TRG2','TRG3','TRG4','TRG5','TRG6']].mean(axis=1)
wind_dur = wind_dur.dropna(subset=['TRG_Avg'])

#adds season data
season_month = pd.read_csv('inputs/season_months.csv')
wind_dur1 = pd.merge(wind_dur,season_month, on='Month', how='left')

#matches the month and day-of-month to the day-of-year (e.g.365) value
wind_dur2 = pd.merge(days,wind_dur1,on=['Month','Day'],how='right')
#print(wind_dur_fin.head())

#add an hour counter
wind_dur2['HOY'] = (wind_dur2['Hour']) + (wind_dur2['DOY'] - 1) * 24
wind_dur2 = wind_dur2.sort_values(by=['Region','HOY'])
unique_hc = pd.Series(wind_dur2['HOY'].unique()).dropna()
#print(unique_hc.tail(2))

#add load data column
l_col = load_dur[['R_Subgroup','HOY','Load_Act']]
wind_dur_fin = pd.merge(wind_dur2,l_col,on=['R_Subgroup','HOY'],how='left')
#print(wind_dur_fin.shape[0]/8760)

#Remove regions without load data (only removes ERC_PHDL)
wind_dur_fin = wind_dur_fin.dropna(subset=['Load_Act'])
#print(wind_dur_fin.shape[0]/8760)

#organized long format data to new csv file
wind_dur_fin = wind_dur_fin[['Region','R_Group','R_Subgroup','State','Season','Month','DOY','Hour','HOY',
                             'Load_Act','TRG1','TRG2','TRG3','TRG4','TRG5','TRG6','TRG_Avg']]
wind_dur_fin.to_csv('../outputs/wind_long_format.csv')
print(wind_dur_fin.tail(3))
print('number of rows in dataset =',wind_dur_fin.shape[0])
print('number of regs in dataset =',wind_dur_fin.shape[0]/8760)
