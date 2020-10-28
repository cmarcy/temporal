#!/usr/bin/env python
# coding: utf-8

# # Data here: 
# ## IPM Approach (2 profiles) 
# ## Sequential Approach (9ish profiles) 

# # Import Long Format 8760 

# In[624]:

print('start current model approaches and sequential approaches')
print()

#importing packages needed for analysis
import os
import pandas as pd
#import numpy as np
#import math
#from pandas import DataFrame

path = os.getcwd()
#print(path)

load_dur = pd.read_csv('../outputs/load_long_format.csv')
solar_dur = pd.read_csv('../outputs/solar_long_format.csv')
wind_dur = pd.read_csv('../outputs/wind_long_format.csv')

## UNCOMMENT WHICH PROFILE TO BE USED WHEN TESTING
#x = load_dur
#x_name = 'load'
#x_name2 = 'Load'
#x_column = 'Load'

#x = solar_dur
#x_name = 'solar'
#x_name2 = 'Solar_Gen'
#x_column = 'TRG_Avg'

x = wind_dur
x_name = 'wind'
x_name2 = 'Wind_Gen'
x_column = 'TRG_Avg'

#this code creates an output directory in the parent director, if one does not exist yet
#Note: this is where all of the output files will be written, since outputs are large this saves space in git
path = os.getcwd()
parent = os.path.dirname(path)
outputs_dir = parent+'\outputs'
if not os.path.exists(outputs_dir):
    os.makedirs(outputs_dir)
#print('output files are written out in parent directory: '+outputs_dir)

outputs_x = outputs_dir+'/'+x_name
if not os.path.exists(outputs_x):
    os.makedirs(outputs_x)
print('output files are written out in parent directory: '+outputs_x)
print()

print(x_name, 'setup')
print()

#for testing only, should display a small number of regions
#x = x[x['R_Group']=='ERC']

#Initial dataset
unique_r = pd.Series(x['Region'].unique()).dropna()
x = x[['Region','R_Group','R_Subgroup','Season','Month','DOY','Hour','HOY','Load_Act',x_column]]
#print(x.tail(2))
#print()
print('Number of regions in initial dataset',len(unique_r))
print()

print('Normal IPM Approach (Max of 72 Representative Hours)')
# ### Details: Split into 3 seasons, then 6 groups, then 4 times of day
# #### Methodology: Use counters to keep track of season split, then use groupby function to find load averages

# In[625]:


#Assign the time of day (TOD) categories to hours

#Read in the time of day categories
tod = pd.read_csv('inputs/time_of_day.csv')
#print(tod)

#merge the time of data categories to the dataframe
x2 = pd.merge(x,tod,on='Hour',how='left')
#print()
#print(x2.head(2))
#print(x2.shape)


# In[626]:


#Assign the Group categories to season/hours

#Create a list of season to the 8760 hours
seasons = x2[x2['Region']==x2['Region'].iloc[0]]
seasons = seasons[['Season','HOY']]
#print(seasons.head())
#print(seasons.shape)

#get the number of hours in each season
season_count = seasons.groupby('Season',as_index=False).count().rename(columns={'HOY':'Season_Tot'})
season_count = season_count.sort_values('Season')
#print(season_count)
#print()

#read in the group shares data
group = pd.read_csv('inputs/group_shares.csv')
#print(group)
#print()

#combined the group shares data with the season/hours data
#for each Season create a column in the group DF, determines number of hours in Sea-G combo
for i in season_count.index:
    group[season_count.iloc[i,0]] = group['Share']*season_count.iloc[i,1]
#print(group)

group_sea = pd.melt(group,id_vars=['Group','Share'],var_name='Seasons',value_name='Sea-G_Tot')
group_sea['Season_Counter'] = group_sea['Sea-G_Tot'].cumsum()
group_sea['Season_Counter'] = round(group_sea['Season_Counter'])
#print(group.dtypes)
#print(group)
#print(group_sea)
#print()

#Create a dataframe with 8760 numbers
unique_hc = pd.Series(x['HOY'].unique()).dropna()
unique_hc = pd.DataFrame(unique_hc,columns=['Season_Counter'])
unique_hc['Season_Counter'] = unique_hc['Season_Counter']*1.0
season_8760 = pd.merge_asof(unique_hc, group_sea, on='Season_Counter', direction='forward')
#print(unique_hc.tail())
#print(season_8760.head(3))
#print(season_8760.tail(3))
#print(season_8760.shape)


# In[627]:


#Assign the load group categories to each of the 8760 hours

x2 = x2.sort_values(['Region','Season','Load_Act'], ascending=[True, True, False])
x2 = x2.reset_index(drop=True)
x2['Season_Counter'] = ( ( x2.index + 8760 ) % 8760 ) + 1
x2['Season_Counter'] = x2['Season_Counter'].astype(int)
#print(x2.head(2))
#print(x2.dtypes)

#Merge seasonal group data to the full dataframe
x3 = pd.merge(x2,season_8760,on='Season_Counter',how='left')
x3 = x3.drop(columns=['Season_Counter','Share','Seasons','Sea-G_Tot'])
#print()
#print(x3.tail(2))
#print()
#print('number of rows for each region =',x3.shape[0]/len(unique_r))


# In[628]:


x3 = x3.sort_values(['Region','Season','Group','TOD'])

aggregations = {x_column:['count','mean']}
norm = x3.groupby(['Region','Season','Group','TOD'],as_index=False).agg(aggregations)
norm.columns = norm.columns.droplevel(0)
norm.columns = ['Region','Season','Group','TOD','Hour_Tot','Avg']
#print(norm.head(3))
print('number segments for each region =',norm.shape[0]/len(unique_r))
#norm.to_csv('../outputs/'+x_name+'/'+x_name+'_segments_norm.csv')
#print()

x4 = pd.merge(x3,norm,on=['Region','Season','Group','TOD'],how='left')
x4 = x4.sort_values(['Region',x_column])
#print(x4.head(3))
print('number of rows in dataset for each region =',x4.shape[0]/len(unique_r))
x4.to_csv('../outputs/'+x_name+'/'+x_name+'_8760_norm.csv')

print()
print('Alternative IPM Approach (72 Representative Hours)')
# ### Details: Regions will be split first by season, then time of day, then group
# #### Methodology: Use same methods & code as norm case, but switch order of groupby for group and time of day

# In[631]:


#Assign the time of day (TOD) categories to hours

#Read in the time of day categories
tod = pd.read_csv('inputs/time_of_day.csv')
#print(tod)

#merge the time of data categories to the dataframe
tod_x = pd.merge(x,tod,on='Hour',how='left')
#print()
#print(tod_x.head(2))


# In[632]:


#Assign the Group categories to season/hours

#Create a list of season to the 8760 hours
seasons = x2[x2['Region']==x2['Region'].iloc[0]]
seasons = seasons[['Season','TOD','HOY']]
#print(seasons.head())
#print(seasons.shape)

#get the number of hours in each season
season_count = seasons.groupby(['Season','TOD'],as_index=False).count().rename(columns={'HOY':'ST_Tot'})
season_count = season_count.sort_values('Season')
season_count['Sea_TOD'] = season_count['Season']+"_"+season_count['TOD']
#print(season_count)
#print()

#read in the group shares data
group = pd.read_csv('inputs/group_shares.csv')
#print(group)

#for each Sea_TOD create a column in the group DF, determines number of hours in STG combo
for i in season_count.index:
    group[season_count.iloc[i,3]] = group['Share']*season_count.iloc[i,2]
#print(group)

group_STG = pd.melt(group,id_vars=['Group','Share'],var_name='Sea_TOD',value_name='STG_Tot')
group_STG['STG_Counter'] = group_STG['STG_Tot'].cumsum()
group_STG['STG_Counter'] = round(group_STG['STG_Counter'])
#print(group_STG.tail())
#print()

#Create a dataframe with 8760 numbers
unique_hc2 = pd.Series(x['HOY'].unique()).dropna()
unique_hc2 = pd.DataFrame(unique_hc2,columns=['STG_Counter'])
unique_hc2['STG_Counter'] = unique_hc2['STG_Counter']*1.0
STG_8760 = pd.merge_asof(unique_hc2, group_STG, on='STG_Counter', direction='forward')
#print(unique_hc.tail())
#print(STG_8760.head())
#print(STG_8760.tail(3))
#print(STG_8760.shape)
#print()


# In[633]:


#Assign the load group categories to each of the 8760 hours

#Create the seasonal counter in the load dataset to merge with
tod_x = tod_x.sort_values(['Region','Season','Load_Act'], ascending=[True, True, False])
tod_x = tod_x.reset_index(drop=True)
tod_x['STG_Counter'] = ( ( tod_x.index + 8760 ) % 8760 ) + 1
tod_x['STG_Counter'] = tod_x['STG_Counter'].astype(int)
#print(tod_x.head(2))
#print(tod_x.dtypes)

#Merge seasonal group data to the full dataframe
tod_x_2 = pd.merge(tod_x,STG_8760,on='STG_Counter',how='left')
tod_x_2 = tod_x_2.drop(columns=['STG_Counter','Share','Sea_TOD','STG_Tot'])
#print(tod_x_2.head(2))
#print()
#print(tod_x_2.tail(2))
#print()
#print('number of rows in dataset =',tod_x_2.shape[0])


# In[634]:


tod_x_2 = tod_x_2.sort_values(['Region','Season','TOD','Group'])

aggregations = {x_column:['count','mean']}
case2 = tod_x_2.groupby(['Region','Season','TOD','Group'],as_index=False).agg(aggregations)
case2.columns = case2.columns.droplevel(0)
case2.columns = ['Region','Season','TOD','Group','Hour_Tot','Avg']
#print(case2.head(3))
print('number of segments for each region =',case2.shape[0]/len(unique_r))
#case2.to_csv('../outputs/'+x_name+'/'+x_name+'_segments_timeofday.csv')
#print()

tod_x_3 = pd.merge(tod_x_2,case2,on=['Region','Season','TOD','Group'],how='left')
tod_x_3 = tod_x_3.sort_values(['Region',x_column])
#print(tod_x_3.head(3))
print('number of rows in dataset =',tod_x_3.shape[0]/len(unique_r))
tod_x_3.to_csv('../outputs/'+x_name+'/'+x_name+'_8760_timeofday.csv')

print()
print('Sequential Approach')
# ### Details: Split into different hour intervals (e.g. 1, 2, 4, 8, 12, ..., 8760) to test the accuracy of the load estimations
# #### Methodology: use a single csv numbered from 1 to 8760 to identify the groups that each hour will go in 

# In[636]:


#load useful info into notebook
seq_intervals = pd.read_csv('inputs/sequential_hours.csv')
seq_x = pd.merge(x, seq_intervals, on='HOY', how='left')
#print(seq_x.head(9))

hr_list = seq_intervals.columns[1:]
print(hr_list)
print()

for i in hr_list:
    print(i)
    #average load based on order of groups
    aggregations = {x_column:['count','mean']}
    case = seq_x.groupby(['Region',i],as_index=False).agg(aggregations)
    case.columns = ['Region',i,'HT_'+i,'Avg_'+i]
    #print(case.head())
    print('number of segments for each region =',case.shape[0]/len(unique_r))
    
    seq_x = pd.merge(seq_x,case,on=['Region',i],how='left')
    seq_x = seq_x.sort_values(['Region','HOY']).reset_index(drop=True)
    print('number of hours for each region =',seq_x.shape[0]/len(unique_r))
    print()

#print(seq_x.head(1))


# In[637]:


#This just exports the data into separate csv files
#Note: I know I can print these in a loop with a dictionary, but this seemed easier at the time... 

#print(seq_x.columns)

#hr-1
seq_1hr = seq_x[['Region', 'Season', 'Month', 'DOY', 'Hour','HOY','Load_Act','HT_1-hr',x_column, 
                 '1-hr','Avg_1-hr']].rename(columns={'1-hr':'Seg_ID','HT_1-hr':'Hour_Tot','Avg_1-hr': 'Avg'})
seq_1hr.to_csv('../outputs/'+x_name+'/'+x_name+'_8760_seq_1hr.csv')

#hr-2
seq_2hr = seq_x[['Region', 'Season', 'Month', 'DOY', 'Hour','HOY','Load_Act','HT_2-hr',x_column,
                 '2-hr','Avg_2-hr']].rename(columns={'2-hr':'Seg_ID','HT_2-hr':'Hour_Tot','Avg_2-hr': 'Avg'})
seq_2hr.to_csv('../outputs/'+x_name+'/'+x_name+'_8760_seq_2hr.csv')

#hr-4
seq_4hr = seq_x[['Region', 'Season', 'Month', 'DOY', 'Hour','HOY','Load_Act','HT_4-hr',x_column,
                 '4-hr','Avg_4-hr']].rename(columns={'4-hr':'Seg_ID','HT_4-hr':'Hour_Tot','Avg_4-hr': 'Avg'})
seq_4hr.to_csv('../outputs/'+x_name+'/'+x_name+'_8760_seq_4hr.csv')

#hr-6
seq_6hr = seq_x[['Region', 'Season', 'Month', 'DOY', 'Hour','HOY','Load_Act','HT_6-hr',x_column,
                 '6-hr','Avg_6-hr']].rename(columns={'6-hr':'Seg_ID','HT_6-hr':'Hour_Tot','Avg_6-hr': 'Avg'})
seq_6hr.to_csv('../outputs/'+x_name+'/'+x_name+'_8760_seq_6hr.csv')

#hr-8
seq_8hr = seq_x[['Region', 'Season', 'Month', 'DOY', 'Hour','HOY','Load_Act','HT_8-hr',x_column, 
                 '8-hr','Avg_8-hr']].rename(columns={'8-hr':'Seg_ID','HT_8-hr':'Hour_Tot','Avg_8-hr': 'Avg'})
seq_8hr.to_csv('../outputs/'+x_name+'/'+x_name+'_8760_seq_8hr.csv')

#hr-12
seq_12hr = seq_x[['Region', 'Season', 'Month', 'DOY', 'Hour','HOY','Load_Act','HT_12-hr',x_column,
                  '12-hr','Avg_12-hr']].rename(columns={'12-hr':'Seg_ID','HT_12-hr':'Hour_Tot','Avg_12-hr': 'Avg'})
seq_12hr.to_csv('../outputs/'+x_name+'/'+x_name+'_8760_seq_12hr.csv')

#hr-120
seq_120hr = seq_x[['Region', 'Season', 'Month', 'DOY', 'Hour','HOY','Load_Act','HT_120-hr',x_column, 
                   '120-hr','Avg_120-hr']].rename(columns={'120-hr':'Seg_ID','HT_120-hr':'Hour_Tot','Avg_120-hr':'Avg'})
seq_120hr.to_csv('../outputs/'+x_name+'/'+x_name+'_8760_seq_120hr.csv')

#hr-8760
seq_8760hr = seq_x[['Region', 'Season', 'Month', 'DOY', 'Hour','HOY','Load_Act','HT_8760-hr',x_column, 
                    '8760-hr','Avg_8760-hr']].rename(columns={'8760-hr':'Seg_ID','HT_8760-hr':'Hour_Tot','Avg_8760-hr':'Avg'})
seq_8760hr.to_csv('../outputs/'+x_name+'/'+x_name+'_8760_seq_8760hr.csv')

#print(seq_1hr.head())
#print(seq_8760hr.head())

print('completed current model approaches and sequential approaches')

# In[ ]:




