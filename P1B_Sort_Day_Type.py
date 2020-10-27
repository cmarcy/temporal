#!/usr/bin/env python
# coding: utf-8

# In[385]:


#importing packages needed for analysis
import os
import numpy as np
import pandas as pd
import math
from pandas import DataFrame

path = os.getcwd()
#print(path)

load_dur = pd.read_csv('../outputs/load_long_format.csv')
solar_dur = pd.read_csv('../outputs/solar_long_format.csv')
wind_dur = pd.read_csv('../outputs/wind_long_format.csv')

## UNCOMMENT WHICH PROFILE TO BE USED
x = load_dur
x_name = 'load'
x_name2 = 'Load'
x_column = 'Load'

#x = solar_dur
#x_name = 'solar'
#x_name2 = 'Solar_Gen'
#x_column = 'TRG_Avg'

#x = wind_dur
#x_name = 'wind'
#x_name2 = 'Wind_Gen'
#x_column = 'TRG_Avg'

#this code creates an output directory in the parent director, if one does not exist yet
#Note: this is where all of the output files will be written, since outputs are large this saves space in git
path = os.getcwd()
parent = os.path.dirname(path)
outputs_dir = parent+'\outputs'
if not os.path.exists(outputs_dir):
    os.makedirs(outputs_dir)
print('output files are written out in parent directory: '+outputs_dir)

outputs_x = outputs_dir+'/'+x_name
if not os.path.exists(outputs_x):
    os.makedirs(outputs_x)
print('output files are written out in parent directory: '+outputs_x)

#for testing only, should display a small number of regions
#x = x[x['R_Group']=='ERC']

x = x[['Region','R_Group','R_Subgroup','Season','Month','DOY','Hour','HOY','Load_Act',x_column]]
years = pd.read_csv('inputs/years.csv').dropna()
#print(x.head())

unique_r = pd.Series(x['Region'].unique()).dropna()
#print(unique_r)
reg_count = unique_r.shape[0]
print(reg_count)


# In[386]:


import datetime
daydata = pd.read_csv('inputs/days_365.csv')
#print(daydata.tail())
daydata = daydata.drop(columns='Month')
x2 = pd.merge(x,daydata,on=['DOY'],how='left')
#print(x2.tail())

#sets the year for each region
x2['Year']=2011
x2.loc[x2['R_Group'] == 'ERC', 'Year'] = 2016
#print(x2.head())

#Creates a date column
x2['Date']=pd.to_datetime(x2[['Year', 'Month', 'Day']], errors='coerce')
#print(x2.tail())

#convert date to a datetime type 
#x2['Date'] = pd.to_datetime(x2['Date'])
x2['DOW'] = x2['Date'].dt.weekday

#check if it is a weekday or not 
weekday = pd.read_csv('inputs/weekday.csv')
x2 = pd.merge(x2,weekday,on='DOW',how='left')
x2 = x2.drop(columns=['Year','Day','DOW','Week'])
print(x2.tail())
print('number of rows in dataset =',x2.shape[0])
print('number of regions in dataset =',x2.shape[0]/8760)


# ## Case 1: Monthly, single day type, 24 hours (288 segments)
# #### Methodology: Using groupby function to group first by month, then by 24 hours
# 

# In[353]:


case1_x = x2.copy()

aggregations = {x_column:['count','mean']}
case1 = case1_x.groupby(['Region','Month','Hour'],as_index=False).agg(aggregations)
case1.columns = case1.columns.droplevel(0)
case1.columns = ['Region','Month','Hour','Hour_Tot','Avg']
#print(case1.head())
#case1.to_csv('../outputs/'+x_name+'/'+x_name+'_segments_1dt_mon_24hr.csv')
print('number of segments in dataset =',case1.shape[0]/reg_count)
print()

case1_x2 = pd.merge(case1_x,case1,on=['Region','Month','Hour'],how='left')
case1_x2 = case1_x2.sort_values(['Region',x_column])
print(case1_x2.head(3))
print('number of rows in dataset =',case1_x2.shape[0])
case1_x2.to_csv('../outputs/'+x_name+'/'+x_name+'_8760_1dt_mon_24hr.csv')


# ## Case 2: Season, single day-type, 24 hours (72 segments)
# #### Methodology: Use groupby function to group by season and hour

# In[354]:


case2_x = x2.copy()
#case2_seasons = pd.read_csv('inputs/season_bimonthly.csv')
#case2_seasons = case2_seasons.drop(['bimonthly'], axis=1)
#case2_seasons = case2_seasons.rename(columns={'seasonal':'Season_Group','month':'Month'})
#print(case2_x.head())
#case2_x = pd.merge(case2_x, case2_seasons, on='Month', how='left')

aggregations = {x_column:['count','mean']}
case2 = case2_x.groupby(['Region','Season','Hour'],as_index=False).agg(aggregations)
case2.columns = case2.columns.droplevel(0)
case2.columns = ['Region','Season','Hour','Hour_Tot','Avg']
#print(case2.head())
print('number of segments in dataset =',case2.shape[0]/reg_count)
#case2.to_csv('../outputs/'+x_name+'/'+x_name+'_segments_1dt_sea_24hr.csv')
print()

case2_x2 = pd.merge(case2_x,case2,on=['Region','Season','Hour'],how='left')
case2_x2 = case2_x2.sort_values(['Region',x_column])
print(case2_x2.head(3))
print('number of rows in dataset =',case2_x2.shape[0])
case2_x2.to_csv('../outputs/'+x_name+'/'+x_name+'_8760_1dt_sea_24hr.csv')


# ## Case 3: Monthly, weekend/weekday, 24 hours (576 segments)
# #### Metholodogy: Use groupby function to group by month, then weekend/weekday, then by 24 hours

# In[355]:


case3_x = x2.copy()

aggregations = {x_column:['count','mean']}
case3 = case3_x.groupby(['Region','Month','Weekday','Hour'],as_index=False).agg(aggregations)
case3.columns = case3.columns.droplevel(0)
case3.columns = ['Region','Month','Weekday','Hour','Hour_Tot','Avg']
#print(case1.head())
print('number of segments in dataset =',case3.shape[0]/reg_count)
#case3.to_csv('../outputs/'+x_name+'/'+x_name+'_segments_2dt_mon_24hr.csv')
print()

case3_x2 = pd.merge(case3_x,case3,on=['Region','Month','Weekday','Hour'],how='left')
case3_x2 = case3_x2.sort_values(['Region',x_column])
print(case3_x2.head())
print('number of rows in dataset =',case3_x2.shape[0])
case3_x2.to_csv('../outputs/'+x_name+'/'+x_name+'_8760_2dt_mon_24hr.csv')


# ## 4 hour interval day types

# In[356]:


#read in 4 hour interval counter
interval_4hr = pd.read_csv('inputs/interval_4hr.csv')
#print(interval_4hr)

x3 = pd.merge(x2,interval_4hr,on='Hour',how='left')
print(x3.tail())


# ## Case 5: Monthly, single day type, 4 hour intervals (72 segments)
# #### Methodology: use groupby by month, 4 hour intervals

# In[357]:


case5_x = x3.copy()

aggregations = {x_column:['count','mean']}
case5 = case5_x.groupby(['Region','Month','4-hr'],as_index=False).agg(aggregations)
case5.columns = case5.columns.droplevel(0)
case5.columns = ['Region','Month','4-hr','Hour_Tot','Avg']
#print(case5.head())
print('number of segments in dataset =',case5.shape[0]/reg_count)
#case5.to_csv('../outputs/'+x_name+'/'+x_name+'_segments_1dt_mon_4hr.csv')
print()

case5_x2 = pd.merge(case5_x,case5,on=['Region','Month','4-hr'],how='left')
case5_x2 = case5_x2.sort_values(['Region',x_column])
print(case5_x2.head())
print('number of rows in dataset =',case5_x2.shape[0])
case5_x2.to_csv('../outputs/'+x_name+'/'+x_name+'_8760_1dt_mon_4hr.csv')


# ## Case 6: Bi-monthly weekend/weekday day-types, 4-hour int (72 segs)
# #### Methodology: use groupby function and bimonthly groups

# In[358]:


case6_x = x3.copy()
case6_bimonth = pd.read_csv('inputs/season_bimonthly.csv')
case6_bimonth = case6_bimonth.drop(['seasonal'], axis=1)
case6_bimonth = case6_bimonth.rename(columns={'bimonthly':'Bimonth','month':'Month'})

case6_x = pd.merge(case6_x, case6_bimonth, on='Month', how='left')

aggregations = {x_column:['count','mean']}
case6 = case6_x.groupby(['Region','Bimonth','Weekday','4-hr'],as_index=False).agg(aggregations)
case6.columns = case6.columns.droplevel(0)
case6.columns = ['Region','Bimonth','Weekday','4-hr','Hour_Tot','Avg']
#print(case6.head())
print('number of segments in dataset =',case6.shape[0]/reg_count)
#case6.to_csv('../outputs/'+x_name+'/'+x_name+'_segments_2dt_bim_4hr.csv')
print()

case6_x2 = pd.merge(case6_x,case6,on=['Region','Bimonth','Weekday','4-hr'],how='left')
case6_x2 = case6_x2.sort_values(['Region',x_column])
print(case6_x2.head())
print('number of rows in dataset =',case6_x2.shape[0])
case6_x2.to_csv('../outputs/'+x_name+'/'+x_name+'_8760_2dt_bim_4hr.csv')


# ## Case 7: Season-based months, weekend/weekday day-types, 4-hour intervals (60 segs)
# #### Methodology: groupby function and applied season groups

# In[359]:


case7_x = x3.copy()
case7_seasons = pd.read_csv('inputs/season_bimonthly.csv')
case7_seasons = case7_seasons.drop(['bimonthly'], axis=1)
case7_seasons = case7_seasons.rename(columns={'seasonal':'Season_Group','month':'Month'})

case7_x = pd.merge(case7_x, case7_seasons, on='Month', how='left')
#print(case4_load)

aggregations = {x_column:['count','mean']}
case7 = case7_x.groupby(['Region','Season_Group','Weekday','4-hr'],as_index=False).agg(aggregations)
case7.columns = case7.columns.droplevel(0)
case7.columns = ['Region','Season_Group','Weekday','4-hr','Hour_Tot','Avg']
#print(case7.head())
print('number of segments in dataset =',case7.shape[0]/reg_count)
#case7.to_csv('../outputs/'+x_name+'/'+x_name+'_segments_2dt_sgp_4hr.csv')
print()

case7_x2 = pd.merge(case7_x,case7,on=['Region','Season_Group','Weekday','4-hr'],how='left')
case7_x2 = case7_x2.sort_values(['Region',x_column])
print(case7_x2.head())
print('number of rows in dataset =',case7_x2.shape[0])
case7_x2.to_csv('../outputs/'+x_name+'/'+x_name+'_8760_2dt_sgp_4hr.csv')


# # Day type: Three (Weekday, Weekend, Peak Load)
# ## Find Peak Load Days in Each Month

# In[360]:


#create temporary DF to find peak
load = load_dur.copy()
#print(load.head())

#groupby region, month, and day to sum the total day
aggregations1 = {'Load_Act':sum}
md_sum = load.groupby(['R_Subgroup','Month','DOY'],as_index=False).agg(aggregations1)
md_sum.columns = ['R_Subgroup','Month','DOY','Load_MD_Tot']
#print(md_sum.tail())
#print('number of regions in regional data =',md_sum.shape[0]/365)
#print()

md_sum2 = pd.merge(load,md_sum,on=['R_Subgroup','Month','DOY'],how='left')
#print(md_sum2.tail())

#groupby region and month to find maximum (peak day)
aggregations2 = {'Load_MD_Tot':max}
md_max = md_sum2.groupby(['R_Subgroup','Month'],as_index=False).agg(aggregations2)
md_max.columns = ['R_Subgroup','Month','Load_MD_Max']
#print(md_max[0:13])
#print()

#match monthly peak day data back to 8760 load DF
peakd = pd.merge(md_sum2,md_max,on=['R_Subgroup','Month'],how='left')
#print(peakd.columns)
peakd = peakd[['R_Subgroup','DOY','Hour','Load_MD_Tot','Load_MD_Max']]
print(peakd.tail())


# In[361]:


x_peak = pd.merge(x2,peakd,on=['R_Subgroup','DOY','Hour'],how='left')
x_peak = x_peak.rename(columns={'Weekday':'Day_Type'})
#print(x_peak.tail())

#Return True if the load total equals the day identified as the max
x_peak.loc[x_peak['Day_Type'] == True, 'Day_Type'] = 'Weekday'
x_peak.loc[x_peak['Day_Type'] == False, 'Day_Type'] = 'Weekend'
x_peak.loc[x_peak['Load_MD_Tot'] == x_peak['Load_MD_Max'], 'Day_Type'] = 'Peak'
x_peak = x_peak.drop(['Load_MD_Tot','Load_MD_Max'], axis=1)
print(x_peak[23:25])
print(x_peak[47:49])


# ## Case 1: Monthly, 3 day-types, 24 hours (864 segments)
# #### Methodology: similar to two day type, just adding in peak day types to sort by

# In[362]:


case1_x = x_peak.copy()

aggregations = {x_column:['count','mean']}
case1 = case1_x.groupby(['Region','Month','Day_Type','Hour'],as_index=False).agg(aggregations)
case1.columns = case1.columns.droplevel(0)
case1.columns = ['Region','Month','Day_Type','Hour','Hour_Tot','Avg']
#print(case1.head())
print('number of segments in dataset =',case1.shape[0]/reg_count)
#case1.to_csv('../outputs/'+x_name+'/'+x_name+'_segments_3dt_mon_24hr.csv')
print()

case1_x2 = pd.merge(case1_x,case1,on=['Region','Month','Day_Type','Hour'],how='left')
case1_x2 = case1_x2.sort_values(['Region',x_column])
print(case1_x2.head(3))
print('number of rows in dataset =',case1_x2.shape[0])
case1_x2.to_csv('../outputs/'+x_name+'/'+x_name+'_8760_3dt_mon_24hr.csv')


# ## Case 3: Annual, 3 day-types, 24-hours (72 segments)

# In[363]:


case3_x = x_peak.copy()

aggregations = {x_column:['count','mean']}
case3 = case3_x.groupby(['Region','Day_Type','Hour'],as_index=False).agg(aggregations)
case3.columns = case3.columns.droplevel(0)
case3.columns = ['Region','Day_Type','Hour','Hour_Tot','Avg']
#print(case3.head())
print('number of segments in dataset =',case3.shape[0]/reg_count)
#case3.to_csv('../outputs/'+x_name+'/'+x_name+'_segments_3dt_ann_24hr.csv')
print()

case3_x2 = pd.merge(case3_x,case3,on=['Region','Day_Type','Hour'],how='left')
case3_x2 = case3_x2.sort_values(['Region',x_column])
print(case3_x2.head(3))
print('number of rows in dataset =',case3_x2.shape[0])
case3_x2.to_csv('../outputs/'+x_name+'/'+x_name+'_8760_3dt_ann_24hr.csv')


# ## 4 hour interval

# In[364]:


#read in 4 hour interval counter
#interval_4hr = pd.read_csv('inputs/interval_4hr.csv')
#print(interval_4hr.head())
#print(x_peak.head())

x_peak2 = pd.merge(x_peak,interval_4hr,on='Hour',how='left')
print(x_peak2.head(5))


# ## Case 5: Season-based months, 3 day-types, 4-hour intervals (90 segs)

# In[365]:


case5_x = x_peak2.copy()
case5_seasons = pd.read_csv('inputs/season_bimonthly.csv')
case5_seasons = case5_seasons.drop(['bimonthly'], axis=1)
case5_seasons = case5_seasons.rename(columns={'seasonal':'Season_Group','month':'Month'})

case5_x = pd.merge(case5_x, case5_seasons, on='Month', how='left')

aggregations = {x_column:['count','mean']}
case5 = case5_x.groupby(['Region','Season_Group','Day_Type','4-hr'],as_index=False).agg(aggregations)
case5.columns = case5.columns.droplevel(0)
case5.columns = ['Region','Season_Group','Day_Type','4-hr','Hour_Tot','Avg']
#print(case5.head())
print('number of segments in dataset =',case5.shape[0]/reg_count)
#case5.to_csv('../outputs/'+x_name+'/'+x_name+'_segments_3dt_sgp_4hr.csv')
print()

case5_x2 = pd.merge(case5_x,case5,on=['Region','Season_Group','Day_Type','4-hr'],how='left')
case5_x2 = case5_x2.sort_values(['Region',x_column])
print(case5_x2.head())
print('number of rows in dataset =',case5_x2.shape[0])
case5_x2.to_csv('../outputs/'+x_name+'/'+x_name+'_8760_3dt_sgp_4hr.csv')


# ## Case 6: Season, 3 day-types, 4-hour intervals (54 Segments)

# In[382]:


case6_x = x_peak2.copy()

aggregations = {x_column:['count','mean']}
case6 = case6_x.groupby(['Region','Season','Day_Type','4-hr'],as_index=False).agg(aggregations)
case6.columns = case6.columns.droplevel(0)
case6.columns = ['Region','Season','Day_Type','4-hr','Hour_Tot','Avg']
#print(case6.head())
print('number of segments in dataset =',case6.shape[0]/reg_count)
#case6.to_csv('../outputs/'+x_name+'/'+x_name+'_segments_3dt_sea_4hr.csv')
print()

case6_x2 = pd.merge(case6_x,case6,on=['Region','Season','Day_Type','4-hr'],how='left')
case6_x2 = case6_x2.sort_values(['Region',x_column])
print(case6_x2.head())
print('number of rows in dataset =',case6_x2.shape[0])
case6_x2.to_csv('../outputs/'+x_name+'/'+x_name+'_8760_3dt_sea_4hr.csv')


# ## Extra Case: 24-hours, average weekly

# In[387]:


daydata = pd.read_csv('inputs/days_365.csv')
#print(daydata.tail())
daydata = daydata.drop(columns='Month')
week = pd.merge(x,daydata,on=['DOY'],how='left')
week = week.drop(columns=['Day'])
print(week.tail())
print('number of rows in dataset =',week.shape[0])
print('number of regions in dataset =',week.shape[0]/8760)


# In[388]:


aggregations = {x_column:['count','mean']}
wcase = week.groupby(['Region','Season','Week','Hour'],as_index=False).agg(aggregations)
wcase.columns = wcase.columns.droplevel(0)
wcase.columns = ['Region','Season','Week','Hour','Hour_Tot','Avg']
print(wcase.head())
print('number of segments in dataset =',wcase.shape[0]/reg_count)
#case6.to_csv('../outputs/'+x_name+'/'+x_name+'_segments_3dt_sea_4hr.csv')
print()

wcase_x = pd.merge(week,wcase,on=['Region','Season','Week','Hour'],how='left')
wcase_x = wcase_x.sort_values(['Region',x_column])
print(wcase_x.head())
print('number of rows in dataset =',wcase_x.shape[0])
print('number of regs in dataset =',wcase_x.shape[0]/8760)

wcase_x.to_csv('../outputs/'+x_name+'/'+x_name+'_8760_weekly_24hr.csv')


# In[ ]:




