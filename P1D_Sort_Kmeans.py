#!/usr/bin/env python
# coding: utf-8

# ## kmeans fit
# ##### this code calculate a kmeans fit for the duration data by:
#     # by each dataset by region for each season
#     # combining all three datasets, load/solar/wind and solving for region/season

# In[230]:

print('start clustering approach')
print()

#importing packages needed for analysis
from sklearn.cluster import KMeans
import pandas as pd
import os
#import numpy as np
#import math

path = os.getcwd()
#print(path)

load_dur = pd.read_csv('../outputs/load_long_format.csv')
solar_dur = pd.read_csv('../outputs/solar_long_format.csv')
wind_dur = pd.read_csv('../outputs/wind_long_format.csv')
lwsset = pd.read_csv('../outputs/8760_combo.csv')

reg_count = len(pd.Series(lwsset['R_Subgroup'].unique()).dropna())

#this code creates an output directory in the parent director, if one does not exist yet
#Note: this is where all of the output files will be written, since outputs are large this saves space in git
path = os.getcwd()
parent = os.path.dirname(path)
outputs_dir = parent+'\outputs'
if not os.path.exists(outputs_dir):
    os.makedirs(outputs_dir)
print('output files are written out in parent directory: '+outputs_dir)

print('kmeans, all three datasets, by region and season')

# In[206]:

#NOTE THIS ONLY NEEDS TO RUN ONCE, not separately for load/wind/solar

#Creates the Region + Season ID
lws = lwsset[['R_Subgroup','Season','HOY','Load','Wind','Solar']].copy()
lws['ID'] = lws['R_Subgroup']+'_'+lws['Season']
unique_ID = pd.Series(lws['ID'].unique()).dropna()
ID_count = unique_ID.shape[0]
print('number of regions (check):',ID_count/3,'=',reg_count)
print('number of regions X seasons:',ID_count)
print()

#unique_ID = unique_ID.head(6)
#print(unique_ID)

# In[207]:

print('start of loop... wait for it...')
k_fit = []
k_hr = []
#ID = region+season, 63*3=189 kmeans solves
for ID in unique_ID:
    kh = lws.copy()
    kh = kh[kh['ID']==ID]
    
    #create a kmeans fit to the data for each region for each season (24 per season)
    kmeans = KMeans(n_clusters=24)
    model = kmeans.fit(kh[['Load','Wind','Solar']])
    kf = pd.DataFrame(model.cluster_centers_, columns=['AvgL','AvgW','AvgS'])
    
    kf['ID'] = ID
    kh['ID'] = ID
    kf['Label'] = kf.index
    kh['Label'] = pd.Series(model.labels_, index=kh.index)
    
    #appends each regional fit/hourly data into one dataframe, includes cluster labels
    k_fit.append(kf)
    k_hr.append(kh)
    
    #uncomment out print statement below to see progress of this loop, one print per region
    #print(ID + ' regional kmeans done')   

k_fit = pd.concat(k_fit)
k_hr = pd.concat(k_hr)

print('end of loop')
print()

# In[208]:

#copy these dataframes here because the loop takes so long to rerun, 
#s=season, a=all, f=fit, h=hourly
kfas = k_fit.copy()
#print(kfas.head(2))
print('number of segments for each region =',(kfas.shape[0]/reg_count))

khas = k_hr.copy()
#print(khas.head(2))
print('number of rows for each region =',(khas.shape[0]/reg_count))
print()

# In[214]:

width = khas.groupby(['ID','Label'],as_index=False).agg({'Season':['count']})
width.columns = width.columns.droplevel(0)
width.columns = ['ID','Label','Hour_Tot']
#print(width.head())
kfas2 = pd.merge(kfas,width,on=['ID','Label'],how='left')

#merge the fit data to the hourly data
khas2 = pd.merge(khas,kfas2,on=['ID','Label'],how='left').drop(columns=['ID','Load','Wind','Solar'])
#print(khas2.head())

#merge the fit the final datasets, note that we're matching fit R_subgroup (IPM) to the Region (IPM+State)
#load
khasl = khas2.rename(columns={'AvgL':'Avg'})
khasl2 = pd.merge(load_dur,khasl,on=['R_Subgroup','Season','HOY'],how='left').drop(columns=['Unnamed: 0','AvgW','AvgS'])
khasl2.to_csv('../outputs/load/load_8760_k_seasons_all.csv')

#solar
khass = khas2.rename(columns={'AvgS':'Avg'})
khass2 = pd.merge(solar_dur,khass,on=['R_Subgroup','Season','HOY'],how='left').drop(columns=['Unnamed: 0','AvgL','AvgW'])
khass2.to_csv('../outputs/solar/solar_8760_k_seasons_all.csv')

#wind
khasw = khas2.rename(columns={'AvgW':'Avg'})
khasw2 = pd.merge(wind_dur,khasw,on=['R_Subgroup','Season','HOY'],how='left').drop(columns=['Unnamed: 0','AvgL','AvgS'])
khasw2.to_csv('../outputs/wind/wind_8760_k_seasons_all.csv')
#print(khasw2.head())

print('number of regions in load file:', khasl2.shape[0]/8760)
print('number of regions in solar file:', khass2.shape[0]/8760)
print('number of regions in wind file:', khasw2.shape[0]/8760)
print()

print('completed clustering approach')

# In[ ]:
