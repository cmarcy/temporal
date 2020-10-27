#!/usr/bin/env python
# coding: utf-8

# ## kmeans fit
# ##### this code calculate a kmeans fit for the duration data by:
#     # by each dataset by region for each season
#     # combining all three datasets, load/solar/wind and solving for region/season

# In[230]:


#temporary code because CWD wasn't syncing for some reason
import os
path = os.getcwd()
print(path)
os.chdir('C:/Users/cmarcy/Desktop/py_projects/temporal/new_version/temporal-resolution/')
path = os.getcwd()
print(path)


# ## Setup

# In[231]:


#importing packages needed for analysis
from sklearn.cluster import KMeans
import pandas as pd
import numpy as np
import os
import numpy as np
import pandas as pd
import math
from pandas import DataFrame

path = os.getcwd()
print(path)

load_dur = pd.read_csv('../outputs/load_long_format.csv')
solar_dur = pd.read_csv('../outputs/solar_long_format.csv')
wind_dur = pd.read_csv('../outputs/wind_long_format.csv')

#Set TRG to consider
s_trg = 'TRG_Avg'
w_trg = 'TRG_Avg'

## UNCOMMENT WHICH PROFILE TO BE USED
x = load_dur
x_name = 'load'
x_name2 = 'Load'
x_column = 'Load'

#x = solar_dur
#x_name = 'solar'
#x_name2 = 'Solar'
#x_column = s_trg

#x = wind_dur
#x_name = 'wind'
#x_name2 = 'Wind'
#x_column = w_trg

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


# In[232]:


#combined all three datasets

#print(load_dur.head(1))
#print(solar_dur.head(1))
#print(wind_dur.head(1))

#Default is TRG_Avg
if x_name == 'solar':
    s_trg = x_column 
elif x_name == 'wind':
    w_trg = x_column

#average the state data across IPM regions
wset =  wind_dur[['R_Subgroup','HOY', w_trg]]
wset2 = wset.groupby(['R_Subgroup','HOY'],as_index=False).agg({w_trg:['mean']})
wset2.columns = wset2.columns.droplevel(0)
wset2.columns=['R_Subgroup','HOY','Wind']
#print(wset2.head())
wnan = wset2[wset2.isna().any(axis=1)]
print(wnan)
unique_w = pd.Series(wset2['R_Subgroup'].unique()).dropna()
print(len(unique_w),'regions with wind resource')
#print(unique_w)

sset = solar_dur[['R_Subgroup','HOY', s_trg]]
sset2 = sset.groupby(['R_Subgroup','HOY'],as_index=False).agg({s_trg:['mean']})
sset2.columns = sset2.columns.droplevel(0)
sset2.columns=['R_Subgroup','HOY','Solar']
#print(sset2.head())
snan = sset2[sset2.isna().any(axis=1)]
print(snan)
unique_s = pd.Series(sset2['R_Subgroup'].unique()).dropna()
print(len(unique_s),'regions with solar resource')
#print(unique_s)

#combined wind and solar and then load
wsset = pd.merge(wset2,sset2,on=['R_Subgroup','HOY'],how='outer')
#print(rwsset.head())

lset = load_dur.copy()
lnan = lset[lset.isna().any(axis=1)]
unique_l = pd.Series(lset['R_Subgroup'].unique()).dropna()
print(len(unique_l),'regions with load')
#print(unique_l)
print()

lwsset = pd.merge(lset,wsset,on=['R_Subgroup','HOY'],how='left')
#print(len(lwsset)/8760)
#lwsset.to_csv('../outputs/8760_combo.csv')

lwsnan = lwsset[lwsset.isna().any(axis=1)]
unique_lws = pd.Series(lwsnan['R_Subgroup'].unique()).dropna()
lwsset = lwsset.fillna(0)
print(len(unique_lws),'regions with missing combo resource:')
print('to be filled in as zeros')
print(unique_lws)
print()

#Comment out the line below, only use for testing the loop
#lwsset=lwsset[lwsset['R_Group']=='ERC']

#Set to use for loops
lwsset=lwsset.drop(columns=['Unnamed: 0'])
lwsset.to_csv('../outputs/8760_combo.csv')
print(lwsset.head())


# ## kmeans by region and season

# In[233]:


#s = season
xs = lwsset[['R_Subgroup','Season','HOY','Load','Wind','Solar']].copy()
xs['ID'] = xs['R_Subgroup']+'_'+xs['Season']
unique_ID = pd.Series(xs['ID'].unique()).dropna()
ID_count = unique_ID.shape[0]
print('number of regions (check):') 
print(ID_count/3,'=',reg_count)
print('number of regions X seasons:') 
print(ID_count)

#unique_ID = unique_ID.head(6)
#print(unique_ID)


# In[234]:


k_fit = []
k_hr = []
#ID = region+season, 63*3=189 kmeans solves
for ID in unique_ID:
    k = xs.copy()
    k = k[k['ID']==ID]
    
    #create the order column inside the loop since the n month number changes across seasons
    kh = k.sort_values([x_name2], ascending=[False])
    kh = kh.reset_index(drop=True)
    kh['Order'] = kh.index+1.0
    
    #create a kmeans fit to the data for each region for each season (where n=24=72/3)
    kmeans = KMeans(n_clusters=24)
    model = kmeans.fit(kh[[x_name2,'Order']])
    kf = pd.DataFrame(model.cluster_centers_, columns=['Avg','Mid'])

    kf['ID'] = ID
    kh['ID'] = ID
    kf['Label'] = kf.index
    kh['Label'] = pd.Series(model.labels_, index=kh.index)
    
    #appends each regional fit/hourly data into one dataframe, includes cluster labels
    k_fit.append(kf)
    k_hr.append(kh)
    
    #uncomment out print statement below to see progress of this loop, one print per region
    if k.iloc[0,sea]=='winter':
        print(ID + ' regional kmeans done')   

k_fit = pd.concat(k_fit)
k_hr = pd.concat(k_hr)


# In[235]:


#copy these dataframes here because the loop takes so long to rerun, 
#s=season, f=fit, h=hourly
kfs = k_fit.copy()
print(kfs.head(2))
print('number of rows for each region =',(kfs.shape[0]/reg_count))
print()

khs = k_hr.copy()
print(khs.head(2))
print('number of rows for each region =',(khs.shape[0]/reg_count))
print()


# In[236]:


#aggregating the fit data again to get the resulting averages from the other non-fitted data
#so if x_name is load, what are the averages for wind and solar in when fitting for load?
aggregations = {'Load':['count','mean'],'Wind':['mean'],'Solar':['mean']}
width = khs.groupby(['R_Subgroup','Season','Label'],as_index=False).agg(aggregations)
width.columns = width.columns.droplevel(0)
width.columns = ['R_Subgroup','Season','Label','Hour_Tot','Load','Wind','Solar']
#print(width.head(2))
print(width.shape[0]/reg_count)

#merging to this dataset to get the label data to match to hourly
khs2 = pd.merge(khs[['R_Subgroup','Season','Label','HOY']],width,on=['R_Subgroup','Season','Label'],how='left')
#print(khs2.head(2))
print(khs2.shape[0]/reg_count)
print(khs2.shape[0]/8760)

#merge the fit the final datasets, note that we're matching fit R_subgroup (IPM) to the Region (IPM+State)
#load
khsl = khs2.rename(columns={'Load':'Avg'})
khsl2 = pd.merge(load_dur,khsl,on=['R_Subgroup','Season','HOY'],how='left').drop(columns=['Unnamed: 0','Wind','Solar'])
khsl2.to_csv('../outputs/load/load_8760_k_seasons_'+x_name+'.csv')

#solar
khss = khs2.rename(columns={'Solar':'Avg'})
khss2 = pd.merge(solar_dur,khss,on=['R_Subgroup','Season','HOY'],how='left').drop(columns=['Unnamed: 0','Load','Wind'])
khss2.to_csv('../outputs/solar/solar_8760_k_seasons_'+x_name+'.csv')

#wind
khsw = khs2.rename(columns={'Wind':'Avg'})
khsw2 = pd.merge(wind_dur,khsw,on=['R_Subgroup','Season','HOY'],how='left').drop(columns=['Unnamed: 0','Load','Solar'])
khsw2.to_csv('../outputs/wind/wind_8760_k_seasons_'+x_name+'.csv')

print(khsw2.head())
print(khsl2.shape[0]/8760)
print(khss2.shape[0]/8760)
print(khsw2.shape[0]/8760)


# ## kmeans, all three datasets, by region and season

# In[206]:


#NOTE THIS ONLY NEEDS TO RUN ONCE, not separately for load/wind/solar

#Creates the Region + Season ID
lws = lwsset[['R_Subgroup','Season','HOY','Load','Wind','Solar']].copy()
lws['ID'] = lws['R_Subgroup']+'_'+lws['Season']
unique_ID = pd.Series(lws['ID'].unique()).dropna()
ID_count = unique_ID.shape[0]
print('number of regions (check):') 
print(ID_count/3,'=',reg_count)
print('number of regions X seasons:') 
print(ID_count)

#unique_ID = unique_ID.head(6)
#print(unique_ID)


# In[207]:


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
    if kh.iloc[0,sea]=='winter':
        print(ID + ' regional kmeans done')   

k_fit = pd.concat(k_fit)
k_hr = pd.concat(k_hr)


# In[208]:


#copy these dataframes here because the loop takes so long to rerun, 
#s=season, a=all, f=fit, h=hourly
kfas = k_fit.copy()
print(kfas.head(2))
print('number of rows for each region =',(kfs.shape[0]/reg_count))
print()

khas = k_hr.copy()
print(khas.head(2))
print('number of rows for each region =',(khs.shape[0]/reg_count))
print()


# In[214]:


width = khas.groupby(['ID','Label'],as_index=False).agg({'Label':['count']})
width.columns = width.columns.droplevel(0)
width.columns=['ID','Label','Hour_Tot']
print(width.head())
kfas2 = pd.merge(kfas,width,on=['ID','Label'],how='left')

#merge the fit data to the hourly data
khas2 = pd.merge(khas,kfas2,on=['ID','Label'],how='left').drop(columns=['ID','Load','Wind','Solar'])
print(khas2.head())

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

print(khasw2.head())


# In[120]:


import matplotlib.pyplot as plt
from mpl_toolkits.mplot3d import Axes3D
import pandas as pd
import numpy as np

IDn = khas2.columns.get_loc('ID')
reg = khas2.iloc[8000,IDn]
df = khas2[khas2['ID']==reg]
print(reg)
print(len(df))

fig = plt.figure()
ax = fig.add_subplot(111, projection='3d')
x = np.array(df['Load'])
y = np.array(df['Wind'])
z = np.array(df['Solar'])

ax.scatter(x,y,z, marker="s", c=df['Label'], s=4, cmap='viridis')

print(plt.show())


# In[121]:


print(reg)
df2 = df[['Load','Wind','Solar','Label']].copy()
df2['Lab'] = (df2['Label']+1).apply(str)
df2['Lab'] = df2['Lab'].str.zfill(2)
df2 = df2[['Load','Wind','Solar','Lab']]
#print(df2.dtypes)
print(df2.head(2))

# Seaborn visualization library
import seaborn as sns# Create the default pairplot
sns.pairplot(df2, vars=['Load','Wind','Solar'], hue='Lab')


# In[ ]:




