#!/usr/bin/env python
# coding: utf-8

# # Plots -- plots created are exported to file, not shown here

# In[529]:


#importing packages needed for analysis
import os
import numpy as np
import pandas as pd
import math
from pandas import DataFrame
import matplotlib as mpl 

path = os.getcwd()
#print(path)

#this code creates an output directory in the parent director, if one does not exist yet
#Note: this is where all of the output files will be written, since outputs are large this saves space in git
path = os.getcwd()
parent = os.path.dirname(path)
outputs_dir = parent+'\outputs\plots'
if not os.path.exists(outputs_dir):
    os.makedirs(outputs_dir)
print('output files are written out in parent directory: '+outputs_dir)

#global file sets x variables throughout code, similar to commented out code below
import global2

##UNCOMMENT WHICH PROFILE BEING ANALYZED 
#x = 'load'
#x2 = 'Load'

#x = 'solar'
#x2 = 'TRG6'

#x = 'wind'
#x2 = 'TRG4'


# # Import Data 

# In[530]:


diff = pd.read_csv('../outputs/error_analysis/'+x+'_profile_diff.csv').dropna()
print(diff.columns)


# In[531]:


get_ipython().run_line_magic('matplotlib', 'inline')

## agg backend is used to create plot as a .png file
mpl.use('agg')

import matplotlib.pyplot as plt 
import seaborn as sns


# ## Seaborn Boxplots

# In[504]:


#creating dataset to graph
fig=diff.copy().rename(columns={'norm':'IPM'})
#fig=fig[['IPM','timeofday']]
fig=fig[['IPM']]

#used to set the width of the figure based on the number of profiles
width=fig.shape[1]
#print(width)

#used to set the color palette of the violins
palette = ['r']
for n in fig.columns:
    palette.append('g')
palette=palette[:-1]
#print(palette)

#test dataset for plot
#fig=fig.head(10)

#Violin plot details
sns.set(font_scale=1.2,style="whitegrid")
plt.figure(figsize=(width,4))
ax = sns.boxplot(data=fig,palette=palette)
ax.axhline(y=0,c='grey',zorder=0)
ax.set_xlabel('profiles',size = 16)
ax.set_ylabel("error difference",size = 16)
plt.ylim(-1,1)
plt.xticks(rotation=70)
plt.title(x)
ax.grid(True)
plt.savefig('../outputs/plots/bp1'+x+'.png', bbox_inches='tight')


# In[505]:


#creating dataset to graph
fig=diff.copy().rename(columns={'norm':'IPM'})
fig=fig[['IPM','seq_2hr','seq_4hr','seq_6hr','seq_8hr','seq_12hr','seq_120hr','seq_8760hr']]

#used to set the width of the figure based on the number of profiles
width=fig.shape[1]
#print(width)

#used to set the color palette of the violins
palette = ['r']
for n in fig.columns:
    palette.append('b')
palette=palette[:-1]
#print(palette)

#test dataset for plot
#fig=fig.head(10)

#Violin plot details
sns.set(font_scale=1.2,style="whitegrid")
plt.figure(figsize=(width,4))
ax = sns.boxplot(data=fig,palette=palette)
ax.axhline(y=0,c='grey',zorder=0)
ax.set_xlabel('profiles',size = 16)
ax.set_ylabel("error difference",size = 16)
plt.ylim(-1,1)
plt.xticks(rotation=70)
plt.title(x)
ax.grid(True)
plt.savefig('../outputs/plots/bp2'+x+'.png', bbox_inches='tight')


# In[506]:


#creating dataset to graph
fig=diff.copy().rename(columns={'norm':'IPM'})
fig=fig[['IPM','seq_2hr','seq_4hr','seq_6hr','seq_120hr']]

#used to set the width of the figure based on the number of profiles
width=fig.shape[1]
#print(width)

#used to set the color palette of the violins
palette = ['r']
for n in fig.columns:
    palette.append('b')
palette=palette[:-1]
#print(palette)

#test dataset for plot
#fig=fig.head(10)

#Violin plot details
sns.set(font_scale=1.2,style="whitegrid")
plt.figure(figsize=(width,4))
ax = sns.boxplot(data=fig,palette=palette)
ax.axhline(y=0,c='grey',zorder=0)
ax.set_xlabel('profiles',size = 16)
ax.set_ylabel("error difference",size = 16)
plt.ylim(-1,1)
plt.xticks(rotation=70)
plt.title(x)
ax.grid(True)
plt.savefig('../outputs/plots/bp3'+x+'.png', bbox_inches='tight')


# In[519]:


#creating dataset to graph
fig=diff.copy().rename(columns={'norm':'IPM'})
fig=fig[['IPM','1dt_mon_4hr','1dt_sea_24hr','2dt_bim_4hr','2dt_sgp_4hr','3dt_ann_24hr','3dt_sea_4hr','3dt_sgp_4hr']]

#used to set the width of the figure based on the number of profiles
width=fig.shape[1]
#print(width)

#used to set the color palette of the violins
palette = ['r']
for n in fig.columns:
    palette.append('g')
palette=palette[:-1]
#print(palette)

#test dataset for plot
#fig=fig.head(10)

#Violin plot details
sns.set(font_scale=1.2,style="whitegrid")
plt.figure(figsize=(width,4))
ax = sns.boxplot(data=fig,palette=palette)
ax.axhline(y=0,c='grey',zorder=0)
ax.set_xlabel('profiles',size = 16)
ax.set_ylabel("error difference",size = 16)
plt.ylim(-1,1)
plt.xticks(rotation=70)
plt.title(x)
ax.grid(True)
plt.savefig('../outputs/plots/bp4'+x+'.png', bbox_inches='tight')


# In[520]:


#creating dataset to graph
fig=diff.copy().rename(columns={'norm':'IPM'})
fig=fig[['IPM','1dt_mon_24hr','2dt_mon_24hr','3dt_mon_24hr','weekly_24hr']]

#used to set the width of the figure based on the number of profiles
width=fig.shape[1]
#print(width)

#used to set the color palette of the violins
palette = ['r']
for n in fig.columns:
    palette.append('g')
palette=palette[:-1]
#print(palette)

#test dataset for plot
#fig=fig.head(10)

#Violin plot details
sns.set(font_scale=1.2,style="whitegrid")
plt.figure(figsize=(width,4))
ax = sns.boxplot(data=fig,palette=palette)
ax.axhline(y=0,c='grey',zorder=0)
ax.set_xlabel('profiles',size = 16)
ax.set_ylabel("error difference",size = 16)
plt.ylim(-1,1)
plt.xticks(rotation=70)
plt.title(x)
ax.grid(True)
plt.savefig('../outputs/plots/bp5'+x+'.png', bbox_inches='tight')


# In[532]:


#creating dataset to graph
fig=diff.copy().rename(columns={'norm':'IPM'})
fig=fig[['IPM','k_seasons_all','k_seasons_load','k_seasons_solar','k_seasons_wind']]

#used to set the width of the figure based on the number of profiles
width=fig.shape[1]
#print(width)

#used to set the color palette of the violins
palette = ['r']
for n in fig.columns:
    palette.append('gold')
palette=palette[:-1]
#print(palette)

#test dataset for plot
#fig=fig.head(10)

#Violin plot details
sns.set(font_scale=1.2,style="whitegrid")
plt.figure(figsize=(width,4))
ax = sns.boxplot(data=fig,palette=palette)
ax.axhline(y=0,c='black',zorder=0)
ax.set_xlabel('profiles',size = 16)
ax.set_ylabel("error difference",size = 16)
plt.ylim(-1,1)
plt.xticks(rotation=70)
plt.title(x)
ax.grid(True)
plt.savefig('../outputs/plots/bp6'+x+'.png', bbox_inches='tight')


# # Plot of Number of Segments vs Error

# In[510]:


RMSE_prof = pd.read_csv('../outputs/error_analysis/'+x+'_profile_RMSE.csv')
RMSE_prof = RMSE_prof.rename(columns={'Unnamed: 0':'Profile'})
number_seg = pd.read_csv('inputs/number_segments.csv')
RMSE_prof = pd.merge(RMSE_prof, number_seg, on='Profile', how='left')
RMSE_prof.to_csv('../outputs/error_analysis/'+x+'_profile_RMSE_segs.csv')


# dropping 8760 segment binding profile to make graph more readable
RMSE_prof = RMSE_prof.drop(2).reset_index(drop=True)

plt.clf()

fig, axis = plt.subplots()

axis.yaxis.grid(True)
axis.set_title('Error and time segment comparison')
axis.set_xlabel('Time Segments')
axis.set_ylabel('Error')

X = RMSE_prof['Segments']
Y = RMSE_prof['RMSE']
#n = RMSE_prof['Profile']

axis.scatter(X, Y)

#for line in range(0,RMSE_prof.shape[0]):
     #plt.text(X[line]+0.2, Y[line], n[line], 
             #horizontalalignment='left', size='medium', color='black', weight='semibold')

fig.savefig('../outputs/plots/segments_'+x+'.png', bbox_inches='tight')


# In[ ]:




