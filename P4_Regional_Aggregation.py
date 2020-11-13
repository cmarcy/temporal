#Data Prep

print('start regional aggregations')
print()
#importing packages needed for analysis
import os
import shutil
import pandas as pd

#this code creates an output directory in the parent director, if one does not exist yet
path = os.getcwd()
parent = os.path.dirname(path)
outputs_dir = parent+'\outputs\\reg'
if not os.path.exists(outputs_dir):
    os.makedirs(outputs_dir)

#print('output files are written out in parent directory: '+outputs_dir)
#print()

load_dur = pd.read_csv('../outputs/load_long_format.csv')
solar_dur = pd.read_csv('../outputs/solar_long_format.csv')
wind_dur = pd.read_csv('../outputs/wind_long_format.csv')

# In[1]:

#Save a copy of the regional data since we are overwriting it below
files = ['../outputs/load_long_format.csv', 
         '../outputs/solar_long_format.csv', 
         '../outputs/wind_long_format.csv',
         '../outputs/8760_combo.csv']
for f in files:
    shutil.copy(f, '../outputs/reg')
    
#Aggregate regional data up to the ISO/Market Region
load = load_dur.copy()
aggregations1 = {'Load_Act':sum}
#md_sum = load.groupby(['R_Subgroup','Month','DOY'],as_index=False).agg(aggregations1)


#Aggregate regional data up to the Interconnect



#Aggregate regional data up to the National Level


print('finished regional aggregation')