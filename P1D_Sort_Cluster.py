#best fit approach on all three datasets
print('Start clustering approach')
print()
#importing packages needed for analysis
from sklearn.cluster import KMeans
import pandas as pd
import os

path = os.getcwd()
parent = os.path.dirname(path)
outputs_dir = parent+'\outputs'

load_dur = pd.read_csv('../outputs/load_long_format.csv')
solar_dur = pd.read_csv('../outputs/solar_long_format.csv')
wind_dur = pd.read_csv('../outputs/wind_long_format.csv')
lwsset = pd.read_csv('../outputs/8760_combo.csv')

# In[1]:

#Creates the Region + Season, (63*3=189 unique IDs)
lws = lwsset[['R_Subgroup','Season','HOY','Load','Wind','Solar']].copy()
lws['ID'] = lws['R_Subgroup']+'_'+lws['Season']
unique_ID = pd.Series(lws['ID'].unique()).dropna()
ID_count = unique_ID.shape[0]
reg_count = len(pd.Series(lwsset['R_Subgroup'].unique()).dropna())
print('number of regions (check):',ID_count/3,'=',reg_count)
print('number of regions X seasons:',ID_count)
print()

# In[2]:

def cluster(seg_num):
    print('start of loop... wait for it...')
    k_fit = []
    k_hr = []
    for ID in unique_ID:
        kh = lws.copy()
        kh = kh[kh['ID']==ID]
        
        #create a kmeans fit to the data for each region for each season (24 per season)
        kmeans = KMeans(n_clusters=seg_num)
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
        #if ID[-3:] == 'mer':
        #    print(ID + ' kmeans done')   
    
    k_fit = pd.concat(k_fit)
    k_hr = pd.concat(k_hr)
    print('end of loop')
    print()
    
    #s=season, a=all, f=fit, h=hourly
    kfas = k_fit.copy()
    khas = k_hr.copy()
    print('number of segments for each region =',(kfas.shape[0]/reg_count))
    print('number of rows for each region =',(khas.shape[0]/reg_count))
    print()
    
    #find the number of hours in each segment
    width = khas.groupby(['ID','Label'],as_index=False).agg({'Season':['count']})
    width.columns = width.columns.droplevel(0)
    width.columns = ['ID','Label','Hour_Tot']
    kfas2 = pd.merge(kfas,width,on=['ID','Label'],how='left')
    khas2 = pd.merge(khas,kfas2,on=['ID','Label'],how='left').drop(columns=['ID','Load','Wind','Solar'])
    
    #merge the fit the final datasets, note that we're matching fit R_subgroup (IPM) to the Region (IPM+State)
    num = str(seg_num*3)
    #load
    khasl = khas2.rename(columns={'AvgL':'Avg'})
    khasl2 = pd.merge(load_dur,khasl,on=['R_Subgroup','Season','HOY'],how='left').drop(columns=['Unnamed: 0','AvgW','AvgS'])
    khasl2.to_csv('../outputs/load/load_8760_k_seasons_all_'+num+'segs.csv')
    
    #solar
    khass = khas2.rename(columns={'AvgS':'Avg'})
    khass2 = pd.merge(solar_dur,khass,on=['R_Subgroup','Season','HOY'],how='left').drop(columns=['Unnamed: 0','AvgL','AvgW'])
    khass2.to_csv('../outputs/solar/solar_8760_k_seasons_all_'+num+'segs.csv')
    
    #wind
    khasw = khas2.rename(columns={'AvgW':'Avg'})
    khasw2 = pd.merge(wind_dur,khasw,on=['R_Subgroup','Season','HOY'],how='left').drop(columns=['Unnamed: 0','AvgL','AvgS'])
    khasw2.to_csv('../outputs/wind/wind_8760_clust_'+num+'segs.csv')
    
    print('number of regions in load file:', khasl2.shape[0]/8760)
    print('number of regions in solar file:', khass2.shape[0]/8760)
    print('number of regions in wind file:', khasw2.shape[0]/8760)
    print()
    
# In[3]:

seg_num_list = [4, 6, 8, 20, 16, 24, 32, 64, 128, 256]

for i in seg_num_list:
    print(i*3,'number of segments')
    print()
    cluster(i)

print('completed clustering approach')
