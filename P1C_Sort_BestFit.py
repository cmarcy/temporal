#best fit approach on a single dataset (load, wind, or solar)
print('start best fit approaches')
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
xs = lwsset[['R_Subgroup','Season','HOY','Load','Wind','Solar']].copy()
#note: originally I was clustering within a season, but for the paper, makes more sense to drop it
xs['ID'] = xs['R_Subgroup']#+'_'+xs['Season']
unique_ID = pd.Series(xs['ID'].unique()).dropna()
ID_count = unique_ID.shape[0]
reg_count = len(pd.Series(lwsset['R_Subgroup'].unique()).dropna())
#print('number of regions (check):',ID_count/3,'=',reg_count)
#print('number of regions X seasons:',ID_count)
#print()

# In[2]:

def bestfit(seg_num):
    print('start of loop... wait for it...')
    k_fit = []
    k_hr = []
    for ID in unique_ID:
        k = xs.copy()
        k = k[k['ID']==ID]
        
        #create the order column inside the loop since the n month number changes across seasons
        kh = k.sort_values([x_name], ascending=[False])
        kh = kh.reset_index(drop=True)
        kh['Order'] = kh.index+1.0
        
        #create a kmeans fit to the data for each region for each season (where n=24=72/3)
        kmeans = KMeans(n_clusters=seg_num)
        model = kmeans.fit(kh[[x_name,'Order']])
        kf = pd.DataFrame(model.cluster_centers_, columns=['Avg','Mid'])
    
        kf['ID'] = ID
        kh['ID'] = ID
        kf['Label'] = kf.index
        kh['Label'] = pd.Series(model.labels_, index=kh.index)
        
        #appends each regional fit/hourly data into one dataframe, includes cluster labels
        k_fit.append(kf)
        k_hr.append(kh)
        
        #uncomment out if&print statement below to see progress of this loop, one print per region
        #if ID[-3:] == 'mer':
        #    print(ID + ' kmeans done')   
    
    k_fit = pd.concat(k_fit)
    k_hr = pd.concat(k_hr)
    print('end of loop')
    print()
    
    #s=season, f=fit, h=hourly
    kfs = k_fit.copy()
    khs = k_hr.copy()
    print('number of segments for each region =',(kfs.shape[0]/reg_count))
    print('number of rows for each region =',(khs.shape[0]/reg_count))
    print()
    
    #aggregating the fit data again to get the resulting averages from the other non-fitted data
    #so if x_name is load, what are the averages for wind and solar in when fitting for load?
    aggregations = {'Load':['count','mean'],'Wind':['mean'],'Solar':['mean']}
    width = khs.groupby(['R_Subgroup','Season','Label'],as_index=False).agg(aggregations)
    width.columns = width.columns.droplevel(0)
    width.columns = ['R_Subgroup','Season','Label','Hour_Tot','Load','Wind','Solar']
    #merging to this dataset to get the label data to match to hourly
    khs2 = pd.merge(khs[['R_Subgroup','Season','Label','HOY']],width,on=['R_Subgroup','Season','Label'],how='left')
    
    #merge the fit the final datasets, note that we're matching fit R_subgroup (IPM) to the Region (IPM+State)
    num = str(seg_num)
    #load
    khsl = khs2.rename(columns={'Load':'Avg'})
    khsl2 = pd.merge(load_dur,khsl,on=['R_Subgroup','Season','HOY'],how='left').drop(columns=['Unnamed: 0','Wind','Solar'])
    khsl2.to_csv('../outputs/load/load_8760_B'+x_name+'_'+num+'.csv')
    
    #solar
    khss2 = pd.merge(solar_dur,khs2,on=['R_Subgroup','Season','HOY'],how='left').drop(columns=['Unnamed: 0','Load','Wind'])
    khss3 = khss2.groupby(['Region','Label'],as_index=False).agg({'TRG_Avg':['mean']})
    khss3.columns = ['Region','Label','Avg']
    khss = pd.merge(khss2,khss3,on=['Region','Label'],how='left')
    khss.to_csv('../outputs/solar/solar_8760_B'+x_name+'_'+num+'.csv')
    
    #wind
    khsw2 = pd.merge(wind_dur,khs2,on=['R_Subgroup','Season','HOY'],how='left').drop(columns=['Unnamed: 0','Load','Solar'])
    khsw3 = khsw2.groupby(['Region','Label'],as_index=False).agg({'TRG_Avg':['mean']})
    khsw3.columns = ['Region','Label','Avg']
    khsw = pd.merge(khsw2,khsw3,on=['Region','Label'],how='left')
    khsw.to_csv('../outputs/wind/wind_8760_B'+x_name+'_'+num+'.csv')
    
    print('number of regions in load file:', khsl2.shape[0]/8760)
    print('number of regions in solar file:', khss2.shape[0]/8760)
    print('number of regions in wind file:', khsw2.shape[0]/8760)
    print()

# In[3]:

seg_num_list = [6,10,15,24,40,73,146,292,438,730,1095,1752,2920,4380]
#TESTING: use line below for testing, comment out for complete solve
seg_num_list = [6,10]

x_name = 'Load'
print(x_name)
print()
outputs_x = outputs_dir+'/'+x_name

for i in seg_num_list:
    print(i,'number of segments')
    print()
    bestfit(i)

# In[4]:

x_name = 'Solar'
print(x_name)
print()
outputs_x = outputs_dir+'/'+x_name

for i in seg_num_list:
    print(i,'number of segments')
    print()
    bestfit(i)

# In[5]:

x_name = 'Wind'
print(x_name)
print()
outputs_x = outputs_dir+'/'+x_name

for i in seg_num_list:
    print(i,'number of segments')
    print()
    bestfit(i)

print('completed best fit approaches')
print()