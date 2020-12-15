#multiple clustering approaches

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

def cluster(lws,seg_num,fit_list):
    print('start of loop... wait for it...')
    k_fit = []
    k_hr = []
    avg_list = []

    unique_ID = pd.Series(lws['ID'].unique()).dropna()
    #TESTING: use lines below for testing, comment out for complete solve
    unique_ID = unique_ID[0:1]
    reg_count = len(pd.Series(unique_ID.unique()).dropna())

    #create a new list of column names with Avg
    for i in fit_list:
        n = 'Avg' + str(i)
        avg_list.append(n)
    
    #loop thru kmeans for each region
    for ID in unique_ID:
        kh = lws.copy()
        kh = kh[kh['ID']==ID]
        
        #create a kmeans fit to the data for each region
        kmeans = KMeans(n_clusters=seg_num)
        model = kmeans.fit(kh[fit_list])
        kf = pd.DataFrame(model.cluster_centers_, columns=avg_list)
        
        kf['ID'] = ID
        kh['ID'] = ID
        kf['Label'] = kf.index
        kh['Label'] = pd.Series(model.labels_, index=kh.index)
        
        #appends each regional fit/hourly data into one dataframe, includes cluster labels
        k_fit.append(kf)
        k_hr.append(kh)
        
        #uncomment out print statement below to see progress of this loop, one print per region
        print(ID + ' kmeans done')   
    
    k_fit = pd.concat(k_fit)
    k_hr = pd.concat(k_hr)
    print('end of loop')
    print()
    
    kf = k_fit.copy()
    kh = k_hr.copy()
    print('number of segments for each region =',(kf.shape[0]/reg_count))
    print('number of rows for each region =',(kh.shape[0]/reg_count))
    print()
    
    #find the number of hours in each segment
    drop_list = ['ID'] + fit_list 
    hr_cnt = kh.groupby(['ID','Label'],as_index=False).agg({'Season':['count']})
    hr_cnt.columns = hr_cnt.columns.droplevel(0)
    hr_cnt.columns = ['ID','Label','Hour_Tot']
    kf2 = pd.merge(kf,hr_cnt,on=['ID','Label'],how='left')
    kh2 = pd.merge(kh,kf2,on=['ID','Label'],how='left').drop(columns=drop_list)

    return kh2

# In[2]:

def merge_datasets(kh2,seg_num,file_ID):
    #merge the fit the final datasets
    num = str(seg_num)

    #load
    khl = kh2.rename(columns={'AvgLoad':'Avg'})
    khl = pd.merge(load_dur,khl,on=['R_Subgroup','HOY'],how='left').drop(columns=['Unnamed: 0','AvgWind','AvgSolar'])
    khl.to_csv('../outputs/load/load_8760_'+file_ID+'_'+num+'.csv')
    
    #solar - regrouping data because clustered at the IPM region level, but VRE data is at the IPM+state regional level
    khs = pd.merge(solar_dur,kh2,on=['R_Subgroup','HOY'],how='left').drop(columns=['Unnamed: 0','AvgLoad','AvgWind','AvgSolar'])
    khs2 = khs.groupby(['Region','Label'],as_index=False).agg({'TRG_Eval':['mean']})
    khs2.columns = ['Region','Label','Avg']
    khs3 = pd.merge(khs,khs2,on=['Region','Label'],how='left')
    khs3.to_csv('../outputs/solar/solar_8760_'+file_ID+'_'+num+'.csv')
    
    #wind - regrouping data because clustered at the IPM region level, but VRE data is at the IPM+state regional level
    khw = pd.merge(wind_dur,kh2,on=['R_Subgroup','HOY'],how='left').drop(columns=['Unnamed: 0','AvgLoad','AvgWind','AvgSolar'])
    khw2 = khw.groupby(['Region','Label'],as_index=False).agg({'TRG_Eval':['mean']})
    khw2.columns = ['Region','Label','Avg']
    khw3 = pd.merge(khw,khw2,on=['Region','Label'],how='left')
    khw3.to_csv('../outputs/wind/wind_8760_'+file_ID+'_'+num+'.csv')
    
    print('number of regions in load file:', khl.shape[0]/8760)
    print('number of regions in solar file:',khs3.shape[0]/8760)
    print('number of regions in wind file:', khw3.shape[0]/8760)
    print()
    
# In[3]:

seg_num_list = [6,10,15,24,40,73,146,292,438,730,1095,1752,2920,4380]

#TESTING: use lines below for testing, comment out for complete solve
seg_num_list = [6,10]


#three-way clustering approach on 8760 data
print('Start clustering approach')
print()

#Initial setup
lws = lwsset[['R_Subgroup','Season','HOY','Load','Wind','Solar']].copy()
lws['ID'] = lws['R_Subgroup']
fit_list = ['Load','Wind','Solar']

for i in seg_num_list:
    print(i,'number of segments')
    print()
    kh2 = cluster(lws,i,fit_list)
    merge_datasets(kh2,i,'Cluster')

print('completed clustering approach')
print()


# In[4]:

#best fit approach on a single dataset (load, wind, or solar)
print('start best fit approaches')
print()

#create the order column 
def order_col(lws,x_name):
    lws = lws.sort_values(['ID',x_name], ascending=[True,False])
    lws = lws.reset_index(drop=True)
    lws['Order'] = ( ( lws.index + 8760 ) % 8760 ) + 1
    return lws

#aggregate non-x_name data
def reagg(kh2,x_name):
    kh2[x_name] = kh2['Avg'+x_name]
    aggregations = {'Load':['count','mean'],'Wind':['mean'],'Solar':['mean']}
    avg_lws = kh2.groupby(['R_Subgroup','Label'],as_index=False).agg(aggregations)
    avg_lws.columns = avg_lws.columns.droplevel(0)
    avg_lws.columns = ['R_Subgroup','Label','Hour_Tot','AvgLoad','AvgWind','AvgSolar']
    print(avg_lws.columns)
    #merging to this dataset to get the label data to match to hourly
    kh3 = pd.merge(kh2[['R_Subgroup','Label','HOY']],avg_lws,on=['R_Subgroup','Label'],how='left')
    return kh3

# In[5]:

for x in ['Solar','Wind']:
	x_name = x
	print(x_name)
	print()
	outputs_x = outputs_dir+'/'+x_name

	lws = lwsset[['R_Subgroup','Season','HOY','Load','Wind','Solar']].copy()
	lws['ID'] = lws['R_Subgroup']
	fit_list = [x_name,'Order']

	lws = order_col(lws,x_name)

	for i in seg_num_list:
		print(i,'number of segments')
		print()
		kh2 = cluster(lws,i,fit_list)
		kh3 = reagg(kh2,x_name)
		merge_datasets(kh3,i,'B_'+x_name)

print('completed best fit approaches')
print()
