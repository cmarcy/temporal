#multiple clustering approaches

#importing packages needed for analysis
from sklearn.cluster import KMeans
from sklearn.cluster import AgglomerativeClustering
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
    k_hr = []

    unique_ID = pd.Series(lws['ID'].unique()).dropna()
    #TESTING: use lines below for testing, comment out for complete solve
    #unique_ID = unique_ID[0:1]

    #loop thru kmeans for each region
    for ID in unique_ID:
        kh = lws.copy()
        kh = kh[kh['ID']==ID]
        
        #create a kmeans fit to the data for each region
        kmeans = KMeans(n_clusters=seg_num)
        model = kmeans.fit(kh[fit_list])
        kh['ID'] = ID
        kh['Label'] = pd.Series(model.labels_, index=kh.index)
        k_hr.append(kh)
        
        #uncomment out print statement below to see progress of this loop, one print per region
        #print(ID + ' kmeans done')   

    print('end of loop')
    print()
    
    kh = pd.concat(k_hr)
    return kh

#alternative cluster approach, currently not used...
def cluster_alt(lws,seg_num,fit_list):
    print('start of loop... wait for it...')
    k_hr = []

    unique_ID = pd.Series(lws['ID'].unique()).dropna()
    #TESTING: use lines below for testing, comment out for complete solve
    #unique_ID = unique_ID[0:1]

    #loop thru kmeans for each region
    for ID in unique_ID:
        kh = lws.copy()
        kh = kh[kh['ID']==ID]
        
        #create a kmeans fit to the data for each region
        cluster = AgglomerativeClustering(n_clusters=seg_num, affinity='euclidean', linkage='ward')
        cluster_results = cluster.fit_predict(kh[fit_list])
        kh['ID'] = ID
        kh['Label'] = pd.Series(cluster_results, index=kh.index)
        k_hr.append(kh)
        
        #uncomment out print statement below to see progress of this loop, one print per region
        #print(ID + ' cluster done')   

    print('end of loop')
    print()
    
    kh = pd.concat(k_hr)
    return kh

# In[2]:

def merge_datasets(kh,seg_num,file_ID):
    #merge the fit the final datasets
    num = str(seg_num)
    kh = kh[['R_Subgroup','HOY','Label']]
    
    for x in ['load','solar','wind']:
        
        if x == 'load':
            dur = load_dur
            x_col = 'Load'
        elif x == 'solar':
            dur = solar_dur
            x_col = 'TRG_Eval'
        elif x == 'wind':
            dur = wind_dur
            x_col = 'TRG_Eval'
        
        reg_count = len(pd.Series(dur['Region'].unique()).dropna())
        kh2 = pd.merge(dur,kh,on=['R_Subgroup','HOY'],how='left')
        kf = kh2.groupby(['Region','Label'],as_index=False).agg({x_col:['count','mean']})
        kf.columns = ['Region','Label','Hour_Tot','Avg']
        kh3 = pd.merge(kh2,kf,on=['Region','Label'],how='left')
        print('number of segments for each region =',(kf.shape[0]/reg_count))
        print('number of rows for each region =',(kh3.shape[0]/reg_count))
        kh3.to_csv('../outputs/'+x+'/'+x+'_8760_'+file_ID+'_'+num+'.csv')
    
        print()
    
# In[3]:

seg_num_list = [6,10,15,24,40,73,120,146,292,438,730,1095,1752]#,2920,4380]

#TESTING: use lines below for testing, comment out for complete solve
seg_num_list = [24,48,72,96,120,144,168,192,216,240,360,480,600,720,840,960,1080,1200]

#best fit approach on a single dataset (load, wind, or solar)
print('start best fit approaches')
print()

#loop thru dataset list
for x_name in ['Load','Solar','Wind']:
	print(x_name)
	print()
    
    #initial setup
	lws = lwsset[['R_Subgroup','HOY','Load','Wind','Solar']].copy()
	lws['ID'] = lws['R_Subgroup']
	lws = lws.sort_values(['ID',x_name], ascending=[True,False])
	lws = lws.reset_index(drop=True)
	#lws['Order'] = ( ( lws.index + 8760 ) % 8760 ) + 1
	fit_list = [x_name]#,'Order']

    #loop thru segment list
	for i in seg_num_list:
		print(i,'number of segments')
		print()
        
        #apply cluster
		kh = cluster_alt(lws,i,fit_list)

        #merge data
		merge_datasets(kh,i,'Best-Fit-'+x_name)

print('completed best fit approaches')
print()
