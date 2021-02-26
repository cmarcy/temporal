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

#original approach, currently not used...
def cluster(lws,seg_num,fit_list):
    print('start of loop... wait for it...')
    k_hr = []

    unique_ID = pd.Series(lws['ID'].unique()).dropna()
    #TESTING: use lines below for testing for a single region, comment out for complete solve
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

#alternative cluster approach
def cluster_alt(lws,seg_num,fit_list):
    print('start of loop... wait for it...')
    k_hr = []

    unique_ID = pd.Series(lws['ID'].unique()).dropna()
    #TESTING: use lines below for testing for a single region, comment out for complete solve
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

seg_num_list = [6,12,24,48,72,96,120,144,168,192,216,240,360,480,600,720,840,960,1080,1200]

#TESTING: use lines below for testing, comment out for complete solve
seg_num_list = [24,48,72]#,96,120,144,168,192,216,240,360,480,600,720,840,960,1080,1200]

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

# In[4]:

#three-way clustering approach on 8760 data
print('start clustering approach')
print()

#initial setup
lws = lwsset[['R_Subgroup','HOY','Load','Wind','Solar']].copy()
lws['ID'] = lws['R_Subgroup']
fit_list = ['Load','Wind','Solar']

#loop thru segment list
for i in seg_num_list:
    print(i,'number of segments')
    print()

    #apply cluster
    kh = cluster_alt(lws,i,fit_list)

    #merge data
    merge_datasets(kh,i,'3-Way-Cluster')

print('completed clustering approach')
print()


# In[5]:

day_num_list = [1,2,3,4,5,6,7,8,9,10,15,20,25,30,35,40,45,50]

#TESTING: use lines below for testing, comment out for complete solve
day_num_list = [1,2,3]#,4,5,6,7,8,9,10,15,20,25,30,35,40,45,50]

#best fit approach on a single dataset (load, wind, or solar) by day
print('start best day-type approach')
print()

#loop thru dataset list
for x in ['Load','Solar','Wind']:
    x_name = x
    print(x_name)
    print()
    
    #initial setup
    lws = lwsset[['R_Subgroup','DOY','Hour','HOY','Load','Wind','Solar']].copy()
    lws['ID'] = lws['R_Subgroup']
        
    lws = pd.pivot_table(lws, values=x_name, index=['ID','R_Subgroup','DOY'],columns=['Hour'])
    lws.reset_index(inplace=True)
    lws.columns.name = None
    fit_list = list(range(1,25))

    #loop thru segment list
    for i in day_num_list:
        print(i,'number of days')
        print(i*24,'number of segments')
        print()
        
        #apply cluster
        kh = cluster_alt(lws,i,fit_list)
        
        #reshape data
        kh = kh.rename(columns={'Label':'D_Label'})
        kh2 = pd.melt(kh,id_vars=['ID','R_Subgroup','DOY','D_Label'],var_name='Hour',value_name=x_name)
        kh2['Hour'] = pd.to_numeric(kh2['Hour'])
        kh2['Label'] = kh2['D_Label'].map(str) + '_' + kh2['Hour'].map(str)
        kh2['HOY'] = (kh2['Hour']) + (kh2['DOY'] - 1) * 24

        #merge data
        merge_datasets(kh2,i,'Clust-DT-'+x_name)

print('completed best day-type approach')
print()

# In[6]:

print('start cluster day-type approach')
print()

#initial setup
lws = lwsset[['R_Subgroup','DOY','Hour','Load','Wind','Solar']].copy()
lws['ID'] = lws['R_Subgroup']

pivot = lws[['ID','R_Subgroup','DOY']].drop_duplicates()
fit_list = []
n_list = list(range(1,25))

for x in ['Load','Solar','Wind']:
    lws2 = lws.copy()
    lws2['Hour']=x[0:1]+lws2['Hour'].map(str)
    piv = pd.pivot_table(lws2, values=x, index=['ID','R_Subgroup','DOY'],columns=['Hour'])
    piv.reset_index(inplace=True)
    piv.columns.name = None
    pivot=pd.merge(pivot,piv,on=['ID','R_Subgroup','DOY'],how='left')
    
    f_list = [x[0:1] + str(n_list[i]) for i in range(len(n_list))] 
    fit_list.extend(f_list)

lws = pivot.copy()

#loop thru segment list
for i in day_num_list:
    print(i,'number of days')
    print(i*24,'number of segments')
    print()

    #apply cluster
    kh = cluster_alt(lws,i,fit_list)

    #reshape data
    kh = kh.rename(columns={'Label':'D_Label'})
    col_list = ['ID','R_Subgroup','DOY','D_Label'] + ['L' + str(n_list[i]) for i in range(len(n_list))]
    kh = kh[col_list]
    kh2 = pd.melt(kh,id_vars=['ID','R_Subgroup','DOY','D_Label'],var_name='Hour',value_name='AvgLoad')
    kh2['Hour'] = pd.to_numeric(kh2['Hour'].str[1:])
    kh2['Label'] = kh2['D_Label'].map(str) + '_' + kh2['Hour'].map(str)
    kh2['HOY'] = (kh2['Hour']) + (kh2['DOY'] - 1) * 24
    
    #merge data
    merge_datasets(kh2,i,'Clust3-DT')

print('completed best day-type approach')
print()
