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
    #unique_ID = unique_ID[0:1]
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
        #print(ID + ' kmeans done')   

    print('end of loop')
    print()
    
    k_fit = pd.concat(k_fit)
    k_hr = pd.concat(k_hr)
    print('number of segments for each region =',(k_fit.shape[0]/reg_count))
    print('number of rows for each region =',(k_hr.shape[0]/reg_count))
    
    kh = pd.merge(k_hr,k_fit,on=['ID','Label'],how='left')
    return kh

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

seg_num_list = [6,10,15,24,40,73,120,146,292,438,730,1095,1752]#,2920,4380]

#TESTING: use lines below for testing, comment out for complete solve
seg_num_list = [6,10]

#best fit approach on a single dataset (load, wind, or solar)
print('start best fit approaches')
print()

#loop thru dataset list
for x in ['Load','Solar','Wind']:
	x_name = x
	print(x_name)
	print()
	outputs_x = outputs_dir+'/'+x_name
    
    #initial setup
	lws = lwsset[['R_Subgroup','HOY','Load','Wind','Solar']].copy()
	lws['ID'] = lws['R_Subgroup']
	lws = lws.sort_values(['ID',x_name], ascending=[True,False])
	lws = lws.reset_index(drop=True)
	lws['Order'] = ( ( lws.index + 8760 ) % 8760 ) + 1
	fit_list = [x_name,'Order']

    #loop thru segment list
	for i in seg_num_list:
		print(i,'number of segments')
		print()
        
        #apply cluster
		kh = cluster(lws,i,fit_list)
		print()
        
        #find the number of hours in each segment and average of data elements
		kh[x_name] = kh['Avg'+x_name]
		aggregations = {'Load':['count','mean'],'Wind':['mean'],'Solar':['mean']}
		avg_lws = kh.groupby(['R_Subgroup','Label'],as_index=False).agg(aggregations)
		avg_lws.columns = avg_lws.columns.droplevel(0)
		avg_lws.columns = ['R_Subgroup','Label','Hour_Tot','AvgLoad','AvgWind','AvgSolar']
		kh2 = pd.merge(kh[['R_Subgroup','Label','HOY']],avg_lws,on=['R_Subgroup','Label'],how='left')

        #merge data
		merge_datasets(kh2,i,'B'+x_name)

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
    kh = cluster(lws,i,fit_list)
    print()
    
    #find the number of hours in each segment
    hr_cnt = kh.copy()
    hr_cnt['Cnt_col'] = hr_cnt['ID']
    hr_cnt = hr_cnt.groupby(['ID','Label'],as_index=False).agg({'Cnt_col':['count']})
    hr_cnt.columns = hr_cnt.columns.droplevel(0)
    hr_cnt.columns = ['ID','Label','Hour_Tot']
    kh2 = pd.merge(kh,hr_cnt,on=['ID','Label'],how='left').drop(columns=['Load'])

    #merge data
    merge_datasets(kh2,i,'Cluster')

print('completed clustering approach')
print()


# In[5]:

print('start best day-type approach')
print()

day_num_list = [2,3,5,6,12,18,30,48,72]

#TESTING: use lines below for testing, comment out for complete solve
day_num_list = [6,12]

#loop thru dataset list
for x in ['Load','Solar','Wind']:
	x_name = x
	print(x_name)
	print()
	outputs_x = outputs_dir+'/'+x_name
    
    #initial setup
	lws = lwsset[['R_Subgroup','DOY','Hour','HOY','Load','Wind','Solar']].copy()
	lws['ID'] = lws['R_Subgroup']
    
	lws = pd.pivot_table(lws, values=x_name, index=['ID','R_Subgroup','DOY'],columns=['Hour'])
	lws.reset_index(inplace=True)
	lws.columns.name = None
	fit_list = list(range(1,25))

    #loop thru segment list
	for i in day_num_list:
		print(i,'number of segments')
		print()
        
        #apply cluster
		kh = cluster(lws,i,fit_list)

        #reshape data
		kh = kh.rename(columns={'Label':'D_Label'}).drop(columns=fit_list)
		kh2 = pd.melt(kh,id_vars=['ID','R_Subgroup','DOY','D_Label'],var_name='Hour',value_name='Avg'+x_name)
		kh2['Hour'] = pd.to_numeric(kh2['Hour'].str[3:])
		kh2['Label'] = kh2['D_Label'].map(str) + '_' + kh2['Hour'].map(str)
		reg_count = len(pd.Series(kh2['R_Subgroup'].unique()).dropna())
		print('number of rows for each region =',(kh2.shape[0]/reg_count))
		print()
        
        #add other dataelements back in
		lws2 = lwsset[['R_Subgroup','DOY','Hour','HOY','Load','Wind','Solar']].copy()
		kh3 = pd.merge(lws2,kh2,on=['R_Subgroup','DOY','Hour'],how='left')
        
        #find the number of hours in each segment and average of data elements
		aggregations = {'Load':['count','mean'],'Wind':['mean'],'Solar':['mean']}
		avg_lws = kh3.groupby(['R_Subgroup','Label'],as_index=False).agg(aggregations)
		avg_lws.columns = avg_lws.columns.droplevel(0)
		avg_lws.columns = ['R_Subgroup','Label','Hour_Tot','AvgLoad','AvgWind','AvgSolar']
		kh4 = pd.merge(kh3[['R_Subgroup','Label','HOY']],avg_lws,on=['R_Subgroup','Label'],how='left')
        
        #merge data
		merge_datasets(kh4,i,'KBDT'+x_name)

print('completed best day-type approach')
print()

# In[6]:

print('start cluster day-type approach')
print()
"""
#TESTING: use lines below for testing, comment out for complete solve
day_num_list = [6,12]

#initial setup
lws = lwsset[['R_Subgroup','DOY','Hour','HOY','Load','Wind','Solar']].copy()
lws['ID'] = lws['R_Subgroup']

pivot = []
fit_list = []

for x in ['Load','Solar','Wind']:
    lws2 = lws.copy()
    lws2['Hour']=x[0:1]+lws2['Hour'].map(str)
    piv = pd.pivot_table(lws2, values=x, index=['ID','R_Subgroup','DOY'],columns=['Hour'])
    piv.reset_index(inplace=True)
    piv.columns.name = None
    pivot.append(piv)
    
    n_list = list(range(1,25))
    f_list = [x[0:1] + str(n_list[i]) for i in range(len(n_list))] 
    fit_list.extend(f_list)

pivot = pd.concat(pivot)
lws = pivot.copy()
print(fit_list)

#loop thru segment list
for i in day_num_list:
    print(i,'number of segments')
    print()

    #apply cluster
    kh = cluster(lws,i,fit_list)

    #reshape data
    kh = kh.rename(columns={'Label':'D_Label'}).drop(columns=fit_list)
    kh2 = pd.melt(kh,id_vars=['ID','R_Subgroup','DOY','D_Label'],var_name='Hour',value_name='Avg'+x_name)
    kh2['Hour'] = pd.to_numeric(kh2['Hour'].str[3:])
    kh2['Label'] = kh2['D_Label'].map(str) + '_' + kh2['Hour'].map(str)
    reg_count = len(pd.Series(kh2['R_Subgroup'].unique()).dropna())
    print('number of rows for each region =',(kh2.shape[0]/reg_count))
    print()
    
    #add other dataelements back in
    lws2 = lwsset[['R_Subgroup','DOY','Hour','HOY','Load','Wind','Solar']].copy()
    kh3 = pd.merge(lws2,kh2,on=['R_Subgroup','DOY','Hour'],how='left')
    
    #find the number of hours in each segment and average of data elements
    aggregations = {'Load':['count','mean'],'Wind':['mean'],'Solar':['mean']}
    avg_lws = kh3.groupby(['R_Subgroup','Label'],as_index=False).agg(aggregations)
    avg_lws.columns = avg_lws.columns.droplevel(0)
    avg_lws.columns = ['R_Subgroup','Label','Hour_Tot','AvgLoad','AvgWind','AvgSolar']
    kh4 = pd.merge(kh3[['R_Subgroup','Label','HOY']],avg_lws,on=['R_Subgroup','Label'],how='left')
    
    #merge data
    merge_datasets(kh4,6,'KBDT'+x_name)
"""
print('completed best day-type approach')
print()
