#Error Analysis on 8760 profiles 
print('start error analysis')
print()
#importing packages needed for analysis
import os
import pandas as pd

#os.chdir('C:\\Users\\cmarcy\\Desktop\\py_projects\\temporal\\py\\code_inputs')
path = os.getcwd()
parent = os.path.dirname(path)
outputs_dir = parent+'\outputs\error_analysis'
if not os.path.exists(outputs_dir):
    os.makedirs(outputs_dir)
print('output files are written out in parent directory: '+outputs_dir)

# In[1]:

#create the hour sets to evaluate

#read in the dataset that combineds load, wind and solar data at the R_Subregion level
combo = pd.read_csv('../outputs/8760_combo.csv').drop(columns={'Unnamed: 0'})
#print(combo.head(2))
#print()

#Creating scoring metric for setting up the critical hours
#Low scoring is opposite of high scoring
combo['LL']=1-combo['Load']
combo['LW']=1-combo['Wind']
combo['LS']=1-combo['Solar']

#High Load (created because we drop the column in the loop and we want to keep Load)
combo['HL']=combo['Load']

#define the critical hour sets under review
#edge solutions of interest
combo['LLHW'] = combo['LL']  +combo['Wind']
combo['LLHS'] = combo['LL']  +combo['Solar']

#corner solutions of interest
combo['HLHWHS']=combo['Load']+combo['Wind']+combo['Solar']
combo['HLHWLS']=combo['Load']+combo['Wind']+combo['LS']
combo['HLLWHS']=combo['Load']+combo['LW']  +combo['Solar']
#combo['HLLWLS']=combo['Load']+combo['LW']  +combo['LS']
combo['LLHWHS']=combo['LL']  +combo['Wind']+combo['Solar']
combo['LLHWLS']=combo['LL']  +combo['Wind']+combo['LS']
combo['LLLWHS']=combo['LL']  +combo['LW']  +combo['Solar']
#combo['LLLWLS']=combo['LL']  +combo['LW']  +combo['LS']

n = combo.columns.get_loc('HL')
lws_list = combo.columns[n:]
#print(lws_list)

#Calculate the top 10% of hours for the high load event and low load + high RE events
lws_10 = lws_list[0:3]
lws_10_t = []
for col in lws_10:
    #print(col)
    combo = combo.sort_values(by=['Region',col],ascending=[True,False])
    combo = combo.reset_index(drop=True)
    combo['counter'] = ( ( combo.index + 8760 ) % 8760 ) + 1
    combo['counter'] = combo['counter'].astype(int)
    combo[col+'_t']= combo['counter']<877
    combo = combo.drop(columns=col)
    lws_10_t.append(col+'_t')

#Calculate the top 10% of hours for the combination of the high RE corner (6) events
lws_6c = lws_list[3:]
lws_6c_t = []
for col in lws_6c:
    #print(col)
    combo = combo.sort_values(by=['Region',col],ascending=[True,False])
    combo = combo.reset_index(drop=True)
    combo['counter'] = ( ( combo.index + 8760 ) % 8760 ) + 1
    combo['counter'] = combo['counter'].astype(int)
    combo[col+'_t']= combo['counter']<147
    combo = combo.drop(columns=col)
    lws_6c_t.append(col+'_t')

#create a corner set that combined the 6 corners identified above
combo['corners_t']=combo[lws_6c_t].any(axis = 1) 

#final dataset that identifies critical hours by their set
crit_hr=combo.copy()
crit_hr=crit_hr.drop(columns=['LL', 'LW', 'LS', 'counter'])
crit_hr['ALL_t']=True
crit_hr.to_csv('../outputs/critical_hours.csv')
#print(crit_hr.columns)
#print()

#creates a list of the critical hour column set names for future loops
#note: commented out reviewing the 6 corners individually for now (see below)
lws_t = lws_10_t #+ lws_6c_t
lws_t.append('corners_t')
lws_t.append('ALL_t')
print(lws_t)
print()

# In[2]:

def error(x,x2):
    outputs_dir_x = parent+'\outputs\error_analysis/'+x
    if not os.path.exists(outputs_dir_x):
        os.makedirs(outputs_dir_x)
    
    print(x, 'setup')
    print()
    
    # DF to use for regional error analysis, like RMSE and number of representative hours 
    long = pd.read_csv('../outputs/'+x+'_long_format.csv')
    regions = long['Region'].unique()
    regions = pd.DataFrame(regions).rename(columns={0:'Region'})
    print(len(regions))
    
    reg_hr = long[['Region','HOY']].copy()
    reg_hr['reg_hr'] = reg_hr['Region'] + '-' + reg_hr['HOY'].astype(str)
    RH_list = reg_hr['reg_hr'].unique()
    RH_list = pd.DataFrame(RH_list).rename(columns={0:'reg_hr'})
    print(len(RH_list)/8760)
    print()
    
    filelist = []
    files = os.listdir(parent+'/outputs/'+x)
    for f in files:
        filelist.append(f)
    
    #Keeping R_subgroup and HOY to merge with datasets, don't need additional columns
    crit_hr2 = crit_hr.copy().drop(columns=['Region','R_Group','Season','Month','DOY','Hour','Load_Act','Load','Wind','Solar'])
    
    #creates a template for setting up the loop further below
    diff = pd.read_csv('../outputs/'+x+'/'+filelist[0])
    diff = diff[['Region','R_Subgroup','Season','Month','DOY','HOY',x2]]
    
    #adds the critical hoursets for the template
    diff = pd.merge(diff, crit_hr2, on=['R_Subgroup','HOY'], how='left')
    
    #test list for testing
    #filelist = filelist[0:3]
    #print(filelist)
    
    #test list for testing
    #lws_t = lws_t[0:3]
    #print(lws_t)
    
    #loop reads through each profile dataset and finds diff value for each hourset
    for i in filelist:
        y=len(x)+6
        n=i[y:-4]
        #print(n)
    
        #reads in each of the 8760 profile data (~25 files total)
        df = pd.read_csv('../outputs/'+x+'/'+i)
        df = df[['Region','HOY','Avg']]
        
        #merges the profile data with the critical hour data
        diff = pd.merge(diff,df, on=['Region','HOY'], how='left')
    
        #calculates the difference between the raw data and the profile data for each set of critical hours
        for col in lws_t:
            m=col.split('_',1)[0]
            #print(col)
            diff.loc[diff[col] == True, n+'-'+m] = diff[x2] - diff['Avg']
        diff=diff.drop(columns=['Avg'])
    #diff.to_csv('../outputs/crit_hr/'+x+'diff.csv')
    
    #loop calculates RMSE for each profile + critical hour set
    prof_RMSE = {}
    stat = diff.copy()
    #print(stat.columns)
    #print()
    for i in stat.columns[18:]:
        #print(i)
        
        stat2 = stat.copy()
        stat2['RMSE'] = stat2[i]**2
        stat2 = stat2.agg({'RMSE' : sum})
        stat2 = stat2 / (8760*len(regions)) 
        stat2[i] = stat2**(1/2)
        stat2[i] = stat2[i].values
        prof_RMSE.update({i : (stat2[i])})
    
    #puts the RMSE data for each profile+critical hour set into a DF and exports it
    profile_df = pd.DataFrame.from_dict(prof_RMSE,orient='index').rename(columns={0:'RMSE'})
    profile_df['ID']=profile_df.index
    profile_df['Profile']=profile_df['ID'].str.split('-',1,expand=True)[0]
    profile_df['Crit_Hr']=profile_df['ID'].str.split('-',1,expand=True)[1]
    profile_df=profile_df.drop(columns={'ID'})
    profile_df.to_csv('../outputs/error_analysis/'+x+'_'+'profile_RMSE_2.csv')
    
    #Add Segments to dataset
    number_seg = pd.read_csv('inputs/number_segments.csv')
    profile_df2 = pd.merge(profile_df, number_seg, on='Profile', how='left')
    profile_df2.to_csv('../outputs/error_analysis/'+x+'_profile_RMSE_2_segs.csv')
    #print(profile_df2.head(8))
    print()
    
# In[3]:

##UNCOMMENT WHICH PROFILE BEING ANALYZED 
error('load','Load')
error('solar','TRG_Avg')
error('wind','TRG_Avg')
print('finished error analysis')
