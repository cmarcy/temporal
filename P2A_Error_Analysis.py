#Error Analysis on 8760 profiles 
print('start error analysis')
print()
#importing packages needed for analysis
import os
import pandas as pd

path = os.getcwd()
parent = os.path.dirname(path)
outputs_dir = parent+'\outputs\error_analysis'
if not os.path.exists(outputs_dir):
    os.makedirs(outputs_dir)
#print('output files are written out in parent directory: '+outputs_dir)

# In[1]:

def error(x,x2):
    outputs_dir_x = parent+'\outputs\error_analysis/'+x
    if not os.path.exists(outputs_dir_x):
        os.makedirs(outputs_dir_x)
    
    print(x)
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
    
    filelist = []
    files = os.listdir(parent+'/outputs/'+x)
    for f in files:
        filelist.append(f)
    print()
    
    # ## Loop and create error files 
    reg_RMSE = regions
    profile_diff = RH_list
    prof_RMSE = {}
    print('start of loop... wait for it...')
    for i in filelist:
        # n = name of the profile type
        y=len(x)+6 # y = the position of "wind_8760_"
        n=i[y:-4] # -4 = the position of ".csv"
        
        stat = pd.read_csv('../outputs/'+x+'/'+i)
        stat = stat[['Region','Season','Month','DOY','HOY','Hour_Tot',x2,'Avg']]
        stat['Diff'] = stat[x2] - stat['Avg'] 
        
        #rows are reg+HOY, columns are the differences calculated for each profile
        #note: creates a very large file, commenting out here and at the end of the loop
        #diff = stat[['Region','HOY','Diff']].copy().rename(columns={'Diff':n})
        #diff['reg_hr'] = diff['Region'] + '-' + diff['HOY'].astype(str)
        #diff = diff.drop(columns={'Region','HOY'})
        #profile_diff = pd.merge(profile_diff, diff, on='reg_hr', how='left')
        
        # regional RMSE
        stat['Square'] = stat['Diff']**2
        stat_reg = stat.groupby('Region',as_index=False).agg({'Square' : sum})
        stat_reg = stat_reg.rename(columns={'Square':'Square_Sum'})
        stat_reg['Mean'] = stat_reg['Square_Sum'] / 8760 
        stat_reg[n] = stat_reg['Mean']**(1/2)
        stat_reg = stat_reg.drop(columns={'Square_Sum','Mean'})
        reg_RMSE = pd.merge(reg_RMSE, stat_reg, on='Region', how='left')
        
        # profile RMSE
        stat2 = stat.copy()
        stat2 = stat2.rename(columns={'Square':'RMSE'})
        stat2['RMSE'] = stat2['Diff']**2
        stat2 = stat2.agg({'RMSE' : sum})
        stat2 = stat2 / (8760*len(regions)) 
        stat2[n] = stat2**(1/2)
        stat2[n] = stat2[n].values
        prof_RMSE.update({n : (stat2[n])})
        
        #print(x, n, 'error calculated')
    
    print('end of loop')
    print()
    
    #profile_diff.to_csv('../outputs/error_analysis/'+x+'_'+'profile_diff.csv')
    reg_RMSE.to_csv('../outputs/error_analysis/'+x+'_'+'regional_RMSE.csv')
    profile_df = pd.DataFrame.from_dict(prof_RMSE,orient='index').rename(columns={0:'RMSE'})
    profile_df.to_csv('../outputs/error_analysis/'+x+'_'+'profile_RMSE.csv')

# In[2]:

def segs(x):
    print('create segment count')
    print()
    from pandas import DataFrame
    profilelist = []
    files = os.listdir(parent+'/outputs/'+x)
    for f in files:
        # n = name of the profile type
        y=len(x)+6 # y = the position of "wind_8760_"
        n=f[y:-4] # -4 = the position of ".csv"
        profilelist.append(n)
    #print(profilelist)
    seg_count = DataFrame(profilelist,columns=['Profile'])
    split = seg_count['Profile'].str.split('_', n = 1, expand = True) 
    seg_count['Group'] = split[0]
    seg_count['P_ID'] = split[1]
    seg_count['Group1'] = seg_count['Group'].str[:1]
    #segment count for clustering
    seg_count.loc[seg_count['Group1'] == 'B', 'Segments'] = seg_count['P_ID']
    seg_count.loc[seg_count['Group1'] == '3', 'Segments'] = seg_count['P_ID']
    seg_count.loc[seg_count['Group1'] == 'C', 'D'] = seg_count['P_ID']
    seg_count.loc[seg_count['Group1'] == 'C', 'Segments'] = pd.to_numeric(seg_count['P_ID'], errors = 'coerce')*24
    #segment count for sequential
    seg_count.loc[seg_count['Group1'] == 'S', 'Int'] = seg_count['P_ID'].str[:-3]
    seg_count.loc[seg_count['Group1'] == 'S', 'Segments'] = 8760/pd.to_numeric(seg_count['Int'])
    #segment count for daytype
    seg_count.loc[seg_count['Group1'] == 'D', 'M'] = seg_count['P_ID'].str[1:3]
    seg_count.loc[seg_count['Group1'] == 'D', 'D'] = seg_count['P_ID'].str[4:5]
    seg_count.loc[seg_count['Group1'] == 'D', 'H'] = seg_count['P_ID'].str[6:]
    seg_count.loc[seg_count['Group1'] == 'D', 'Segments'] = \
        pd.to_numeric(seg_count['M'])*pd.to_numeric(seg_count['D'])*pd.to_numeric(seg_count['H'])
    #segment count for IPM
    seg_count.loc[seg_count['Profile'] == 'IPM', 'Segments'] = 67
    seg_count.loc[seg_count['Profile'] == 'IPMalt', 'Segments'] = 72
    seg_count = seg_count[['Profile','Group','Segments']]
    #print(seg_count)
    seg_count.to_csv('../outputs/error_analysis/number_segments.csv')

segs('load')

# In[3]:

error('load','Load')
error('solar','TRG_Eval')
error('wind','TRG_Eval')

print('end of error analysis')
print()