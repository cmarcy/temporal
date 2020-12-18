#IPM Approach (2 profiles) and Sequential Approach (Many profiles) 

print('start IPM model approach')
print()
#importing packages needed for analysis
import os
import pandas as pd

path = os.getcwd()
parent = os.path.dirname(path)
outputs_dir = parent+'\outputs'

load_dur = pd.read_csv('../outputs/load_long_format.csv')
solar_dur = pd.read_csv('../outputs/solar_long_format.csv')
wind_dur = pd.read_csv('../outputs/wind_long_format.csv')

# In[1]:

def ipm_approach(x):
    print(x_name)
    print()
    
    #Initial directory and dataset
    outputs_x = outputs_dir+'/'+x_name
    if not os.path.exists(outputs_x):
        os.makedirs(outputs_x)
    #print('output files are written out in parent directory: '+outputs_x)
    #print()
    
    unique_r = pd.Series(x['Region'].unique()).dropna()
    x = x[['Region','R_Group','R_Subgroup','Season','Month','DOY','Hour','HOY','Load_Act',x_column]]
    print('Number of regions in initial dataset',len(unique_r))
    
    #Assign the time of day (TOD) categories to hours
    tod = pd.read_csv('inputs/time_of_day.csv')
    x2 = pd.merge(x,tod,on='Hour',how='left')
    
    #Assign the Group categories to season/hours, filter on a single region, get counts by season
    seasons = x2[x2['Region']==x2['Region'].iloc[0]]
    seasons = seasons[['Season','HOY']]
    season_count = seasons.groupby('Season',as_index=False).count().rename(columns={'HOY':'Season_Tot'})
    season_count = season_count.sort_values('Season')
    
    #creates a DF with the number of hours in each season+group combo
    group = pd.read_csv('inputs/group_shares.csv')
    for i in season_count.index:
        group[season_count.iloc[i,0]] = group['Share']*season_count.iloc[i,1]
    group_sea = pd.melt(group,id_vars=['Group','Share'],var_name='Seasons',value_name='Sea-G_Tot')
    group_sea['Season_Counter'] = group_sea['Sea-G_Tot'].cumsum()
    group_sea['Season_Counter'] = round(group_sea['Season_Counter'])
    
    #Create a dataframe with 8760 numbers
    unique_hc = pd.Series(x['HOY'].unique()).dropna()
    unique_hc = pd.DataFrame(unique_hc,columns=['Season_Counter'])
    unique_hc['Season_Counter'] = unique_hc['Season_Counter']*1.0
    season_8760 = pd.merge_asof(unique_hc, group_sea, on='Season_Counter', direction='forward')
    
    #Assign the load group categories to each of the 8760 hours
    x2 = x2.sort_values(['Region','Season','Load_Act'], ascending=[True, True, False])
    x2 = x2.reset_index(drop=True)
    x2['Season_Counter'] = ( ( x2.index + 8760 ) % 8760 ) + 1
    x2['Season_Counter'] = x2['Season_Counter'].astype(int)
    
    #Merge seasonal group data to the full dataframe
    x3 = pd.merge(x2,season_8760,on='Season_Counter',how='left')
    x3 = x3.drop(columns=['Season_Counter','Share','Seasons','Sea-G_Tot'])
    x3 = x3.sort_values(['Region','Season','Group','TOD'])
    
    aggregations = {x_column:['count','mean']}
    norm = x3.groupby(['Region','Season','Group','TOD'],as_index=False).agg(aggregations)
    norm.columns = norm.columns.droplevel(0)
    norm.columns = ['Region','Season','Group','TOD','Hour_Tot','Avg']
    print('number segments for each region =',norm.shape[0]/len(unique_r))
    
    x4 = pd.merge(x3,norm,on=['Region','Season','Group','TOD'],how='left')
    x4 = x4.sort_values(['Region',x_column])
    print('number of rows in dataset for each region =',x4.shape[0]/len(unique_r))
    x4.to_csv('../outputs/'+x_name+'/'+x_name+'_8760_IPM.csv')
    print()

# In[2]:

print('IPM Approach (Max of 72 Representative Hours)')
#Details: Split into 3 seasons, then 6 groups, then 4 times of day
print()

x_name = 'load'
x_column = 'Load'
ipm_approach(load_dur)

x_name = 'solar'
x_column = 'TRG_Eval'
ipm_approach(solar_dur)

x_name = 'wind'
x_column = 'TRG_Eval'
ipm_approach(wind_dur)

# In[3]:

def alt_ipm_approach(x):
    print(x_name)
    print()
    
    unique_r = pd.Series(x['Region'].unique()).dropna()
    x = x[['Region','R_Group','R_Subgroup','Season','Month','DOY','Hour','HOY','Load_Act',x_column]]
    print('Number of regions in initial dataset',len(unique_r))

    #Assign the time of day (TOD) categories to hours
    tod = pd.read_csv('inputs/time_of_day.csv')
    tod_x = pd.merge(x,tod,on='Hour',how='left')
    
    #Assign the Group categories to season/hours
    seasons = tod_x[tod_x['Region']==tod_x['Region'].iloc[0]]
    seasons = seasons[['Season','TOD','HOY']]
    
    #get the number of hours in each season
    season_count = seasons.groupby(['Season','TOD'],as_index=False).count().rename(columns={'HOY':'ST_Tot'})
    season_count = season_count.sort_values('Season')
    season_count['Sea_TOD'] = season_count['Season']+"_"+season_count['TOD']
    
    #for each Sea_TOD create a column in the group DF, determines number of hours in STG combo
    group = pd.read_csv('inputs/group_shares.csv')
    for i in season_count.index:
        group[season_count.iloc[i,3]] = group['Share']*season_count.iloc[i,2]
    
    group_STG = pd.melt(group,id_vars=['Group','Share'],var_name='Sea_TOD',value_name='STG_Tot')
    group_STG['STG_Counter'] = group_STG['STG_Tot'].cumsum()
    group_STG['STG_Counter'] = round(group_STG['STG_Counter'])
    group_STG['STG_ID'] = group_STG['Sea_TOD']+"_"+group_STG['Group'].astype(str)  
    
    #Create a dataframe with 8760 numbers
    unique_hc2 = pd.Series(x['HOY'].unique()).dropna()
    unique_hc2 = pd.DataFrame(unique_hc2,columns=['STG_Counter'])
    unique_hc2['STG_Counter'] = unique_hc2['STG_Counter']*1.0
    STG_8760 = pd.merge_asof(unique_hc2, group_STG, on='STG_Counter', direction='forward')

    #Assign the load group categories to each of the 8760 hours
    tod_x = tod_x.sort_values(['Region','Season','Load_Act'], ascending=[True, True, False])
    tod_x = tod_x.reset_index(drop=True)
    tod_x['STG_Counter'] = ( ( tod_x.index + 8760 ) % 8760 ) + 1
    tod_x['STG_Counter'] = tod_x['STG_Counter'].astype(int)
    
    #Merge seasonal group data to the full dataframe
    tod_x_2 = pd.merge(tod_x,STG_8760,on='STG_Counter',how='left')
    tod_x_2 = tod_x_2.drop(columns=['STG_Counter','Share','Sea_TOD','STG_Tot'])
    tod_x_2 = tod_x_2.sort_values(['Region','Season','TOD','Group'])
        
    aggregations = {x_column:['count','mean']}
    case2 = tod_x_2.groupby(['Region','STG_ID'],as_index=False).agg(aggregations)
    case2.columns = case2.columns.droplevel(0)
    case2.columns = ['Region','STG_ID','Hour_Tot','Avg']
    print('number of segments for each region =',case2.shape[0]/len(unique_r))
    
    tod_x_3 = pd.merge(tod_x_2,case2,on=['Region','STG_ID'],how='left')
    tod_x_3 = tod_x_3.sort_values(['Region',x_column])
    print('number of rows in dataset =',tod_x_3.shape[0]/len(unique_r))
    tod_x_3.to_csv('../outputs/'+x_name+'/'+x_name+'_8760_IPMalt.csv')
    print()

# In[3]:

print('Alternative IPM Approach (72 Representative Hours)')
# ### Details: Regions will be split first by season, then time of day, then group
print()

x_name = 'load'
x_column = 'Load'
alt_ipm_approach(load_dur)

x_name = 'solar'
x_column = 'TRG_Eval'
alt_ipm_approach(solar_dur)

x_name = 'wind'
x_column = 'TRG_Eval'
alt_ipm_approach(wind_dur)

print('completed IPM model approach')
print()