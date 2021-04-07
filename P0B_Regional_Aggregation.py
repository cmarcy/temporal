#Data Prep

print('start initial data read')
print()
#importing packages needed for analysis
import os
import pandas as pd

#this code creates an output directory in the parent director, if one does not exist yet
path = os.getcwd()
parent = os.path.dirname(path)
outputs_dir = parent+'\outputs'
if not os.path.exists(outputs_dir):
    os.makedirs(outputs_dir)

outputs_load = outputs_dir+'/load'
if not os.path.exists(outputs_load):
    os.makedirs(outputs_load)
    os.makedirs(outputs_dir+'/solar')
    os.makedirs(outputs_dir+'/wind')

print('output files are written out in parent directory: '+outputs_dir)
print()

# In[1]:

def cleandata(raw):
    #create copy to make changes on
    x = raw.copy()
    print('number of rows in initial dataset (including CN) =',x.shape[0])
    
    #Regions: Creating Regional IDs
    unique_r = pd.Series(x['Region'].unique()).dropna()
    rl = unique_r.str.split("_",n=1,expand=True)
    rl[2] = unique_r
    print('number of regions in initial dataset (including CN) =',unique_r.shape[0])
    
    #Regions: Cleaning up missing subgroups
    rl.loc[rl[0] == 'NENGREST', 1] = 'REST'
    rl.loc[rl[0] == 'FRCC', 1] = 'FRCC'
    
    #Regions: Cleaning up the misnamed groups
    rl[0] = rl[0].replace('NENGREST','NENG')
    rl[0] = rl[0].replace('WECC','WEC')
    unique_g = pd.Series(rl[0].unique()).dropna()
    #print('number of regional groups in dataset (including CN) =',unique_g.shape[0])
    rl.rename(columns={0 : 'R_Group', 1: 'Drop', 2:'Region'},inplace=True)
    rl = rl[['R_Group','Region']]
    
    #Regions: Merging Regional Data to DF
    x = pd.merge(rl,x,on='Region',how='right')
    
    #Regions: For load dataset r_subgroup = region, for wind and solar this will include state ID
    x['R_Subgroup']=x['Region']
    if 'State' in x.columns:
        x['Region'] = x['State'] + "_" + x['R_Subgroup'] 
        unique_r = pd.Series(x['Region'].unique()).dropna()
        print('number of regions (REG+ST) in dataset (including CN) =',unique_r.shape[0])

    #Regions: Removing Canada
    x = x[x['R_Group']!="CN"]
    #print()
    #print('number of rows in dataset after removing CN =',x.shape[0])
    unique_r = pd.Series(x['Region'].unique()).dropna()
    #print('number of regions in dataset (excluding CN) =',unique_r.shape[0])
    unique_g = pd.Series(x['R_Group'].unique()).dropna()
    #print('number of regional groups in dataset (excluding CN) =',unique_g.shape[0])
    print()
    
    #Regions: Add lookup data for alternative regional aggregation
    x['R_IPM']=x['R_Subgroup']
    x = x.drop(columns = ['R_Subgroup'])
    #r_agg = pd.read_csv('inputs/NERC_regions.csv') 
    r_agg = pd.read_csv('inputs/NERC_regions2.csv') 
    x = pd.merge(x,r_agg,on='R_IPM',how='left')
    
    #Regions: for testing only, otherwise comment out the line below
    #NOTE: use FRCC for one region, ERC for two regions
    #x = x[x['R_Group']=="ERC"]
    
    #Time: Add season id
    if 'Season' in x.columns:
        x = x.drop(columns={'Season'})
    season_month = pd.read_csv('inputs/season_months.csv')
    x = pd.merge(x,season_month, on='Month', how='left')
    
    #Time: Distinguishes between Day of the Month and Day of the Year
    unique_d = pd.Series(x['Day'].unique()).dropna()    
    max_d = max(unique_d)
    if max_d >= 365:
        x = x.rename(columns={'Day':'DOY'})
    else:
        days = pd.read_csv('inputs/days_365.csv').drop(columns=['Week'])
        x = pd.merge(days,x,on=['Month','Day'],how='right')

    #Time: Removing Hour string from column name
    melt_list = [x for x in x.columns if not 'Hour' in x]
    x.columns = x.columns.str.replace('Hour ', '')
    
    #Time: melt function converts values in wide format to long format
    x = pd.melt(x,id_vars=melt_list,var_name='Hour',value_name='Value')

    #Time: add an hour counter
    x['Hour'] = pd.to_numeric(x['Hour'],errors='coerce')
    x['HOY'] = (x['Hour']) + (x['DOY'] - 1) * 24
    x = x.sort_values(by=['Region','HOY'])
    #unique_hc = pd.Series(x['HOY'].unique()).dropna()

    #Value: set as numeric and scale values
    if x['Value'].dtypes == object:
        x['Value'] = pd.to_numeric(x['Value'].str.replace(",",""),errors='coerce')
    x['Value']=x['Value']/1000
    
    return x

# In[2]:

print('LOAD PROCESSING')
print()
load_raw = pd.read_csv('inputs/raw/table_2-2_load.csv')
load_dur = cleandata(load_raw)

#Scale Load as a function of the peak hour in each region
load_dur = load_dur.rename(columns={'Value':'Load_Act'})
peak = load_dur.groupby(['Region'],as_index=False).agg({'Load_Act':max})
peak = peak.rename(columns={'Load_Act':'Load_Peak'})
load_dur = pd.merge(load_dur,peak, on='Region', how='left')
load_dur['Load'] = load_dur['Load_Act'] / load_dur['Load_Peak']

#organized long format data to new csv file
load_dur = load_dur[['Region','R_Group','R_Subgroup','Season','Month','DOY','Hour','HOY','Load_Act','Load']]
load_dur.to_csv('../outputs/load_long_format.csv')
print('number of rows in final load dataset =',load_dur.shape[0])
print('number of regs in final load dataset =',load_dur.shape[0]/8760)
unique_r = pd.Series(load_dur['Region'].unique()).dropna()
print('number of hours for each regs in final load dataset =',load_dur.shape[0]/unique_r.shape[0])

print()

# In[3]:

def vreclean(vre):
    #create DF that only has the labels, easier to merge onto
    vre = vre.rename(columns={'Resource Class':'TRG'})
    trgset = vre.copy().drop(columns={'TRG','Value'})
    trgset = trgset.drop_duplicates()
    merge_list = list(trgset.columns)

    #loops through the TRGs, creates a column for each one, decreases the number of rows in DF
    unique_trg = pd.Series(vre['TRG'].unique()).dropna()
    for n in range(min(unique_trg),max(unique_trg)+1):
        subset = vre.loc[vre['TRG'] == n].reset_index(drop=True)
        subset = subset.rename(columns={'Value':'TRG'+str(n)}).drop(columns={'TRG'})
        trgset = pd.merge(trgset,subset,on=list(merge_list),how='left')
        
    #adds new columns: average TRG column and actual load column
    trg_list = [i for i in trgset.columns if 'TRG' in i]
    trgset['TRG_Avg'] = trgset[trg_list].mean(axis=1)
    l_col = load_dur[['Region','HOY','Load_Act']].rename(columns={'Region':'R_IPM'})
    out = pd.merge(trgset,l_col,on=['R_IPM','HOY'],how='left')
    
    #adds new column: best TRG, fills in column with lowest available TRG value
    out['TRG_Best'] = out['TRG' + str(min(unique_trg))]
    trg_range = range(min(unique_trg),max(unique_trg)+1)
    for n in trg_range:
        out['TRG_Best'].fillna(out['TRG' + str(n)], inplace=True)

    #adds new column: best TRG ID, fills in column with cooresponding TRG category
    trg_reverse = trg_range[::-1]
    for n in trg_reverse:
        out.loc[out['TRG' + str(n)].notnull(), 'TRG_BID'] = n

    #adds new column: TRG evaluate set to Best by default (may loop through other TRGs later)
    out['TRG_Eval'] = out['TRG_Best']

    #creates final column list for export
    trg_eval = ['TRG_Avg','TRG_Best','TRG_BID','TRG_Eval']
    col_list = ['Region','R_Group','R_Subgroup','State','Season','Month','DOY','Hour','HOY','Load_Act']
    col_list = col_list + trg_list + trg_eval
    out = out[col_list]
    
    #Remove regions without load data (only removes ERC_PHDL)
    out = out.dropna(subset=['Load_Act'])
    #print('number of regs in dataset (with load) =',out.shape[0]/8760)
    return out

# In[4]:

print('SOLAR PROCESSING')
print()
solar_raw = pd.read_csv('inputs/raw/solar_generation.csv')
solar_dur = cleandata(solar_raw)
solar_dur2 = vreclean(solar_dur)
solar_dur2.to_csv('../outputs/solar_long_format.csv')
print('number of rows in final solar dataset =',solar_dur2.shape[0])
print('number of regs in final solar dataset =',solar_dur2.shape[0]/8760)
unique_r = pd.Series(solar_dur2['Region'].unique()).dropna()
print('number of hours for each regs in final solar dataset =',solar_dur2.shape[0]/unique_r.shape[0])
print()

# In[5]:

print('WIND PROCESSING')
print()
wind_raw = pd.read_csv('inputs/raw/onshore_wind_gen.csv')
wind_dur = cleandata(wind_raw)
wind_dur2 = vreclean(wind_dur)
wind_dur2.to_csv('../outputs/wind_long_format.csv')
print('number of rows in final wind dataset =',wind_dur2.shape[0])
print('number of regs in final wind dataset =',wind_dur2.shape[0]/8760)
unique_r = pd.Series(wind_dur2['Region'].unique()).dropna()
print('number of hours for each regs in final wind dataset =',wind_dur2.shape[0]/unique_r.shape[0])
print()

# In[6]:

print('combined all three datasets')
print()
#Wind: average the region data across sub_regions
wset =  wind_dur2[['R_Subgroup','HOY','TRG_Eval']]
wset2 = wset.groupby(['R_Subgroup','HOY'],as_index=False).agg({'TRG_Eval':['mean']})
wset2.columns = wset2.columns.droplevel(0)
wset2.columns=['R_Subgroup','HOY','Wind']

#print(wset2[wset2.isna().any(axis=1)])
unique_w = pd.Series(wset2['R_Subgroup'].unique()).dropna()
print(len(unique_w),'regions with wind resource')

#Solar: average the region data across sub_regions
sset = solar_dur2[['R_Subgroup','HOY','TRG_Eval']]
sset2 = sset.groupby(['R_Subgroup','HOY'],as_index=False).agg({'TRG_Eval':['mean']})
sset2.columns = sset2.columns.droplevel(0)
sset2.columns=['R_Subgroup','HOY','Solar']

#print(sset2[sset2.isna().any(axis=1)])
unique_s = pd.Series(sset2['R_Subgroup'].unique()).dropna()
print(len(unique_s),'regions with solar resource')

#combined wind and solar 
wsset = pd.merge(wset2,sset2,on=['R_Subgroup','HOY'],how='outer')

#Load: sum the region data across sub_regions
lset = load_dur[['R_Subgroup','HOY','Load_Act']]
lset2 = lset.groupby(['R_Subgroup','HOY'],as_index=False).agg({'Load_Act':['sum']})
lset2.columns = lset2.columns.droplevel(0)
lset2.columns=['R_Subgroup','HOY','Load_Act']

#Load: calculate peak load and load as a percentage of peak
peak = load_dur.groupby(['R_Subgroup'],as_index=False).agg({'Load_Act':max})
peak = peak.rename(columns={'Load_Act':'Load_Peak'})
lset3 = pd.merge(lset2,peak, on='R_Subgroup', how='left')
lset3['Load'] = lset3['Load_Act'] / lset3['Load_Peak']
lset3 = lset3.drop(columns = ['Load_Peak'])
hoy_match = pd.read_csv('inputs/HOY_match.csv') 
lset3 = pd.merge(lset3,hoy_match, on='HOY', how='left')

#print(lset3[lset3.isna().any(axis=1)])
unique_l = pd.Series(lset3['R_Subgroup'].unique()).dropna()
print(len(unique_l),'regions with load')
print()

#combined wind and solar and load
lwsset = pd.merge(lset3,wsset,on=['R_Subgroup','HOY'],how='left')
lwsset = lwsset[['R_Subgroup','Season','Month','DOY','Hour','HOY','Load_Act','Load','Wind','Solar']]

#fill in missing values with zeros
lwsnan = lwsset[lwsset.isna().any(axis=1)]
unique_lws = pd.Series(lwsnan['R_Subgroup'].unique()).dropna()
lwsset = lwsset.fillna(0)
print(len(unique_lws),'regions with missing combo resource:')
print('to be filled in as zeros')
print(unique_lws)
print()

lwsset.to_csv('../outputs/8760_combo.csv')
print('number of rows in final combo dataset =',lwsset.shape[0])
print('number of regs in final combo dataset =',lwsset.shape[0]/8760)
unique_r = pd.Series(lwsset['R_Subgroup'].unique()).dropna()
print('number of hours for each regs in final combo dataset =',lwsset.shape[0]/unique_r.shape[0])
print()
print('completed initial data read')
print()
