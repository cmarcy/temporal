#Sequential: Splits into different hour intervals (e.g. 1, 2, 4, 8, 12, ..., 8760)

print('start sequential approach')
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

def seq_approach(x):
#Details: Split into different hour intervals (e.g. 1, 2, 4, 8, 12, ..., 8760)
    print(x_name)
    print()
    
    unique_r = pd.Series(x['Region'].unique()).dropna()
    x = x[['Region','R_Group','R_Subgroup','Season','Month','DOY','Hour','HOY','Load_Act',x_column]]
    print('Number of regions in initial dataset',len(unique_r))
    
    #the interval DF defines groups across all 8760 HOY for each interval type
    seq_intervals = pd.read_csv('inputs/sequential_hours.csv')
    seq_x = pd.merge(x, seq_intervals, on='HOY', how='left')
    #print(seq_x.head(9))
    
    hr_list = seq_intervals.columns[1:]
    
    #TESTING: use line below for testing, comment out for complete solve
    #hr_list = seq_intervals.columns[1:2].append(seq_intervals.columns[-1:])
    print(hr_list)
    print()
    
    for i in hr_list:
        print(i)
        #average load based on order of groups
        aggregations = {x_column:['count','mean']}
        case = seq_x.groupby(['Region',i],as_index=False).agg(aggregations)
        case.columns = ['Region',i,'HT_'+i,'Avg_'+i]
        #print(case.head())
        print('number of segments for each region =',case.shape[0]/len(unique_r))
        
        seq_x = pd.merge(seq_x,case,on=['Region',i],how='left')
        seq_x = seq_x.sort_values(['Region','HOY']).reset_index(drop=True)
        print('number of hours for each region =',seq_x.shape[0]/len(unique_r))
        print()
    
        seq_x2 = seq_x[['Region','R_Group','R_Subgroup','Season','Month','DOY','Hour','HOY','Load_Act','HT_'+i,x_column,
                     i,'Avg_'+i]].rename(columns={i:'Seg_ID','HT_'+i:'Hour_Tot','Avg_'+i: 'Avg'})
        seq_x2.to_csv('../outputs/'+x_name+'/'+x_name+'_8760_Sequent_'+i+'.csv')

# In[2]:

x_name = 'load'
x_column = 'Load'
seq_approach(load_dur)

x_name = 'solar'
x_column = 'TRG_Eval'
seq_approach(solar_dur)

x_name = 'wind'
x_column = 'TRG_Eval'
seq_approach(wind_dur)

print('completed Sequential Approach')
print()

# In[1]:

#Day-Type Approach (18ish profiles)

print('start day-type approaches')
print()
#Find Peak Load Days in Each Month

#groupby region, month, and day to sum the total day
load = load_dur.copy()
aggregations1 = {'Load_Act':sum}
md_sum = load.groupby(['R_Subgroup','Month','DOY'],as_index=False).agg(aggregations1)
md_sum.columns = ['R_Subgroup','Month','DOY','Load_MD_Tot']
md_sum2 = pd.merge(load,md_sum,on=['R_Subgroup','Month','DOY'],how='left')

#groupby region and month to find maximum (peak day)
aggregations2 = {'Load_MD_Tot':max}
md_max = md_sum2.groupby(['R_Subgroup','Month'],as_index=False).agg(aggregations2)
md_max.columns = ['R_Subgroup','Month','Load_MD_Max']
peakd = pd.merge(md_sum2,md_max,on=['R_Subgroup','Month'],how='left')
peakd = peakd[['R_Subgroup','DOY','Hour','Load_MD_Tot','Load_MD_Max']]

# In[2]:

def setup(x):
    #Initial directory and dataset
    outputs_x = outputs_dir+'/'+x_name
    if not os.path.exists(outputs_x):
        os.makedirs(outputs_x)
    #print('output files are written out in parent directory: '+outputs_x)
    #print()

    x = x[['Region','R_Group','R_Subgroup','Season','Month','DOY','Hour','HOY','Load_Act',x_column]]
    unique_r = pd.Series(x['Region'].unique()).dropna()
    reg_count = unique_r.shape[0]
    print('Number of regions in initial dataset',reg_count)
    print('Number of rows per region in initial dataset =',x.shape[0]/reg_count)
    print('number of rows in initial dataset =',x.shape[0])
    print()
    
    #adds day of the month and week group data
    daydata = pd.read_csv('inputs/days_365.csv')
    daydata = daydata.drop(columns='Month')
    x = pd.merge(x,daydata,on=['DOY'],how='left')
    
    #adds the year and date data
    x['Year']=2011
    x.loc[x['R_Group'] == 'ERC', 'Year'] = 2016
    x['Date']=pd.to_datetime(x[['Year', 'Month', 'Day']], errors='coerce')
    x['DOW'] = x['Date'].dt.weekday

    #adds monthly group types (bimonthly(6) and seasonal(5))
    monthly = pd.read_csv('inputs/season_bimonthly.csv')
    x = pd.merge(x, monthly, on='Month', how='left')

    #add weekday / weekend day-types
    weekday = pd.read_csv('inputs/weekday.csv')
    x = pd.merge(x,weekday,on='DOW',how='left')
    x = x.drop(columns=['Year','Day','DOW'])

    #adds peak day-type data
    x = pd.merge(x,peakd,on=['R_Subgroup','DOY','Hour'],how='left')
    x['Day_Type'] = x['Weekday']
    x.loc[x['Weekday'] == True, 'Day_Type'] = 'Weekday'
    x.loc[x['Weekday'] == False, 'Day_Type'] = 'Weekend'
    x.loc[x['Load_MD_Tot'] == x['Load_MD_Max'], 'Day_Type'] = 'Peak'
    x = x.drop(['Load_MD_Tot','Load_MD_Max'], axis=1)
    
    #adds 4-hour interval groups
    interval_4hr = pd.read_csv('inputs/interval_4hr.csv')
    x = pd.merge(x,interval_4hr,on='Hour',how='left')

    return x

# In[3]:

def aggregate(agg_name,agg_list):
    print(agg_name,agg_list)
    case = x.copy()
    aggregations = {x_column:['count','mean']}
    grouped = case.groupby(agg_list,as_index=False).agg(aggregations)
    grouped.columns = grouped.columns.droplevel(0)
    col_list = agg_list + ['Hour_Tot','Avg']
    grouped.columns = col_list
    print('number of segments in dataset =',grouped.shape[0]/reg_count)
    
    case_x = pd.merge(case,grouped,on=agg_list,how='left')
    case_x = case_x.sort_values(agg_list)
    case_x.to_csv('../outputs/'+x_name+'/'+x_name+'_8760_'+agg_name+'.csv')
    print('number of rows in dataset =',case_x.shape[0])
    print()

# In[4]:

# TABLE HEADER:               Region    Month-Type   Day-Type   Hour-Type
mydict = {} 
#month
mydict['DayType_M12D3H24'] = ['Region', 'Month',     'Day_Type','Hour']
mydict['DayType_M12D2H24'] = ['Region', 'Month',     'Weekday', 'Hour']
mydict['DayType_M12D1H24'] = ['Region', 'Month',                'Hour']
mydict['DayType_M12D3H06'] = ['Region', 'Month',     'Day_Type','4-hr']
mydict['DayType_M12D2H06'] = ['Region', 'Month',     'Weekday', '4-hr']
mydict['DayType_M12D1H06'] = ['Region', 'Month',                '4-hr']

#bimonth
mydict['DayType_M05D3H24'] = ['Region', 'Bimonth',   'Day_Type','Hour']
mydict['DayType_M05D2H24'] = ['Region', 'Bimonth',   'Weekday', 'Hour']
mydict['DayType_M05D1H24'] = ['Region', 'Bimonth',              'Hour']
mydict['DayType_M05D3H06'] = ['Region', 'Bimonth',   'Day_Type','4-hr']
mydict['DayType_M05D2H06'] = ['Region', 'Bimonth',   'Weekday', '4-hr']
mydict['DayType_M05D1H06'] = ['Region', 'Bimonth',              '4-hr']

#season
mydict['DayType_M03D3H24'] = ['Region', 'Season',    'Day_Type','Hour']
mydict['DayType_M03D2H24'] = ['Region', 'Season',    'Weekday', 'Hour']
mydict['DayType_M03D1H24'] = ['Region', 'Season',               'Hour']
mydict['DayType_M03D3H06'] = ['Region', 'Season',    'Day_Type','4-hr']
mydict['DayType_M03D2H06'] = ['Region', 'Season',    'Weekday', '4-hr']
mydict['DayType_M03D1H06'] = ['Region', 'Season',               '4-hr']

#week
mydict['DayType_W52D1H24'] = ['Region',              'Week',    'Hour']

#Other examples that could be considered:
#mydict['DayType_M01D3H24'] = ['Region',              'Day_Type','Hour']
#mydict['DayType_M05D2H06'] = ['Region', 'Season_Grp','Weekday', '4-hr']

# In[5]:

x_name = 'load'
x_column = 'Load'
print(x_name)
print()

x = setup(load_dur)
unique_r = pd.Series(x['Region'].unique()).dropna()
reg_count = unique_r.shape[0]

for key in mydict:
    aggregate(key,mydict[key])

#TESTING: use line below for testing, 
#uncomment the two lines above and comment line below for complete solve
#aggregate('DayType_M12D3H24',mydict['DayType_M12D3H24'])

# In[6]:

x_name = 'solar'
x_column = 'TRG_Eval'
print(x_name)
print()

x = setup(solar_dur)
unique_r = pd.Series(x['Region'].unique()).dropna()
reg_count = unique_r.shape[0]

for key in mydict:
    aggregate(key,mydict[key])
#TESTING: use line below instead for testing
#aggregate('DayType_M12D3H24',mydict['DayType_M12D3H24'])

# In[7]:

x_name = 'wind'
x_column = 'TRG_Eval'
print(x_name)
print()

x = setup(wind_dur)
unique_r = pd.Series(x['Region'].unique()).dropna()
reg_count = unique_r.shape[0]

for key in mydict:
    aggregate(key,mydict[key])
#TESTING: use line below instead for testing
#aggregate('DayType_M12D3H24',mydict['DayType_M12D3H24'])

print('finished day-type approaches')
print()
