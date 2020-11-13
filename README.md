# Temporal
project that reviews the RMSE of different temporal resolution profiles

To run this project, simply run master.py

*Note:* this is currently setup as the test version. This means that it will solve 1-2 profiles per approach. This takes approximately 1/2 hour to complete. 
To run the full suite of profiles per approach, follow the instructions in the section identified under "TESTING" in each of the P1 .py files.
This typically requires you to comment out (#) a single line of code (and/or uncomment a line or two of additional code). The full suite solves overnight. 

Python files included:

#### master.py - file that just imports the code from the other python files. 

This file shows the order in which the rest of the code in this directory should be executed.
Below is a short description of each .py file and a list of the input/output files used/created. 

#### P0_Initial_Data_Read - preprocessing of the raw input files from IPM. 

*Input files:*
 - P0_Initial_Data_Read.py:season_month = pd.read_csv('inputs/season_months.csv')
 - P0_Initial_Data_Read.py:days = pd.read_csv('inputs/days_365.csv').drop(columns=['Week'])
 - P0_Initial_Data_Read.py:load_raw = pd.read_csv('inputs/raw/table_2-2_load.csv')
 - P0_Initial_Data_Read.py:solar_raw = pd.read_csv('inputs/raw/solar_generation.csv')
 - P0_Initial_Data_Read.py:wind_raw = pd.read_csv('inputs/raw/onshore_wind_gen.csv')

*Output files:*
 - P0_Initial_Data_Read.py:load_dur.to_csv('../outputs/load_long_format.csv')
 - P0_Initial_Data_Read.py:solar_dur2.to_csv('../outputs/solar_long_format.csv')
 - P0_Initial_Data_Read.py:wind_dur2.to_csv('../outputs/wind_long_format.csv')
 - P0_Initial_Data_Read.py:lwsset.to_csv('../outputs/8760_combo.csv')

#### P1A_Sort_Seq_DayType.py - creates the profiles for the sequential approach and the day-type approach

*Input files:*
 - P1B_Sort_Day_Type.py:wind_dur = pd.read_csv('../outputs/load_long_format.csv')
 - P1B_Sort_Day_Type.py:wind_dur = pd.read_csv('../outputs/solar_long_format.csv')
 - P1B_Sort_Day_Type.py:wind_dur = pd.read_csv('../outputs/wind_long_format.csv')
 - P1A_Sort_IPM_Seq.py:seq_intervals = pd.read_csv('inputs/sequential_hours.csv')
 - P1B_Sort_Day_Type.py:daydata = pd.read_csv('inputs/days_365.csv')
 - P1B_Sort_Day_Type.py:monthly = pd.read_csv('inputs/season_bimonthly.csv')
 - P1B_Sort_Day_Type.py:weekday = pd.read_csv('inputs/weekday.csv')
 - P1B_Sort_Day_Type.py:interval_4hr = pd.read_csv('inputs/interval_4hr.csv')

*Output files:*
 - P1A_Sort_IPM_Seq.py:seq_x2.to_csv('../outputs/'+x_name+'/'+x_name+'_8760_Sequent_'+i+'.csv')
 - P1B_Sort_Day_Type.py:case_x.to_csv('../outputs/'+x_name+'/'+x_name+'_8760_'+agg_name+'.csv')

#### P1B_Sort_IPM.py - creates the profiles for the IPM approach 

*Input files:*
 - P1A_Sort_IPM_Seq.py:load_dur = pd.read_csv('../outputs/load_long_format.csv')
 - P1A_Sort_IPM_Seq.py:solar_dur = pd.read_csv('../outputs/solar_long_format.csv')
 - P1A_Sort_IPM_Seq.py:wind_dur = pd.read_csv('../outputs/wind_long_format.csv')
 - P1A_Sort_IPM_Seq.py:tod = pd.read_csv('inputs/time_of_day.csv')
 - P1A_Sort_IPM_Seq.py:group = pd.read_csv('inputs/group_shares.csv')
 - P1A_Sort_IPM_Seq.py:tod = pd.read_csv('inputs/time_of_day.csv')
 - P1A_Sort_IPM_Seq.py:group = pd.read_csv('inputs/group_shares.csv')

*Output files:*
 - P1A_Sort_IPM_Seq.py:x4.to_csv('../outputs/'+x_name+'/'+x_name+'_8760_IPM.csv')
 - P1A_Sort_IPM_Seq.py:tod_x_3.to_csv('../outputs/'+x_name+'/'+x_name+'_8760_IPMalt.csv')

#### P1C_Sort_BestFit.py - creates the profiles for the best-fit approach

*Input files:*
 - P1C_Sort_BestFit.py:load_dur = pd.read_csv('../outputs/load_long_format.csv')
 - P1C_Sort_BestFit.py:solar_dur = pd.read_csv('../outputs/solar_long_format.csv')
 - P1C_Sort_BestFit.py:wind_dur = pd.read_csv('../outputs/wind_long_format.csv')
 - P1C_Sort_BestFit.py:lwsset = pd.read_csv('../outputs/8760_combo.csv')

*Output files:*
 - P1C_Sort_BestFit.py:khsl2.to_csv('../outputs/load/load_8760_B'+x_name+'_'+num+'.csv')
 - P1C_Sort_BestFit.py:khss.to_csv('../outputs/solar/solar_8760_B'+x_name+'_'+num+'.csv')
 - P1C_Sort_BestFit.py:khsw.to_csv('../outputs/wind/wind_8760_B'+x_name+'_'+num+'.csv')

#### P1D_Sort_Cluster.py - creates the profiles for the clustering approach

*Input files:*
 - P1D_Sort_Cluster.py:load_dur = pd.read_csv('../outputs/load_long_format.csv')
 - P1D_Sort_Cluster.py:solar_dur = pd.read_csv('../outputs/solar_long_format.csv')
 - P1D_Sort_Cluster.py:wind_dur = pd.read_csv('../outputs/wind_long_format.csv')
 - P1D_Sort_Cluster.py:lwsset = pd.read_csv('../outputs/8760_combo.csv')

*Output files:*
 - P1D_Sort_Cluster.py:khasl2.to_csv('../outputs/load/load_8760_Cluster_'+num+'.csv' - makes a quick figure )
 - P1D_Sort_Cluster.py:khass.to_csv('../outputs/solar/solar_8760_Cluster_'+num+'.csv')
 - P1D_Sort_Cluster.py:khasw.to_csv('../outputs/wind/wind_8760_Cluster_'+num+'.csv')

#### P2A_Error_Analysis.py - calcuates the RMSE for each profile at a national level and a regional level

*Input files:*
 - P2A_Error_Analysis.py:long = pd.read_csv('../outputs/'+x+'_long_format.csv')
 - P2A_Error_Analysis.py:stat = pd.read_csv('../outputs/'+x+'/'+i)

*Output files:*
 - P2A_Error_Analysis.py:reg_RMSE.to_csv('../outputs/error_analysis/'+x+'_'+'regional_RMSE.csv')
 - P2A_Error_Analysis.py:profile_df.to_csv('../outputs/error_analysis/'+x+'_'+'profile_RMSE.csv')
 - P2A_Error_Analysis.py:seg_count.to_csv('../outputs/error_analysis/number_segments.csv')

#### P2B_Plots.py - makes a quick figure for QA purposes

*Input files:*
 - P2B_Plots.py:RMSE_prof = pd.read_csv('../outputs/error_analysis/'+x+'_profile_RMSE.csv')
 - P2B_Plots.py:number_seg = pd.read_csv('../outputs/error_analysis/number_segments.csv')

*Output files:*
 - P2B_Plots.py:RMSE_prof.to_csv('../outputs/error_analysis/'+x+'_profile_RMSE_segs.csv')

#### P3_Critical_Hours.py - calcuates the RMSE for each profile and each critical hour set at a national level

*Input files:*
 - P3_Critical_Hours.py:combo = pd.read_csv('../outputs/8760_combo.csv').drop(columns={'Unnamed: 0'})
 - P3_Critical_Hours.py:long = pd.read_csv('../outputs/'+x+'_long_format.csv')
 - P3_Critical_Hours.py:diff = pd.read_csv('../outputs/'+x+'/'+filelist[0])
 - P3_Critical_Hours.py:df = pd.read_csv('../outputs/'+x+'/'+i)
 - P3_Critical_Hours.py:number_seg = pd.read_csv('../outputs/error_analysis/number_segments.csv')

*Output files:*
 - P3_Critical_Hours.py:crit_hr.to_csv('../outputs/critical_hours.csv')
 - P3_Critical_Hours.py:profile_df2.to_csv('../outputs/error_analysis/'+x+'_profile_RMSE_sets.csv')

