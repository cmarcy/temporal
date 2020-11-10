#Plots -- plots created are exported to file, not shown here
print('start plots')
print()
#importing packages needed for analysis
import os
import pandas as pd
import matplotlib.pyplot as plt 

path = os.getcwd()
parent = os.path.dirname(path)
outputs_dir = parent+'\outputs\plots'
if not os.path.exists(outputs_dir):
    os.makedirs(outputs_dir)
print('output files are written out in parent directory: '+outputs_dir)


# In[1]:

def plot(x):
    #Set up the data for plotting
    
    RMSE_prof = pd.read_csv('../outputs/error_analysis/'+x+'_profile_RMSE.csv')
    RMSE_prof = RMSE_prof.rename(columns={'Unnamed: 0':'Profile'})
    number_seg = pd.read_csv('inputs/number_segments.csv')
    RMSE_prof = pd.merge(RMSE_prof, number_seg, on='Profile', how='left')
    RMSE_prof.to_csv('../outputs/error_analysis/'+x+'_profile_RMSE_segs.csv')
    
    RMSE_prof = RMSE_prof.drop(2).reset_index(drop=True)
    group = RMSE_prof['Profile'].str.split("_", n = 1, expand = True) 
    RMSE_prof['Group'] = group[0]
    RMSE_prof['Group'] = pd.Categorical(RMSE_prof['Group'])
    RMSE_prof['Color'] = RMSE_prof['Group'].cat.codes
    #unique_g = pd.Series(RMSE_prof['Group'].unique()).dropna()
    
    #Plot of Number of Segments vs Error
    
    #plt.legend(loc='upper right')
    plt.grid()
    plt.title('Error and time segment comparison')
    plt.xlabel('Time Segments')
    plt.ylabel('Error')
    
    plt.scatter(x=RMSE_prof['Segments'], y=RMSE_prof['RMSE'], c=RMSE_prof['Color'])
    plt.savefig('../outputs/plots/segments_'+x+'.png', bbox_inches='tight')

plot('load')
plot('wind')
plot('solar')

print('end of plots')
