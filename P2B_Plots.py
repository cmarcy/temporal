#Plots -- plots created are exported to file, not sh own here
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
#print('output files are written out in parent directory: '+outputs_dir)

# In[1]:

def plot(x):
    #Set up the data for plotting
    print(x)
    #importing datasets
    RMSE_prof = pd.read_csv('../outputs/error_analysis/'+x+'_profile_RMSE.csv')
    RMSE_prof = RMSE_prof.rename(columns={'Unnamed: 0':'Profile'})
    number_seg = pd.read_csv('../outputs/error_analysis/number_segments.csv')
    number_seg = number_seg.drop(columns=['Unnamed: 0'])
    RMSE_prof = pd.merge(RMSE_prof, number_seg, on='Profile', how='left')
    
    #creating categories for plotting the data
    RMSE_prof = RMSE_prof.drop(2).reset_index(drop=True)
    #group = RMSE_prof['Profile'].str.split("_", n = 1, expand = True) 
    #RMSE_prof['Group'] = group[0]
    
    #creating min and max points for certain categories of data
    Gframe = pd.DataFrame(['Sequent','BLoad','BSolar','BWind','Cluster']).rename(columns={0:'Group'})

    minerror = Gframe.copy()
    minerror['Profile'] = minerror['Group']+'_min'
    minerror['Segments'] = 8760
    minerror['RMSE'] = 0

    maxerror = Gframe.copy()
    maxerror['Profile'] = minerror['Group']+'_max'
    maxerror['Segments'] = 1
    maxerror['RMSE'] = RMSE_prof['RMSE'].max()
    RMSE_prof = pd.concat([RMSE_prof,minerror, maxerror], sort=False)
    
    #assigns a number for the categories, needed for plotting in different colors
    RMSE_prof['Group'] = pd.Categorical(RMSE_prof['Group'])
    RMSE_prof['Color'] = RMSE_prof['Group'].cat.codes

    #normalizing the data
    RMSE_prof['RMSE_max'] = RMSE_prof['RMSE'].max()
    RMSE_prof['RMSE_scale'] = RMSE_prof['RMSE'] / RMSE_prof['RMSE_max']
    
    #print(RMSE_prof)
        
    #Plot of Number of Segments vs Error for each category
    unique_g = pd.Series(RMSE_prof['Group'].unique()).dropna()
    fig, ax = plt.subplots()
    for group in unique_g:
        df = RMSE_prof.copy()
        df = df[df['Group']==group]
        df.sort_values(by=['RMSE_scale'], inplace=True)
        ax.plot('Segments', 'RMSE_scale', data=df, linestyle='-', marker='o', label = group)
    
    #making plot pretty
    plt.grid(True)
    plt.legend(loc='upper right',edgecolor='None')
    plt.title(x+' error and time segment comparison')
    plt.xlabel('Time Segments')
    plt.ylabel('Error (percent of max)')

    #exporting    
    RMSE_prof.to_csv('../outputs/error_analysis/'+x+'_profile_RMSE_segs.csv')
    plt.savefig('../outputs/plots/segments_'+x+'.png', bbox_inches='tight')

plot('load')
plot('solar')
plot('wind')

print('end of plots')
print()