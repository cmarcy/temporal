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

##UNCOMMENT WHICH PROFILE BEING ANALYZED 
x = 'load'
x2 = 'Load'

#x = 'solar'
#x2 = 'TRG6'

#x = 'wind'
#x2 = 'TRG4'

# In[1]:

#Plot of Number of Segments vs Error

RMSE_prof = pd.read_csv('../outputs/error_analysis/'+x+'_profile_RMSE.csv')
RMSE_prof = RMSE_prof.rename(columns={'Unnamed: 0':'Profile'})
number_seg = pd.read_csv('inputs/number_segments.csv')
RMSE_prof = pd.merge(RMSE_prof, number_seg, on='Profile', how='left')
RMSE_prof.to_csv('../outputs/error_analysis/'+x+'_profile_RMSE_segs.csv')

RMSE_prof = RMSE_prof.drop(2).reset_index(drop=True)

plt.clf()
fig, axis = plt.subplots()

axis.yaxis.grid(True)
axis.set_title('Error and time segment comparison')
axis.set_xlabel('Time Segments')
axis.set_ylabel('Error')

X = RMSE_prof['Segments']
Y = RMSE_prof['RMSE']

axis.scatter(X, Y)

fig.savefig('../outputs/plots/segments_'+x+'.png', bbox_inches='tight')

print('end of plots')