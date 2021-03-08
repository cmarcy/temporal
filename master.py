#master

import timeit

start = timeit.default_timer()

#import P0_Initial_Data_Read
import P0B_Regional_Aggregation
import P1A_Sort_IPM
import P1B_Sort_Seq_DayType
import P1C_Sort_Cluster
import P2A_Error_Analysis
import P2B_Plots
#import P3_Critical_Hours

stop = timeit.default_timer()

print('Time: ', (stop - start)/60, 'mins')