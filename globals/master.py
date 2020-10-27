# -*- coding: utf-8 -*-
"""
Created on Mon Oct 19 15:22:56 2020

@author: CMARCY

This file just calls all of the individual modules for each dataset
"""
import pandas as pd

import P0_Initial_Data_Read

pd.read_csv('globals/load1.csv', index_col=None).to_csv('global1.py', index = False)
pd.read_csv('globals/load2.csv', index_col=None).to_csv('global2.py', index = False)

import P1A_Sort_IPM_Seq
import P1B_Sort_Day_Type
import P1C_Sort_Kmeans
import P2A_Error_Analysis
import P2B_Plots
import P3_Critical_Hours

pd.read_csv('globals/wind1.csv', index_col=None).to_csv('global1.py', index = False)
pd.read_csv('globals/wind2.csv', index_col=None).to_csv('global2.py', index = False)

import P1A_Sort_IPM_Seq
import P1B_Sort_Day_Type
import P1C_Sort_Kmeans
import P2A_Error_Analysis
import P2B_Plots
import P3_Critical_Hours

pd.read_csv('globals/solar1.csv', index_col=None).to_csv('global1.py', index = False)
pd.read_csv('globals/solar2.csv', index_col=None).to_csv('global2.py', index = False)

import P1A_Sort_IPM_Seq
import P1B_Sort_Day_Type
import P1C_Sort_Kmeans
import P2A_Error_Analysis
import P2B_Plots
import P3_Critical_Hours