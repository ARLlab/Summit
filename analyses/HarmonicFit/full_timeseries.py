"""
Created on Thursday May 14nd, 2019

This script explores the ratio of ethane to methane, as well as acetylene to methane. It plots the ratio
along all of the years where the data is present, and then performs harmonic fitting on the plots.

Goals:
1) Import ratio data
2) Create timeseries of ratio data
3) Perform harmonic fit on the data
4) Identify regions of large residual


"""

import numpy as np
from fileInput import fileLoad
from isleapyear import isleapyear
from ratio_calc import ratioCalc

ethaneMethane, aceMethane = ratioCalc()     # call this function to get the ratio information

