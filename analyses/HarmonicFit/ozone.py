"""
Created on May 30th, 2019. This script compares surface ozone data with hourly resolution from Summit (SUM) station
in Greenland with the ethane/methane and acetylene/methane ratios developed and analyzed in 'ratioComparison.py.

The ozone data used here is courtesy of NOAA GMD. See the citation below.

McClure-Begley, A., Petropavlovskikh, I., Oltmans, S., (2014) NOAA Global Monitoring Surface Ozone Network.
 Summit, 2012-2018. National Oceanic and Atmospheric Administration, Earth Systems Research Laboratory
 Global Monitoring Division. Boulder, CO. 5/30/2019. http://dx.doi.org/10.7289/V57P8WBF

"""
# Importing Libraries
import pandas as pd

# Read the Data
root = r'C:\Users\ARL\Desktop\OzoneData'
years = [2012, 2013, 2014, 2015, 2016, 2017, 2018]
fileExt = [12, 13, 14, 15, 16, 17, 18]
numYears = len(years)
ozone = []

for yr, ext in zip(years, fileExt):
    folder = root + r'\%s\sutccl%s' % yr, ext
