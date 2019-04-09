"""
Created on Thursday, April 4th, 2019.

This is a learning script. It was developed for the purpose of categorizing
methane chroms and log files into distinct yearly and monthly folders. It
handles only this specific goal and is not adaptable.

Created in Spyder 3.3.2 in Anaconda Distribution, Python 3.7

"""
## Import Libraries
import os
import shutil
import string
import pandas as pd

years = [2016, 2017, 2018]
months = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec']

# create initial folders
for yrs in years:
    path = r'C:\Users\ARL\Desktop\pastch4\%i'%yrs
    os.mkdir(path)
    for mo in months:
        path = r'C:\Users\ARL\Desktop\pastch4\%i\%s'%(yrs,mo)
        os.mkdir(path)

# move files into respective folders
source = r'C:\Users\ARL\Desktop\test'
files = [file for file in os.scandir(source) if '.chr' in file.name]

from isleapyear import isleapyear

for f in files:                     # iterate over all the files
    for yr in years:                # iterate over years
        for mo in months:           # iterate over months
            if f[:4] == '%i'%yr:    # if first four letters indicate yr
                if isleapyear(yr):  # leap year julian dates are different than normal

                    if f[5:7] >= 1 && f[5:7] <= 31:
                        dest = r'C:\Users\ARL\Desktop\pastch4\%i'%yr
                        shutil.move(f,dest)     # move the files
                    elif: f[5:7] >= 32 && f[5:7] <= 60:
                        dest = r'C:\Users\ARL\Desktop\pastch4\%i'%yr
                        shutil.move(f,dest)     # move the files
                    elif: f[5:7] >= 61 && f[5:7] <= 91:
                        dest = r'C:\Users\ARL\Desktop\pastch4\%i'%yr
                        shutil.move(f,dest)     # move the files
                    elif: f[5:7] >= 92 && f[5:7] <= 121:
                        dest = r'C:\Users\ARL\Desktop\pastch4\%i'%yr
                        shutil.move(f,dest)     # move the files
                    elif: f[5:7] >= 122 && f[5:7] <= 152:
                        dest = r'C:\Users\ARL\Desktop\pastch4\%i'%yr
                        shutil.move(f,dest)     # move the files
                    elif: f[5:7] >= 153 && f[5:7] <= 182:
                        dest = r'C:\Users\ARL\Desktop\pastch4\%i'%yr
                        shutil.move(f,dest)     # move the files
                    elif: f[5:7] >= 183 && f[5:7] <= 213:
                        dest = r'C:\Users\ARL\Desktop\pastch4\%i'%yr
                        shutil.move(f,dest)     # move the files
                    elif: f[5:7] >= 214 && f[5:7] <= 244:
                        dest = r'C:\Users\ARL\Desktop\pastch4\%i'%yr
                        shutil.move(f,dest)     # move the files
                    elif: f[5:7] >= 245 && f[5:7] <= 274:
                        dest = r'C:\Users\ARL\Desktop\pastch4\%i'%yr
                        shutil.move(f,dest)     # move the files
                    elif: f[5:7] >= 275 && f[5:7] <= 305:
                        dest = r'C:\Users\ARL\Desktop\pastch4\%i'%yr
                        shutil.move(f,dest)     # move the files
                    elif: f[5:7] >= 306 && f[5:7] <= 335:
                        dest = r'C:\Users\ARL\Desktop\pastch4\%i'%yr
                        shutil.move(f,dest)     # move the files
                    else: f[5:7] >= 336 && f[5:7] <= 366:
                        dest = r'C:\Users\ARL\Desktop\pastch4\%i'%yr
                        shutil.move(f,dest)     # move the files
                else:
