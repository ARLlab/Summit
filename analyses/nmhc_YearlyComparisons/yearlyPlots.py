"""
Created on Thursday, March 7th, 2019.

This script serves to develop plots of each specific NHMC Compound from 2008-2018,
Each compound is plotted over the course of a single year with each line representing
a separate year

This code was written in Spyder via Anaconda Distribution [Python 3.7]

Overall Project Goals:
1) Times on the x axis should be written as day of the year instead of decimal
2) Each plot should have a proper legend
3) Remove outliers from the data sets
4) [Maybe?] Put each compound graph in one larger subplot for trend comparison
5) Modularize code to have an input / sort matrices file, and then a plotting file
"""

## Code from previous singular script
# Need to figure out how to import variables from FileInput.py function into this

## Plotting [Ethane]
plt.figure(1)
# Compound indicies coorespond with dates, would prefer not to hardcode them in though
plt.plot(dates2008,ethane[0:476:1],'.',dates2009,ethane[477:2182:1],'.',dates2010,ethane[2183:3067:1],'.'\
         ,dates2012,ethane[3068:3709:1],'.',dates2013,ethane[3710:4616:1],'.',dates2014,ethane[4617:5315:1],'.'\
         ,dates2015,ethane[5316:6215:1],'.',dates2016,ethane[6216:7248:1],'.',dates2017,ethane[7249:8111:1],'.'\
         ,dates2018,ethane[8112:8794:1],'.')

plt.title('Summit Ethane from 2008-2018',fontdict=None,loc='center',pad=None) # Plot Title
plt.xlabel('Day of Year',fontdict=None,labelpad=None) # Plot Xlabel
plt.ylabel('Mixing Ratio [Parts per Billion]',fontdict=None,labelpad=None) # Plot Ylabel
# FIX THE Y AXIS, SHOULD START AT 0 AND CLEARLY END AT 360, I like the gap though ?
# plt.legend(loc='upper right') # brainstorm ways to make this look better
plt.show()
