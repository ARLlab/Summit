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

plt.figure(1) # NMHC Plots

plt.plot(nmhcDates,ethane,'.',nmhcDates,ethene,'.',nmhcDates,propane,'.',nmhcDates,propene, \
         '.',nmhcDates,iButane,'.',nmhcDates,acetylene,'.',nmhcDates,nButane,'.',nmhcDates,iPentane,'.' \
         ,nmhcDates,nPentane,'.',nmhcDates,hexane,'.',nmhcDates,benzene,'.',nmhcDates,toluene,'.')
plt.title('Summit NMHC from 2008-2018',fontdict=None,loc='center',pad=None) # Plot Title
plt.xlabel('Date [Fractional Year]',fontdict=None,labelpad=None) # Plot Xlabel
plt.ylabel('Mixing Ratio [Parts per Billion]',fontdict=None,labelpad=None) # Plot Ylabel
plt.legend(loc='upper right')
plt.show()

# Need legend
