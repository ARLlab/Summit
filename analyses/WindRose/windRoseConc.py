"""
Created on June 7th, 2019. The met data used here is courtesy of NOAA ESRL GMD. ftp://ftp.cmdl.noaa.gov/met/sum/README,
a full citation can be found in the file 'metTrim.py'. This function creates a windrose plot of cardinal direction
with various atmospheric tracer concentrations

"""

# import libraries and functions
import matplotlib.pyplot as plt
from WindRose.metTrim import metTrim, createDatetime
import pandas as pd
import matplotlib.cm as cm
import windrose
from metRemoval import metRemove


# TODO: Move metCombo to it's own file
def nmhcRead(filename):

    # ---- import data
    sheet = pd.read_csv(filename, encoding='utf8', delim_whitespace=True)

    # ---- data organization
    colnames = ['yr', 'mo', 'day', 'hr', 'min', 'val']                                      # column names
    sheet.columns = colnames                                                                # rename cols
    sheet['datetime'] = createDatetime(sheet['yr'],                                         # create datetime
                                       sheet['mo'],
                                       sheet['day'],
                                       sheet['hr'])
    sheet.drop(['yr', 'mo', 'day', 'hr', 'min'], axis=1, inplace=True)                      # drop old cols

    return sheet


def windRose():

    root = r'C:\Users\ARL\Desktop\Summit\analyses\Data'          # root source
    ethPath = root + r'\ethane.txt'
    acePath = root + r'\acetylene.txt'

    ethane = nmhcRead(ethPath)
    ace = nmhcRead(acePath)

    ethane = metRemove(ethane, 1, dropMet=False)
    ace = metRemove(ace, 1, dropMet=False)

    # ---- plotting
    # setup subplots
    fig, (ax1, ax2) = plt.subplots(1, 2, subplot_kw=dict(projection='windrose'))                            #
    fig.suptitle('NMHC Conc. Residuals at Summit by Wind Direction', fontsize=16)
    plt.subplots_adjust(left=None, bottom=None, right=None, top=None, wspace=0.2, hspace=-0.2)

    # setup ethane windrose
    ax1.bar(ethane['dir'].values, ethane['val'].values, normed=False, opening=0.9, edgecolor='black',
            nsector=24, bins=14, cmap=cm.viridis_r, blowto=False)
    ax1.set_title('Summit Ethane Conc. Residual [ppb]\n')
    ax1.set_legend(loc=8, fancybox=True, shadow=True, bbox_to_anchor=(0.70, -.45))

    # setup acetylene windrose
    ax2.bar(ace['dir'].values, ace['val'].values, normed=False, opening=0.9, edgecolor='black',
            nsector=24, bins=6, cmap=cm.viridis_r, blowto=False)
    ax2.set_title('Summit Ace Conc. Residual [ppb]\n')
    ax2.set_legend(loc=8, fancybox=True, shadow=True, bbox_to_anchor=(0.5, -.35))

    plt.show()


if __name__ == '__main__':
    windRose()
