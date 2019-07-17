from fireFuncs import fireData
from dateConv import createDatetime

import numpy as np
import cartopy.crs as ccrs
import matplotlib.pyplot as plt
import shapely.geometry as sgeom
import pandas as pd
import os
import matplotlib.patches as mpatches
import matplotlib.cm as cm
import matplotlib as mpl


def trajPlot(root, grids=True, title=None, zscores=None):

    fig = plt.figure(figsize=[10, 8])                                               # setup fig
    ax = fig.add_subplot(projection=ccrs.NearsidePerspective(-38, 72))              # subplots
    fig.subplots_adjust(top=0.970,                                                  # adjust sizing
                        bottom=0.012,
                        left=.002,
                        right=.981,
                        hspace=0.27,
                        wspace=0.02)
    ax.coastlines(zorder=3)                                                         # add coastlines
    ax.stock_img()                                                                  # stock background
    if grids is True:
        ax.gridlines()                                                              # gridlines

    ax.set_title(title)

    # plot summit
    ax.plot(-38.4592, 72.5796, marker='*', color='orange', markersize=10, alpha=0.9,
            transform=ccrs.Geodetic(), label='Summit')

    # create colormap
    cmap = cm.winter                                                                # cmap
    colors = cmap(np.arange(256))                                                   # color linspace
    minz = min(zscores['zscores'].values)                                           # max and min z scores
    maxz = max(zscores['zscores'].values)
    zscorechart = np.linspace(minz, maxz, num=256)                                  # equal z score linspace to match

    # plot trajectories
    frame = []
    for filename in os.listdir(root):
        colnames = ['recep', 'v2', 'yr', 'mo', 'dy', 'hr', 'na', 'na1', 'backhrs', 'lat', 'long', 'alt', 'pres']
        data = pd.read_csv(root + '\\' + filename,
                           header=None,                                             # read trajectories
                           delim_whitespace=True,
                           error_bad_lines=False,
                           names=colnames)

        data.dropna(axis=0, how='any', inplace=True)                                # drop NaNs
        badcols = ['recep', 'v2', 'na', 'na1']                                      # drop poor columns
        data.drop(badcols, axis=1, inplace=True)
        data.reset_index(drop=True, inplace=True)

        data['datetime'] = createDatetime(data['yr'].values,                        # create datetimes
                                          data['mo'].values,
                                          data['dy'].values,
                                          data['hr'].values)
        data.drop(['yr', 'mo', 'dy', 'hr'], axis=1, inplace=True)                   # drop old dates

        merged = pd.merge_asof(data.sort_values('datetime'), zscores,               # merge with zscores
                               on='datetime',
                               direction='nearest',
                               tolerance=pd.Timedelta('1 hours'))

        frame.append(merged)                                                        # append to frame

        track = sgeom.LineString(zip(data['long'].values, data['lat'].values))      # create trajectory

        currentz = np.nanmax(merged['zscores'].values)                              # identify z value of trajectory
        nearmatch = zscorechart.flat[np.abs(zscorechart - currentz).argmin()]       # identify nearest match
        index = np.where(zscorechart == nearmatch)[0][0]                            # identify index in zscorechart
        z_color = tuple(colors[index].tolist())                                     # backsearch for color tuple

        ax.add_geometries([track], ccrs.PlateCarree(),                              # add to plot
                          facecolor='none', edgecolor=z_color,
                          linewidth=0.5, label='Trajectories')

    traj = pd.concat(frame, axis=0, ignore_index=True)                              # concat plots

    # plot fires
    fire = fireData()                                                               # import fire data
    matches = []                                                                    # matches preallocated

    for row in fire.iterrows():                                                     # loop thru each fire

        decplace = 0                                                                # num of decimal places to match to
        # identify if fire is in a back trajectory path at same times
        lats = np.in1d(np.around(traj['lat'].values, decplace),
                       np.around(row[1]['latitude'], decplace))

        longs = np.in1d(np.around(traj['long'].values, decplace),
                        np.around(row[1]['longitude'], decplace))

        combos = traj[lats & longs].values.tolist()                                 # lat/long at same time
        matches.append(combos)                                                      # append the combo

        projection = ccrs.Geodetic()                                                # fire projection to use
        if combos:
            ax.plot(row[1]['longitude'], row[1]['latitude'],                        # if combo, make purple
                    marker='.', color='purple', markersize=8,
                    alpha=0.7, transform=projection)
        else:
            ax.plot(row[1]['longitude'], row[1]['latitude'],                        # else, make red
                    marker='.', color='red', markersize=4,
                    alpha=0.7, transform=projection)

    # create colorbar
    cbar_ax = fig.add_axes([.88, 0.15, 0.05, 0.7])                                  # add fig axes
    norm = mpl.colors.Normalize(vmin=minz, vmax=maxz)                               # normalize units
    cb = mpl.colorbar.ColorbarBase(cbar_ax, cmap=cmap,                              # create the colorbar
                                   orientation='vertical',
                                   norm=norm)
    cb.set_label('Z-Score Magnitude')                                               # give title

    legend_elements = [mpatches.Rectangle((0, 0), 1, 1, facecolor='orange'),        # create legend elements
                       mpatches.Rectangle((0, 0), 1, 1, facecolor='red'),
                       mpatches.Rectangle((0, 0), 1, 1, facecolor='purple'),
                       mpatches.Rectangle((0, 0), 1, 1,
                                          facecolor=tuple(colors[0].tolist()))]

    labels = ['Summit', 'VIIRS Fires', 'Intersected Fire',                          # label creation
              'Trajectory']

    ax.legend(handles=legend_elements, loc='lower left',                            # create legend
              fancybox=True, labels=labels,
              bbox_to_anchor=(-0.1, 0.01))

    matches = pd.DataFrame(matches)                                                 # dataframe matches
    matches.dropna(axis=0, how='any', inplace=True)                                 # drop nans
    print(f'{len(matches.columns)} intersections were '
          f'found between a back trajectory and a fire')

    plt.show()


if __name__ == '__main__':
    trajPlot()
