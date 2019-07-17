import numpy as np
from dateConv import createDatetime
import cartopy.crs as ccrs
import matplotlib.pyplot as plt
import shapely.geometry as sgeom
import cartopy.io.shapereader as shpreader

import pandas as pd
import os
import matplotlib.patches as mpatches

# TODO: Color trajectories by z score


def firedt(dataframe):
    """
    :param dataframe: a dataframe of NASA VIIRS data, with their column titles
    :return: the same dataframe with a datetime column
    """

    import datetime as dt
    import pandas as pd
    import numpy as np
    from dateConv import createDatetime

    # preallocate and define used columns of dataframe
    values = dataframe['acq_date']
    timedeltas = dataframe['acq_time']

    # seperate datetime components
    sep = values.str.split('-')
    dataframe.insert(len(dataframe.columns), 'yr', sep.str[0].astype(int), True)
    dataframe.insert(len(dataframe.columns), 'mo', sep.str[1].astype(int), True)
    dataframe.insert(len(dataframe.columns), 'dy', sep.str[2].astype(int), True)
    dataframe.insert(len(dataframe.columns), 'hr', np.zeros(len(dataframe['dy'])).astype(int), True)

    # create datetimes
    dataframe.insert(len(dataframe.columns), 'dt',
                     createDatetime(dataframe['yr'].values,
                                    dataframe['mo'].values,
                                    dataframe['dy'].values,
                                    dataframe['hr'].values))

    # add timedelta of hour and minute from the acq_time column
    timedeltas = pd.to_timedelta(timedeltas, unit='m')
    dataframe.insert(len(dataframe.columns), 'datetime',
                     dataframe['dt'] + timedeltas)

    # remove other columns
    badcols = ['acq_time', 'acq_date', 'yr', 'mo', 'dy', 'hr', 'dt']
    df = dataframe.drop(badcols, axis=1)

    return df


def fireData():

    # import fire data
    virrs = True
    root = r'C:\Users\ARL\Desktop\FireData'
    if virrs:
        fire = pd.read_csv(root + r'\fire_archive_V1_60132.csv')
    else:
        fire = pd.read_csv(root + r'\fire_archive_M6_60131.csv')

    fire = firedt(fire)

    return fire


def main():

    fig = plt.figure(figsize=[8, 8])
    ax = fig.add_subplot(projection=ccrs.Orthographic(-30, 70))
    fig.subplots_adjust(top=0.970,
                        bottom=0.012,
                        left=0.019,
                        right=0.981,
                        hspace=0.27,
                        wspace=0.02)
    ax.coastlines(zorder=3)
    ax.stock_img()
    ax.gridlines()
    ax.set_title('Back Trajectories of 72h from Summit, Greenland w/ '
                 'intersection with VIIRS fire counts')

    root = r'C:\Users\ARL\Desktop\Jashan PySplit\pysplitprocessor-master\pysplitprocessor\trajectories'

    # plot summit
    ax.plot(-38.4592, 72.5796, marker='*', color='blue', markersize=10, alpha=0.9,
            transform=ccrs.Geodetic(), label='Summit')

    # plot trajectories
    frame = []
    for filename in os.listdir(root):
        colnames = ['recep', 'v2', 'yr', 'mo', 'dy', 'hr', 'na', 'na1', 'backhrs', 'lat', 'long', 'alt', 'pres']
        data = pd.read_csv(root + '\\' + filename,
                           header=None,
                           delim_whitespace=True,
                           error_bad_lines=False,
                           names=colnames)

        data.dropna(axis=0, how='any', inplace=True)
        badcols = ['recep', 'v2', 'na', 'na1']
        data.drop(badcols, axis=1, inplace=True)
        data.reset_index(drop=True, inplace=True)
        frame.append(data)

        track = sgeom.LineString(zip(data['long'].values, data['lat'].values))

        ax.add_geometries([track], ccrs.PlateCarree(),
                          facecolor='none', edgecolor='gray',
                          linewidth=0.5, label='Trajectories')

    traj = pd.concat(frame, axis=0, ignore_index=True)
    traj['datetime'] = createDatetime(traj['yr'].values.astype(int),
                                      traj['mo'].values.astype(int),
                                      traj['dy'].values.astype(int),
                                      traj['hr'].values.astype(int))
    traj.drop(['yr', 'mo', 'dy', 'hr'], axis=1, inplace=True)

    # plot fires
    fire = fireData()
    matches = []

    for row in fire.iterrows():

        # identify if fire is in a back trajectory path at same times
        lats = np.in1d(np.around(traj['lat'].values, 0),
                       np.around(row[1]['latitude'], 0))

        longs = np.in1d(np.around(traj['long'].values, 0),
                        np.around(row[1]['longitude'], 0))

        # TODO: seems to not get many, just double check method and that it gets the date
        combos = traj[lats & longs].values.tolist()
        matches.append(combos)

        if combos:
            ax.plot(row[1]['longitude'], row[1]['latitude'],
                    marker='.', color='purple', markersize=8,
                    alpha=0.7, transform=ccrs.Geodetic())
        else:
            ax.plot(row[1]['longitude'], row[1]['latitude'],
                    marker='.', color='red', markersize=4,
                    alpha=0.7, transform=ccrs.Geodetic())

    legend_elements = [mpatches.Rectangle((0, 0), 1, 1, facecolor='blue'),
                       mpatches.Rectangle((0, 0), 1, 1, facecolor='red'),
                       mpatches.Rectangle((0, 0), 1, 1, facecolor='purple'),
                       mpatches.Rectangle((0, 0), 1, 1, facecolor='gray')]
    labels = ['Summit', 'VIIRS Fires', 'Intersected Fire', 'Trajectories']

    ax.legend(handles=legend_elements, loc='lower left', fancybox=True, labels=labels)

    matches = pd.DataFrame(matches)
    matches.dropna(axis=0, how='any', inplace=True)
    print(f'{len(matches.columns)} intersections were found between a back trajectory and a fire')
    plt.show()


if __name__ == '__main__':
    main()
