import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns

from fireFuncs import readSF, shapePlot

# seaborn setup
sns.set(style='whitegrid', palette='pastel', color_codes=True)
sns.mpl.rc('figure', figsize=(10, 6))

# opening vector map

root = r'C:\Users\ARL\Desktop\FireData'
fire, sf = readSF(root + r'\fire_archive_V1_58078.shp')

plt.figure()
ax = plt.axes()
ax.set_aspect('equal')

shape_ex = sf.shape(id)
longitudes = []
latitudes = []

for i in range(len(shape_ex.points)):
    longitudes.append(shape_ex.points[i][0])
    latitudes.append(shape_ex.points[i][1])

plt.plot(longitudes, latitudes)


