import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import matplotlib.cm as cm
import matplotlib.image as mpimg

root = r'C:\Users\ARL\Desktop\FireData'
fire = pd.read_csv(root + r'\fire_archive_V1_58066.csv')

# only keep high tolerence values
# cond = fire['confidence'] == 'h'
# fire = fire[cond]

# lets get the years
# TODO: This dataset will require a custom function to get a datetime because of strange time formatting
dates = fire['acq_date'].values
yrs = []
for i in range(len(fire)):
    yrs.append(dates[i][:4])
fire['yrs'] = yrs
fire['yrs'] = fire['yrs'].values.astype(int)

sns.set(style="dark", palette="muted", color_codes=True)
sns.despine()

# sns.distplot(fire['yrs'].values.astype(int), norm_hist=False, kde=False)
# plt.title('Distribution of NASA VIIRS Fire Count Data by Magnitude')
# plt.xlabel('Year')
# plt.ylabel('Raw Count')
# plt.show()

# sns.distplot(fire['bright_ti4'].values.astype(int), norm_hist=False, kde=False, bins=20)
# plt.title('Distribution of NASA VIIRS Fire Count Data by Magnitude')
# plt.xlabel('I-4 Intensity in Kelvin')
# plt.ylabel('Raw Count')
# plt.xlim([280, 380])

mybounds = {'x': (-73.2, -9.4),
            'y': (57.8, 84.3)}

# scatterplot mapping
img = mpimg.imread(root + r'\greenland.PNG')

fire.plot(kind='scatter', x='longitude', y='latitude',
          c='bright_ti4', cmap=plt.get_cmap('magma_r'),
          colorbar=True, figsize=(10, 7))

plt.imshow(img, extent=[mybounds['x'][0], mybounds['x'][1],
                        mybounds['y'][0], mybounds['y'][1]], alpha=0.5)
plt.xlabel('Longitude', fontsize=14)
plt.ylabel('Latitude', fontsize=14)
plt.title('NASA VIIRS Fire Count Overlay on Greenland')
plt.legend()


plt.show()

