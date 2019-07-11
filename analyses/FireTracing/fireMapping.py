import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import matplotlib.cm as cm
import matplotlib.image as mpimg
from fireFuncs import fireCombo

# import fire data
root = r'C:\Users\ARL\Desktop\FireData'
fire = pd.read_csv(root + r'\fire_archive_V1_58066.csv')

# import other dataset to compare with

# call fire combo to combine the datasets
fire = fireCombo(fire, fire)

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

