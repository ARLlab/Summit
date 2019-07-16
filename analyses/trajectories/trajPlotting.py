from trajFuncs import readTraj
import matplotlib.pyplot as plt
import matplotlib.image as mpimg

# import data
traj = readTraj()

mybounds = {'x': (-73.2, -9.4),
            'y': (57.8, 84.3)}

# scatterplot mapping
img = mpimg.imread(r'C:\Users\ARL\Desktop\FireData\greenland.png')
traj.plot(kind='line', x='long', y='lat',
          c='alt', cmap=plt.get_cmap('magma'), figsize=(10, 7))

plt.imshow(img, extent=[mybounds['x'][0], mybounds['x'][1],
                        mybounds['y'][0], mybounds['y'][1]], alpha=0.5)

plt.xlabel('Longitude', fontsize=14)
plt.ylabel('Latitude', fontsize=14)
plt.xlim(mybounds['x'][0], mybounds['x'][1])
plt.ylim(mybounds['y'][0], mybounds['y'][1])
plt.title('Back Trajectories')
plt.legend()

plt.show()