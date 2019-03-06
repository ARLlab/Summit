import os
import pandas as pd
import numpy as np

os.chdir(r'C:\Users\arl\Desktop\Summit Processors\Summit VOC Processor\integration_exploring')

data = pd.read_excel('Ambient_2018_V2.xlsx', header=None, usecols="A,E:AMJ")

rts = data.iloc[97:113].T  # take only retention time rows and transpose
rts.columns = rts.iloc[0].str.lower()  # set columns to variable names, set all to lowercase
rts = rts.reindex(rts.index.drop(0)).reset_index(drop=True)
# drop column names, reset int index and don't keep the old

rts[rts == 0] = np.nan

rts = rts.loc[850:,:]  # optional slicing of data (get all data after change to current oven program and butane order)

# for col in rts.columns.tolist():
# 	print(f"Compound {col} had a mean of {rts[col].mean():.03f}, median of {rts[col].median():.03f}, and stdev of {rts[col].std():.03f}.")
#
# for col in rts.columns.tolist():
# 	print(f"{col}: {rts[col].mean() - rts[col].std():.03f} : {rts[col].mean() + rts[col].std():.03f}")

rts['ibut_acet_diff'] = rts['acetylene'] - rts['i-butane']
rts['ibut_nbut_diff'] = rts['n-butane'] - rts['i-butane']

import matplotlib.pyplot as plt

f1 = plt.figure()
ax = f1.gca()

ax.plot(rts.index, rts['ibut_acet_diff'], '-o')
ax.plot(rts.index, rts['ibut_nbut_diff'], '-o')
ax.legend(['acet - ibut', 'nbut - ibut'])

# Shows that differences are stable within GC Oven program regimes. Currently, data is reliable with:
	# Acet: .6 - .7 difference
	# Nbut: .55 - .6 difference (but picks up co-eluted acet/nbut peak combos
