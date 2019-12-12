import pandas as pd
from datetime import datetime, timedelta

dframe = pd.read_excel(r'C:\Users\ARL\Desktop\Summit_GC_2019\CH4_results\Methane_Automated_2019_BUP.xlsx')
dframe.dropna(subset=['std_median'], inplace=True)
dframe = dframe.loc[:, ['filename', 'std_median']]

year = 2019

dframe['Year'] = dframe['filename'].apply(lambda x: int(str(x)[0:4]))
dframe['Yearly_Day'] = dframe['filename'].apply(lambda x: int(str(x)[4:7]))
dframe['Hour'] = dframe['filename'].apply(lambda x: int(str(x)[7:9]))
dframe['Minute'] = dframe['filename'].apply(lambda x: int(str(x)[9:11]))
dframe['Second'] = dframe['filename'].apply(lambda x: int(str(x)[11:13]))

base_date = datetime(year=year, month=1, day=1)
dframe['date'] = [base_date + timedelta(days=int(row[0] - 1), hours=int(row[1]), minutes=int(row[2]),
                                        seconds=int(row[3])
                                        ) for row in
                  dframe[[
                      'Yearly_Day',
                      'Hour',
                      'Minute',
                      'Second']].values]

dframe = dframe.loc[:, ['std_median', 'date']]
dframe.set_index('date', inplace=True)
print(dframe)