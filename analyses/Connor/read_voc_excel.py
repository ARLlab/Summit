import pandas as pd
from datetime import datetime, timedelta

def excel_voc(compound, start, end):
    start_date = datetime.strptime(start, '%Y-%m-%d %H:%M:%S')
    end_date = datetime.strptime(end, '%Y-%m-%d %H:%M:%S')
    year = start_date.year


    dframe = pd.read_excel(r'C:\Users\ARL\Desktop\Summit_GC_2019\NMHC_results\Ambient_2019.xlsx', header=None)
    dframe.set_index(0, inplace=True)
    dframe_transposed = dframe.T
    dframe = dframe_transposed.loc[:, [compound]]
    dframe = dframe.iloc[:, [j for j, c in enumerate(dframe.columns) if j not in [0, 2, 3]]]

    dframe['file'] = dframe_transposed.iloc[:, 0]
    dframe['decimal_date'] = dframe_transposed.iloc[:, 39]
    dframe.dropna(inplace=True, subset=['file'])

    dframe['Year'] = dframe['file'].apply(lambda x: int(str(x)[0:4]))
    dframe['Yearly_Day'] = dframe['file'].apply(lambda x: int(str(x)[4:7]))
    dframe['Hour'] = dframe['file'].apply(lambda x: int(str(x)[7:9]))
    dframe['Minute'] = dframe['file'].apply(lambda x: int(str(x)[9:11]))
    dframe['Second'] = dframe['file'].apply(lambda x: int(str(x)[11:13]))

    base_date = datetime(year=year, month=1, day=1)
    dframe['date'] = [base_date + timedelta(days=int(row[0] - 1), hours=int(row[1]), minutes=int(row[2]),
                                            seconds=int(row[3])
                                            ) for row in
                      dframe[[
                          'Yearly_Day',
                          'Hour',
                          'Minute',
                          'Second']].values]

    dframe.drop(columns=['file','Year', 'Hour', 'Minute','Yearly_Day','Second'], inplace=True)
    dframe = dframe[dframe['date'] < end_date]
    dframe = dframe[dframe['date'] > start_date]
    dframe.dropna(inplace=True)
    dframe.set_index('date', inplace=True)
    dframe.rename(columns={compound: f'{compound}_mr'}, inplace=True)
    dframe = dframe[dframe[f'{compound}_mr'] != 0]
    return dframe

def excel_blanks(compound, start, end):
    start_date = datetime.strptime(start, '%Y-%m-%d %H:%M:%S')
    end_date = datetime.strptime(end, '%Y-%m-%d %H:%M:%S')
    year = start_date.year

    dframe = pd.read_excel(r'C:\Users\ARL\Desktop\Summit_GC_2019\NMHC_results\Blanks_2019.xlsx', header=None)
    dframe.set_index(0, inplace=True)
    dframe_transposed = dframe.T
    dframe = dframe_transposed.loc[:, [compound]]
    dframe = dframe.iloc[:, [j for j, c in enumerate(dframe.columns) if j not in [0, 2, 3]]]

    dframe['file'] = dframe_transposed.iloc[:, 0]
    dframe['decimal_date'] = dframe_transposed.iloc[:, 39]
    dframe.dropna(inplace=True, subset=['file'])
    dframe['Year'] = dframe['file'].apply(lambda x: int(str(x)[0:4]))
    dframe['Yearly_Day'] = dframe['file'].apply(lambda x: int(str(x)[4:7]))
    dframe['Hour'] = dframe['file'].apply(lambda x: int(str(x)[7:9]))
    dframe['Minute'] = dframe['file'].apply(lambda x: int(str(x)[9:11]))
    dframe['Second'] = dframe['file'].apply(lambda x: int(str(x)[11:13]))

    base_date = datetime(year=year, month=1, day=1)
    dframe['date'] = [base_date + timedelta(days=int(row[0] - 1), hours=int(row[1]), minutes=int(row[2]),
                                            seconds=int(row[3])
                                            ) for row in
                      dframe[[
                          'Yearly_Day',
                          'Hour',
                          'Minute',
                          'Second']].values]

    dframe.drop(columns=['file', 'Year', 'Hour', 'Minute', 'Yearly_Day', 'Second'], inplace=True)
    dframe = dframe[dframe['date'] < end_date]
    dframe = dframe[dframe['date'] > start_date]
    dframe.dropna(inplace=True)
    dframe.set_index('date', inplace=True)
    dframe.rename(columns={compound: f'{compound}_mr'}, inplace=True)

    return dframe

def excel_trap_blanks(compound, start, end):
    start_date = datetime.strptime(start, '%Y-%m-%d %H:%M:%S')
    end_date = datetime.strptime(end, '%Y-%m-%d %H:%M:%S')
    year = start_date.year

    dframe = pd.read_excel(r'C:\Users\ARL\Desktop\Summit_GC_2019\NMHC_results\TrapBlanks_2019.xlsx', header=None)
    dframe.set_index(0, inplace=True)
    dframe_transposed = dframe.T
    dframe = dframe_transposed.loc[:, [compound]]
    dframe = dframe.iloc[:, [j for j, c in enumerate(dframe.columns) if j not in [0, 2, 3]]]

    dframe['file'] = dframe_transposed.iloc[:, 0]
    dframe['decimal_date'] = dframe_transposed.iloc[:, 39]
    dframe.dropna(inplace=True, subset=['file'])
    dframe['Year'] = dframe['file'].apply(lambda x: int(str(x)[0:4]))
    dframe['Yearly_Day'] = dframe['file'].apply(lambda x: int(str(x)[4:7]))
    dframe['Hour'] = dframe['file'].apply(lambda x: int(str(x)[7:9]))
    dframe['Minute'] = dframe['file'].apply(lambda x: int(str(x)[9:11]))
    dframe['Second'] = dframe['file'].apply(lambda x: int(str(x)[11:13]))

    base_date = datetime(year=year, month=1, day=1)
    dframe['date'] = [base_date + timedelta(days=int(row[0] - 1), hours=int(row[1]), minutes=int(row[2]),
                                            seconds=int(row[3])
                                            ) for row in
                      dframe[[
                          'Yearly_Day',
                          'Hour',
                          'Minute',
                          'Second']].values]

    dframe.drop(columns=['file', 'Year', 'Hour', 'Minute', 'Yearly_Day', 'Second'], inplace=True)
    dframe = dframe[dframe['date'] < end_date]
    dframe = dframe[dframe['date'] > start_date]
    dframe.dropna(inplace=True)
    dframe.set_index('date', inplace=True)
    dframe.rename(columns={compound: f'{compound}_mr'}, inplace=True)

    return dframe

def excel_rf_BH(compound, start, end):
    start_date = datetime.strptime(start, '%Y-%m-%d %H:%M:%S')
    end_date = datetime.strptime(end, '%Y-%m-%d %H:%M:%S')
    year = start_date.year

    dframe = pd.read_excel(r'C:\Users\ARL\Desktop\Summit_GC_2019\NMHC_results\BH_STD_2019.xlsx', header=None)
    dframe.set_index(0, inplace=True)
    dframe_transposed = dframe.T
    dframe = dframe_transposed.loc[:, [compound]]
    dframe = dframe.iloc[:, [j for j, c in enumerate(dframe.columns) if j not in [0, 2, 3]]]

    dframe['file'] = dframe_transposed.iloc[:, 0]
    dframe['decimal_date'] = dframe_transposed.iloc[:, 39]
    dframe.dropna(inplace=True, subset=['file'])

    dframe['Year'] = dframe['file'].apply(lambda x: int(str(x)[0:4]))
    dframe['Yearly_Day'] = dframe['file'].apply(lambda x: int(str(x)[4:7]))
    dframe['Hour'] = dframe['file'].apply(lambda x: int(str(x)[7:9]))
    dframe['Minute'] = dframe['file'].apply(lambda x: int(str(x)[9:11]))
    dframe['Second'] = dframe['file'].apply(lambda x: int(str(x)[11:13]))

    base_date = datetime(year=year, month=1, day=1)
    dframe['date'] = [base_date + timedelta(days=int(row[0] - 1), hours=int(row[1]), minutes=int(row[2]),
                                            seconds=int(row[3])
                                            ) for row in
                      dframe[[
                          'Yearly_Day',
                          'Hour',
                          'Minute',
                          'Second']].values]

    dframe.drop(columns=['file', 'Year', 'Hour', 'Minute', 'Yearly_Day', 'Second'], inplace=True)
    dframe = dframe[dframe['date'] < end_date]
    dframe = dframe[dframe['date'] > start_date]
    dframe.dropna(inplace=True)
    dframe.set_index('date', inplace=True)
    dframe.rename(columns={compound: f'{compound}_rf'}, inplace=True)
    dframe = dframe[dframe[f'{compound}_rf'] != 0]
    return dframe


def excel_rf_Brad6(compound, start, end):
    start_date = datetime.strptime(start, '%Y-%m-%d %H:%M:%S')
    end_date = datetime.strptime(end, '%Y-%m-%d %H:%M:%S')
    year = start_date.year

    dframe = pd.read_excel(r'C:\Users\ARL\Desktop\Summit_GC_2019\NMHC_results\Brad6_STD_2019.xlsx', header=None,
                           sheet_name='BA 2019 data')
    dframe.set_index(0, inplace=True)
    dframe_transposed = dframe.T
    dframe = dframe_transposed.loc[:, [compound]]
    dframe = dframe.iloc[:, [j for j, c in enumerate(dframe.columns) if j not in [0, 2, 3]]]

    dframe['file'] = dframe_transposed.iloc[:, 0]
    dframe['decimal_date'] = dframe_transposed.iloc[:, 39]
    dframe.dropna(inplace=True, subset=['file'])

    dframe['Year'] = dframe['file'].apply(lambda x: int(str(x)[0:4]))
    dframe['Yearly_Day'] = dframe['file'].apply(lambda x: int(str(x)[4:7]))
    dframe['Hour'] = dframe['file'].apply(lambda x: int(str(x)[7:9]))
    dframe['Minute'] = dframe['file'].apply(lambda x: int(str(x)[9:11]))
    dframe['Second'] = dframe['file'].apply(lambda x: int(str(x)[11:13]))

    base_date = datetime(year=year, month=1, day=1)
    dframe['date'] = [base_date + timedelta(days=int(row[0] - 1), hours=int(row[1]), minutes=int(row[2]),
                                            seconds=int(row[3])
                                            ) for row in
                      dframe[[
                          'Yearly_Day',
                          'Hour',
                          'Minute',
                          'Second']].values]

    dframe.drop(columns=['file', 'Year', 'Hour', 'Minute', 'Yearly_Day', 'Second'], inplace=True)
    dframe = dframe[dframe['date'] < end_date]
    dframe = dframe[dframe['date'] > start_date]
    dframe.dropna(inplace=True)
    dframe.set_index('date', inplace=True)
    dframe.rename(columns={compound: f'{compound}_rf'}, inplace=True)
    dframe = dframe[dframe[f'{compound}_rf'] != 0]
    return dframe

if __name__ == '__main__':
    print(excel_rf_Brad6('Benzene','2019-1-1 0:0:0', '2019-12-30 0:0:0'))





