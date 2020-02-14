import pandas as pd
from datetime import datetime, timedelta

def excel_voc(compound, start, end):
    '''returns a dataframe with columns '{compound}_mr', 'decminal_date', and 'decimal_date_year'. index is in
    datetime format 'yyyy-mm-dd HH-MM-SS'''

    start_date = datetime.strptime(start, '%Y-%m-%d %H:%M:%S')
    end_date = datetime.strptime(end, '%Y-%m-%d %H:%M:%S')
    start_year = int(start_date.year)
    end_year = int(end_date.year)

    dframe_2017 = pd.DataFrame()
    dframe_2018 = pd.DataFrame()
    dframe_2019 = pd.DataFrame()

    if start_year == 2017:
        if end_year >= 2017:
            dframe_2017 = pd.read_excel(r'Z:\Data\Summit_GC\Summit_GC_2017\NMHC_results\Ambient_2017.xlsx',
                                        header=None)
            dframe_2017.set_index(0, inplace=True)
            dframe_transposed = dframe_2017.T
            dframe_2017 = dframe_transposed.loc[:, [compound]]
            dframe_2017 = dframe_2017.iloc[:, [j for j, c in enumerate(dframe_2017.columns) if j not in [0, 2, 3]]]

            dframe_2017['file'] = dframe_transposed.iloc[:, 0]
            dframe_2017['decimal_date'] = dframe_transposed.iloc[:, 39]
            dframe_2017.dropna(inplace=True, subset=['file'])
            dframe_2017['decmial_date_year'] = [(2017 + (float(row[0]) - 1) / 365) for row in
                                                dframe_2017[['decimal_date']].values]

            dframe_2017['Year'] = dframe_2017['file'].apply(lambda x: int(str(x)[0:4]))
            dframe_2017['Yearly_Day'] = dframe_2017['file'].apply(lambda x: int(str(x)[4:7]))
            dframe_2017['Hour'] = dframe_2017['file'].apply(lambda x: int(str(x)[7:9]))
            dframe_2017['Minute'] = dframe_2017['file'].apply(lambda x: int(str(x)[9:11]))
            dframe_2017['Second'] = dframe_2017['file'].apply(lambda x: int(str(x)[11:13]))

            base_date = datetime(year=2017, month=1, day=1)
            dframe_2017['date'] = [base_date + timedelta(days=int(row[0] - 1), hours=int(row[1]), minutes=int(row[2]),
                                                         seconds=int(row[3])
                                                         ) for row in
                                   dframe_2017[[
                                       'Yearly_Day',
                                       'Hour',
                                       'Minute',
                                       'Second']].values]

            dframe_2017.drop(columns=['file', 'Hour', 'Minute', 'Yearly_Day', 'Second', 'Year'], inplace=True)
            dframe_2017.dropna(inplace=True)
            dframe_2017.set_index('date', inplace=True)
            dframe_2017.rename(columns={compound: f'{compound}_mr'}, inplace=True)

            if end_year >= 2018:
                dframe_2018 = pd.read_excel(r'Z:\Data\Summit_GC\Summit_GC_2018\NMHC_results\Ambient_2018_V2.xlsx',
                                            header=None)
                dframe_2018.set_index(0, inplace=True)
                dframe_transposed = dframe_2018.T
                dframe_2018 = dframe_transposed.loc[:, [compound]]
                dframe_2018 = dframe_2018.iloc[:, [j for j, c in enumerate(dframe_2018.columns) if j not in [0, 2, 3]]]

                dframe_2018['file'] = dframe_transposed.iloc[:, 0]
                dframe_2018['decimal_date'] = dframe_transposed.iloc[:, 39]
                dframe_2018.dropna(inplace=True, subset=['file'])
                dframe_2018['decmial_date_year'] = [(2018 + (float(row[0]) - 1) / 365) for row in
                                                    dframe_2018[['decimal_date']].values]

                dframe_2018['Year'] = dframe_2018['file'].apply(lambda x: int(str(x)[0:4]))
                dframe_2018['Yearly_Day'] = dframe_2018['file'].apply(lambda x: int(str(x)[4:7]))
                dframe_2018['Hour'] = dframe_2018['file'].apply(lambda x: int(str(x)[7:9]))
                dframe_2018['Minute'] = dframe_2018['file'].apply(lambda x: int(str(x)[9:11]))
                dframe_2018['Second'] = dframe_2018['file'].apply(lambda x: int(str(x)[11:13]))

                base_date = datetime(year=2018, month=1, day=1)
                dframe_2018['date'] = [
                    base_date + timedelta(days=int(row[0] - 1), hours=int(row[1]), minutes=int(row[2]),
                                          seconds=int(row[3])
                                          ) for row in
                    dframe_2018[[
                        'Yearly_Day',
                        'Hour',
                        'Minute',
                        'Second']].values]

                dframe_2018.drop(columns=['file', 'Hour', 'Minute', 'Yearly_Day', 'Second', 'Year'], inplace=True)

                dframe_2018.dropna(inplace=True)
                dframe_2018.set_index('date', inplace=True)
                dframe_2018.rename(columns={compound: f'{compound}_mr'}, inplace=True)
                if end_year == 2019:
                    dframe_2019 = pd.read_excel(r'C:\Users\ARL\Desktop\Summit_GC_2019\NMHC_results\Ambient_2019.xlsx',
                                                header=None)
                    dframe_2019.set_index(0, inplace=True)
                    dframe_transposed = dframe_2019.T
                    dframe_2019 = dframe_transposed.loc[:, [compound]]
                    dframe_2019 = dframe_2019.iloc[:,
                                  [j for j, c in enumerate(dframe_2019.columns) if j not in [0, 2, 3]]]

                    dframe_2019['file'] = dframe_transposed.iloc[:, 0]
                    dframe_2019['decimal_date'] = dframe_transposed.iloc[:, 39]
                    dframe_2019.dropna(inplace=True, subset=['file'])
                    dframe_2019['decmial_date_year'] = [(2019 + (float(row[0]) - 1) / 365) for row in
                                                        dframe_2019[['decimal_date']].values]

                    dframe_2019['Year'] = dframe_2019['file'].apply(lambda x: int(str(x)[0:4]))
                    dframe_2019['Yearly_Day'] = dframe_2019['file'].apply(lambda x: int(str(x)[4:7]))
                    dframe_2019['Hour'] = dframe_2019['file'].apply(lambda x: int(str(x)[7:9]))
                    dframe_2019['Minute'] = dframe_2019['file'].apply(lambda x: int(str(x)[9:11]))
                    dframe_2019['Second'] = dframe_2019['file'].apply(lambda x: int(str(x)[11:13]))

                    base_date = datetime(year=2019, month=1, day=1)
                    dframe_2019['date'] = [
                        base_date + timedelta(days=int(row[0] - 1), hours=int(row[1]), minutes=int(row[2]),
                                              seconds=int(row[3])
                                              ) for row in
                        dframe_2019[[
                            'Yearly_Day',
                            'Hour',
                            'Minute',
                            'Second']].values]

                    dframe_2019.drop(columns=['file', 'Hour', 'Minute', 'Yearly_Day', 'Second', 'Year'], inplace=True)

                    dframe_2019.dropna(inplace=True)
                    dframe_2019.set_index('date', inplace=True)
                    dframe_2019.rename(columns={compound: f'{compound}_mr'}, inplace=True)
    elif start_year == 2018:
        if end_year >= 2018:
            dframe_2018 = pd.read_excel(r'Z:\Data\Summit_GC\Summit_GC_2018\NMHC_results\Ambient_2018_V2.xlsx',
                                        header=None)
            dframe_2018.set_index(0, inplace=True)
            dframe_transposed = dframe_2018.T
            dframe_2018 = dframe_transposed.loc[:, [compound]]
            dframe_2018 = dframe_2018.iloc[:, [j for j, c in enumerate(dframe_2018.columns) if j not in [0, 2, 3]]]

            dframe_2018['file'] = dframe_transposed.iloc[:, 0]
            dframe_2018['decimal_date'] = dframe_transposed.iloc[:, 39]
            dframe_2018.dropna(inplace=True, subset=['file'])
            dframe_2018['decmial_date_year'] = [(2018 + (float(row[0]) - 1) / 365) for row in
                                                dframe_2018[['decimal_date']].values]

            dframe_2018['Year'] = dframe_2018['file'].apply(lambda x: int(str(x)[0:4]))
            dframe_2018['Yearly_Day'] = dframe_2018['file'].apply(lambda x: int(str(x)[4:7]))
            dframe_2018['Hour'] = dframe_2018['file'].apply(lambda x: int(str(x)[7:9]))
            dframe_2018['Minute'] = dframe_2018['file'].apply(lambda x: int(str(x)[9:11]))
            dframe_2018['Second'] = dframe_2018['file'].apply(lambda x: int(str(x)[11:13]))

            base_date = datetime(year=2018, month=1, day=1)
            dframe_2018['date'] = [base_date + timedelta(days=int(row[0] - 1), hours=int(row[1]), minutes=int(row[2]),
                                                         seconds=int(row[3])
                                                         ) for row in
                                   dframe_2018[[
                                       'Yearly_Day',
                                       'Hour',
                                       'Minute',
                                       'Second']].values]

            dframe_2018.drop(columns=['file', 'Hour', 'Minute', 'Yearly_Day', 'Second', 'Year'], inplace=True)

            dframe_2018.dropna(inplace=True)
            dframe_2018.set_index('date', inplace=True)
            dframe_2018.rename(columns={compound: f'{compound}_mr'}, inplace=True)
            if end_year == 2019:
                dframe_2019 = pd.read_excel(r'C:\Users\ARL\Desktop\Summit_GC_2019\NMHC_results\Ambient_2019.xlsx',
                                            header=None)
                dframe_2019.set_index(0, inplace=True)
                dframe_transposed = dframe_2019.T
                dframe_2019 = dframe_transposed.loc[:, [compound]]
                dframe_2019 = dframe_2019.iloc[:, [j for j, c in enumerate(dframe_2019.columns) if j not in [0, 2, 3]]]

                dframe_2019['file'] = dframe_transposed.iloc[:, 0]
                dframe_2019['decimal_date'] = dframe_transposed.iloc[:, 39]
                dframe_2019.dropna(inplace=True, subset=['file'])
                dframe_2019['decmial_date_year'] = [(2019 + (float(row[0]) - 1) / 365) for row in
                                                    dframe_2019[['decimal_date']].values]

                dframe_2019['Year'] = dframe_2019['file'].apply(lambda x: int(str(x)[0:4]))
                dframe_2019['Yearly_Day'] = dframe_2019['file'].apply(lambda x: int(str(x)[4:7]))
                dframe_2019['Hour'] = dframe_2019['file'].apply(lambda x: int(str(x)[7:9]))
                dframe_2019['Minute'] = dframe_2019['file'].apply(lambda x: int(str(x)[9:11]))
                dframe_2019['Second'] = dframe_2019['file'].apply(lambda x: int(str(x)[11:13]))

                base_date = datetime(year=2019, month=1, day=1)
                dframe_2019['date'] = [
                    base_date + timedelta(days=int(row[0] - 1), hours=int(row[1]), minutes=int(row[2]),
                                          seconds=int(row[3])
                                          ) for row in
                    dframe_2019[[
                        'Yearly_Day',
                        'Hour',
                        'Minute',
                        'Second']].values]

                dframe_2019.drop(columns=['file', 'Hour', 'Minute', 'Yearly_Day', 'Second', 'Year'], inplace=True)

                dframe_2019.dropna(inplace=True)
                dframe_2019.set_index('date', inplace=True)
                dframe_2019.rename(columns={compound: f'{compound}_mr'}, inplace=True)
    elif start_year == 2019:
        dframe_2019 = pd.read_excel(r'C:\Users\ARL\Desktop\Summit_GC_2019\NMHC_results\Ambient_2019.xlsx', header=None)
        dframe_2019.set_index(0, inplace=True)
        dframe_transposed = dframe_2019.T
        dframe_2019 = dframe_transposed.loc[:, [compound]]
        dframe_2019 = dframe_2019.iloc[:, [j for j, c in enumerate(dframe_2019.columns) if j not in [0, 2, 3]]]

        dframe_2019['file'] = dframe_transposed.iloc[:, 0]
        dframe_2019['decimal_date'] = dframe_transposed.iloc[:, 39]
        dframe_2019.dropna(inplace=True, subset=['file'])
        dframe_2019['decmial_date_year'] = [(2019 + (float(row[0]) - 1) / 365) for row in
                                            dframe_2019[['decimal_date']].values]

        dframe_2019['Year'] = dframe_2019['file'].apply(lambda x: int(str(x)[0:4]))
        dframe_2019['Yearly_Day'] = dframe_2019['file'].apply(lambda x: int(str(x)[4:7]))
        dframe_2019['Hour'] = dframe_2019['file'].apply(lambda x: int(str(x)[7:9]))
        dframe_2019['Minute'] = dframe_2019['file'].apply(lambda x: int(str(x)[9:11]))
        dframe_2019['Second'] = dframe_2019['file'].apply(lambda x: int(str(x)[11:13]))

        base_date = datetime(year=2019, month=1, day=1)
        dframe_2019['date'] = [base_date + timedelta(days=int(row[0] - 1), hours=int(row[1]), minutes=int(row[2]),
                                                     seconds=int(row[3])
                                                     ) for row in
                               dframe_2019[[
                                   'Yearly_Day',
                                   'Hour',
                                   'Minute',
                                   'Second']].values]

        dframe_2019.drop(columns=['file', 'Hour', 'Minute', 'Yearly_Day', 'Second', 'Year'], inplace=True)

        dframe_2019.dropna(inplace=True)
        dframe_2019.set_index('date', inplace=True)
        dframe_2019.rename(columns={compound: f'{compound}_mr'}, inplace=True)

    dframe = pd.concat([dframe_2017, dframe_2018, dframe_2019])
    dframe = dframe.loc[dframe.index < end_date]
    dframe = dframe.loc[dframe.index > start_date]
    return dframe

def excel_blanks(compound, start, end):

    start_date = datetime.strptime(start, '%Y-%m-%d %H:%M:%S')
    end_date = datetime.strptime(end, '%Y-%m-%d %H:%M:%S')
    start_year = int(start_date.year)
    end_year = int(end_date.year)

    dframe_2017 = pd.DataFrame()
    dframe_2018 = pd.DataFrame()
    dframe_2019 = pd.DataFrame()

    if start_year == 2017:
        if end_year >= 2017:
            dframe_2017 = pd.read_excel(r'Z:\Data\Summit_GC\Summit_GC_2017\NMHC_results\Blanks_2017.xlsx',
                                        header=None)
            dframe_2017.set_index(0, inplace=True)
            dframe_transposed = dframe_2017.T
            dframe_2017 = dframe_transposed.loc[:, [compound]]
            dframe_2017 = dframe_2017.iloc[:, [j for j, c in enumerate(dframe_2017.columns) if j not in [0, 2, 3]]]

            dframe_2017['file'] = dframe_transposed.iloc[:, 0]
            dframe_2017['decimal_date'] = dframe_transposed.iloc[:, 39]
            dframe_2017.dropna(inplace=True, subset=['file'])
            dframe_2017['decmial_date_year'] = [(2017 + (float(row[0]) - 1) / 365) for row in
                                                dframe_2017[['decimal_date']].values]
            dframe_2017['Year'] = dframe_2017['file'].apply(lambda x: int(str(x)[0:4]))
            dframe_2017['Yearly_Day'] = dframe_2017['file'].apply(lambda x: int(str(x)[4:7]))
            dframe_2017['Hour'] = dframe_2017['file'].apply(lambda x: int(str(x)[7:9]))
            dframe_2017['Minute'] = dframe_2017['file'].apply(lambda x: int(str(x)[9:11]))
            dframe_2017['Second'] = dframe_2017['file'].apply(lambda x: int(str(x)[11:13]))

            base_date = datetime(year=2017, month=1, day=1)
            dframe_2017['date'] = [base_date + timedelta(days=int(row[0] - 1), hours=int(row[1]), minutes=int(row[2]),
                                                         seconds=int(row[3])
                                                         ) for row in
                                   dframe_2017[[
                                       'Yearly_Day',
                                       'Hour',
                                       'Minute',
                                       'Second']].values]

            dframe_2017.drop(columns=['file', 'Year', 'Hour', 'Minute', 'Yearly_Day', 'Second'], inplace=True)
            dframe_2017 = dframe_2017[dframe_2017['date'] < end_date]
            dframe_2017 = dframe_2017[dframe_2017['date'] > start_date]
            dframe_2017.set_index('date', inplace=True)
            dframe_2017.rename(columns={compound: f'{compound}_mr'}, inplace=True)
            if end_year >= 2018:
                dframe_2018 = pd.read_excel(r'Z:\Data\Summit_GC\Summit_GC_2018\NMHC_results\Blanks_2018.xlsx',
                                            header=None)
                dframe_2018.set_index(0, inplace=True)
                dframe_transposed = dframe_2018.T
                dframe_2018 = dframe_transposed.loc[:, [compound]]
                dframe_2018 = dframe_2018.iloc[:, [j for j, c in enumerate(dframe_2018.columns) if j not in [0, 2, 3]]]

                dframe_2018['file'] = dframe_transposed.iloc[:, 0]
                dframe_2018['decimal_date'] = dframe_transposed.iloc[:, 39]
                dframe_2018.dropna(inplace=True, subset=['file'])
                dframe_2018['decmial_date_year'] = [(2018 + (float(row[0]) - 1) / 365) for row in
                                                    dframe_2018[['decimal_date']].values]
                dframe_2018['Year'] = dframe_2018['file'].apply(lambda x: int(str(x)[0:4]))
                dframe_2018['Yearly_Day'] = dframe_2018['file'].apply(lambda x: int(str(x)[4:7]))
                dframe_2018['Hour'] = dframe_2018['file'].apply(lambda x: int(str(x)[7:9]))
                dframe_2018['Minute'] = dframe_2018['file'].apply(lambda x: int(str(x)[9:11]))
                dframe_2018['Second'] = dframe_2018['file'].apply(lambda x: int(str(x)[11:13]))

                base_date = datetime(year=2018, month=1, day=1)
                dframe_2018['date'] = [
                    base_date + timedelta(days=int(row[0] - 1), hours=int(row[1]), minutes=int(row[2]),
                                          seconds=int(row[3])
                                          ) for row in
                    dframe_2018[[
                        'Yearly_Day',
                        'Hour',
                        'Minute',
                        'Second']].values]

                dframe_2018.drop(columns=['file', 'Year', 'Hour', 'Minute', 'Yearly_Day', 'Second'], inplace=True)
                dframe_2018 = dframe_2018[dframe_2018['date'] < end_date]
                dframe_2018 = dframe_2018[dframe_2018['date'] > start_date]
                dframe_2018.set_index('date', inplace=True)
                dframe_2018.rename(columns={compound: f'{compound}_mr'}, inplace=True)
                if end_year >= 2019:
                    dframe_2019 = pd.read_excel(r'C:\Users\ARL\Desktop\Summit_GC_2019\NMHC_results\Blanks_2019.xlsx',
                                                header=None)
                    dframe_2019.set_index(0, inplace=True)
                    dframe_transposed = dframe_2019.T
                    dframe_2019 = dframe_transposed.loc[:, [compound]]
                    dframe_2019 = dframe_2019.iloc[:,
                                  [j for j, c in enumerate(dframe_2019.columns) if j not in [0, 2, 3]]]

                    dframe_2019['file'] = dframe_transposed.iloc[:, 0]
                    dframe_2019['decimal_date'] = dframe_transposed.iloc[:, 39]
                    dframe_2019.dropna(inplace=True, subset=['file'])
                    dframe_2019['decmial_date_year'] = [(2019 + (float(row[0]) - 1) / 365) for row in
                                                        dframe_2019[['decimal_date']].values]
                    dframe_2019['Year'] = dframe_2019['file'].apply(lambda x: int(str(x)[0:4]))
                    dframe_2019['Yearly_Day'] = dframe_2019['file'].apply(lambda x: int(str(x)[4:7]))
                    dframe_2019['Hour'] = dframe_2019['file'].apply(lambda x: int(str(x)[7:9]))
                    dframe_2019['Minute'] = dframe_2019['file'].apply(lambda x: int(str(x)[9:11]))
                    dframe_2019['Second'] = dframe_2019['file'].apply(lambda x: int(str(x)[11:13]))

                    base_date = datetime(year=2019, month=1, day=1)
                    dframe_2019['date'] = [
                        base_date + timedelta(days=int(row[0] - 1), hours=int(row[1]), minutes=int(row[2]),
                                              seconds=int(row[3])
                                              ) for row in
                        dframe_2019[[
                            'Yearly_Day',
                            'Hour',
                            'Minute',
                            'Second']].values]

                    dframe_2019.drop(columns=['file', 'Year', 'Hour', 'Minute', 'Yearly_Day', 'Second'], inplace=True)
                    dframe_2019 = dframe_2019[dframe_2019['date'] < end_date]
                    dframe_2019 = dframe_2019[dframe_2019['date'] > start_date]
                    dframe_2019.set_index('date', inplace=True)
                    dframe_2019.rename(columns={compound: f'{compound}_mr'}, inplace=True)
    elif start_year == 2018:
        if end_year >= 2018:
            dframe_2018 = pd.read_excel(r'Z:\Data\Summit_GC\Summit_GC_2018\NMHC_results\Blanks_2018.xlsx',
                                        header=None)
            dframe_2018.set_index(0, inplace=True)
            dframe_transposed = dframe_2018.T
            dframe_2018 = dframe_transposed.loc[:, [compound]]
            dframe_2018 = dframe_2018.iloc[:, [j for j, c in enumerate(dframe_2018.columns) if j not in [0, 2, 3]]]

            dframe_2018['file'] = dframe_transposed.iloc[:, 0]
            dframe_2018['decimal_date'] = dframe_transposed.iloc[:, 39]
            dframe_2018.dropna(inplace=True, subset=['file'])
            dframe_2018['decmial_date_year'] = [(2018 + (float(row[0]) - 1) / 365) for row in
                                                dframe_2018[['decimal_date']].values]
            dframe_2018['Year'] = dframe_2018['file'].apply(lambda x: int(str(x)[0:4]))
            dframe_2018['Yearly_Day'] = dframe_2018['file'].apply(lambda x: int(str(x)[4:7]))
            dframe_2018['Hour'] = dframe_2018['file'].apply(lambda x: int(str(x)[7:9]))
            dframe_2018['Minute'] = dframe_2018['file'].apply(lambda x: int(str(x)[9:11]))
            dframe_2018['Second'] = dframe_2018['file'].apply(lambda x: int(str(x)[11:13]))

            base_date = datetime(year=2018, month=1, day=1)
            dframe_2018['date'] = [base_date + timedelta(days=int(row[0] - 1), hours=int(row[1]), minutes=int(row[2]),
                                                         seconds=int(row[3])
                                                         ) for row in
                                   dframe_2018[[
                                       'Yearly_Day',
                                       'Hour',
                                       'Minute',
                                       'Second']].values]

            dframe_2018.drop(columns=['file', 'Year', 'Hour', 'Minute', 'Yearly_Day', 'Second'], inplace=True)
            dframe_2018 = dframe_2018[dframe_2018['date'] < end_date]
            dframe_2018 = dframe_2018[dframe_2018['date'] > start_date]
            dframe_2018.set_index('date', inplace=True)
            dframe_2018.rename(columns={compound: f'{compound}_mr'}, inplace=True)
            if end_year >= 2019:
                dframe_2019 = pd.read_excel(r'C:\Users\ARL\Desktop\Summit_GC_2019\NMHC_results\Blanks_2019.xlsx',
                                            header=None)
                dframe_2019.set_index(0, inplace=True)
                dframe_transposed = dframe_2019.T
                dframe_2019 = dframe_transposed.loc[:, [compound]]
                dframe_2019 = dframe_2019.iloc[:, [j for j, c in enumerate(dframe_2019.columns) if j not in [0, 2, 3]]]

                dframe_2019['file'] = dframe_transposed.iloc[:, 0]
                dframe_2019['decimal_date'] = dframe_transposed.iloc[:, 39]
                dframe_2019.dropna(inplace=True, subset=['file'])
                dframe_2019['decmial_date_year'] = [(2019 + (float(row[0]) - 1) / 365) for row in
                                                    dframe_2019[['decimal_date']].values]
                dframe_2019['Year'] = dframe_2019['file'].apply(lambda x: int(str(x)[0:4]))
                dframe_2019['Yearly_Day'] = dframe_2019['file'].apply(lambda x: int(str(x)[4:7]))
                dframe_2019['Hour'] = dframe_2019['file'].apply(lambda x: int(str(x)[7:9]))
                dframe_2019['Minute'] = dframe_2019['file'].apply(lambda x: int(str(x)[9:11]))
                dframe_2019['Second'] = dframe_2019['file'].apply(lambda x: int(str(x)[11:13]))

                base_date = datetime(year=2019, month=1, day=1)
                dframe_2019['date'] = [
                    base_date + timedelta(days=int(row[0] - 1), hours=int(row[1]), minutes=int(row[2]),
                                          seconds=int(row[3])
                                          ) for row in
                    dframe_2019[[
                        'Yearly_Day',
                        'Hour',
                        'Minute',
                        'Second']].values]

                dframe_2019.drop(columns=['file', 'Year', 'Hour', 'Minute', 'Yearly_Day', 'Second'], inplace=True)
                dframe_2019 = dframe_2019[dframe_2019['date'] < end_date]
                dframe_2019 = dframe_2019[dframe_2019['date'] > start_date]
                dframe_2019.set_index('date', inplace=True)
                dframe_2019.rename(columns={compound: f'{compound}_mr'}, inplace=True)
    elif start_year == 2019:
        dframe_2019 = pd.read_excel(r'C:\Users\ARL\Desktop\Summit_GC_2019\NMHC_results\Blanks_2019.xlsx', header=None)
        dframe_2019.set_index(0, inplace=True)
        dframe_transposed = dframe_2019.T
        dframe_2019 = dframe_transposed.loc[:, [compound]]
        dframe_2019 = dframe_2019.iloc[:, [j for j, c in enumerate(dframe_2019.columns) if j not in [0, 2, 3]]]

        dframe_2019['file'] = dframe_transposed.iloc[:, 0]
        dframe_2019['decimal_date'] = dframe_transposed.iloc[:, 39]
        dframe_2019.dropna(inplace=True, subset=['file'])
        dframe_2019['decmial_date_year'] = [(2019 + (float(row[0]) - 1) / 365) for row in
                                            dframe_2019[['decimal_date']].values]
        dframe_2019['Year'] = dframe_2019['file'].apply(lambda x: int(str(x)[0:4]))
        dframe_2019['Yearly_Day'] = dframe_2019['file'].apply(lambda x: int(str(x)[4:7]))
        dframe_2019['Hour'] = dframe_2019['file'].apply(lambda x: int(str(x)[7:9]))
        dframe_2019['Minute'] = dframe_2019['file'].apply(lambda x: int(str(x)[9:11]))
        dframe_2019['Second'] = dframe_2019['file'].apply(lambda x: int(str(x)[11:13]))

        base_date = datetime(year=2019, month=1, day=1)
        dframe_2019['date'] = [base_date + timedelta(days=int(row[0] - 1), hours=int(row[1]), minutes=int(row[2]),
                                                     seconds=int(row[3])
                                                     ) for row in
                               dframe_2019[[
                                   'Yearly_Day',
                                   'Hour',
                                   'Minute',
                                   'Second']].values]

        dframe_2019.drop(columns=['file', 'Year', 'Hour', 'Minute', 'Yearly_Day', 'Second'], inplace=True)
        dframe_2019 = dframe_2019[dframe_2019['date'] < end_date]
        dframe_2019 = dframe_2019[dframe_2019['date'] > start_date]
        dframe_2019.set_index('date', inplace=True)
        dframe_2019.rename(columns={compound: f'{compound}_mr'}, inplace=True)


    dframe = pd.concat([dframe_2017, dframe_2018, dframe_2019])
    dframe = dframe.loc[dframe.index < end_date]
    dframe = dframe.loc[dframe.index > start_date]
    dframe.fillna(value=99999, inplace=True)

    return dframe

def excel_trap_blanks(compound, start, end):
    start_date = datetime.strptime(start, '%Y-%m-%d %H:%M:%S')
    end_date = datetime.strptime(end, '%Y-%m-%d %H:%M:%S')
    start_year = int(start_date.year)
    end_year = int(end_date.year)

    dframe_2017 = pd.DataFrame()
    dframe_2018 = pd.DataFrame()
    dframe_2019 = pd.DataFrame()

    if start_year == 2017:
        if end_year >= 2017:
            dframe_2017 = pd.read_excel(r'Z:\Data\Summit_GC\Summit_GC_2017\NMHC_results\TrapBlanks_2017.xlsx',
                                        header=None)
            dframe_2017.set_index(0, inplace=True)
            dframe_transposed = dframe_2017.T
            dframe_2017 = dframe_transposed.loc[:, [compound]]
            dframe_2017 = dframe_2017.iloc[:, [j for j, c in enumerate(dframe_2017.columns) if j not in [0, 2, 3]]]

            dframe_2017['file'] = dframe_transposed.iloc[:, 0]
            dframe_2017['decimal_date'] = dframe_transposed.iloc[:, 39]
            dframe_2017.dropna(inplace=True, subset=['file'])
            dframe_2017['decmial_date_year'] = [(2017 + (float(row[0]) - 1) / 365) for row in
                                                dframe_2017[['decimal_date']].values]
            dframe_2017['Year'] = dframe_2017['file'].apply(lambda x: int(str(x)[0:4]))
            dframe_2017['Yearly_Day'] = dframe_2017['file'].apply(lambda x: int(str(x)[4:7]))
            dframe_2017['Hour'] = dframe_2017['file'].apply(lambda x: int(str(x)[7:9]))
            dframe_2017['Minute'] = dframe_2017['file'].apply(lambda x: int(str(x)[9:11]))
            dframe_2017['Second'] = dframe_2017['file'].apply(lambda x: int(str(x)[11:13]))

            base_date = datetime(year=2017, month=1, day=1)
            dframe_2017['date'] = [base_date + timedelta(days=int(row[0] - 1), hours=int(row[1]), minutes=int(row[2]),
                                                         seconds=int(row[3])
                                                         ) for row in
                                   dframe_2017[[
                                       'Yearly_Day',
                                       'Hour',
                                       'Minute',
                                       'Second']].values]

            dframe_2017.drop(columns=['file', 'Year', 'Hour', 'Minute', 'Yearly_Day', 'Second'], inplace=True)
            dframe_2017 = dframe_2017[dframe_2017['date'] < end_date]
            dframe_2017 = dframe_2017[dframe_2017['date'] > start_date]
            dframe_2017.set_index('date', inplace=True)
            dframe_2017.rename(columns={compound: f'{compound}_mr'}, inplace=True)
            if end_year >= 2018:
                dframe_2018 = pd.read_excel(r'Z:\Data\Summit_GC\Summit_GC_2018\NMHC_results\TrapBlanks_2018.xlsx',
                                            header=None)
                dframe_2018.set_index(0, inplace=True)
                dframe_transposed = dframe_2018.T
                dframe_2018 = dframe_transposed.loc[:, [compound]]
                dframe_2018 = dframe_2018.iloc[:, [j for j, c in enumerate(dframe_2018.columns) if j not in [0, 2, 3]]]

                dframe_2018['file'] = dframe_transposed.iloc[:, 0]
                dframe_2018['decimal_date'] = dframe_transposed.iloc[:, 39]
                dframe_2018.dropna(inplace=True, subset=['file'])
                dframe_2018['decmial_date_year'] = [(2018 + (float(row[0]) - 1) / 365) for row in
                                                    dframe_2018[['decimal_date']].values]
                dframe_2018['Year'] = dframe_2018['file'].apply(lambda x: int(str(x)[0:4]))
                dframe_2018['Yearly_Day'] = dframe_2018['file'].apply(lambda x: int(str(x)[4:7]))
                dframe_2018['Hour'] = dframe_2018['file'].apply(lambda x: int(str(x)[7:9]))
                dframe_2018['Minute'] = dframe_2018['file'].apply(lambda x: int(str(x)[9:11]))
                dframe_2018['Second'] = dframe_2018['file'].apply(lambda x: int(str(x)[11:13]))

                base_date = datetime(year=2018, month=1, day=1)
                dframe_2018['date'] = [
                    base_date + timedelta(days=int(row[0] - 1), hours=int(row[1]), minutes=int(row[2]),
                                          seconds=int(row[3])
                                          ) for row in
                    dframe_2018[[
                        'Yearly_Day',
                        'Hour',
                        'Minute',
                        'Second']].values]

                dframe_2018.drop(columns=['file', 'Year', 'Hour', 'Minute', 'Yearly_Day', 'Second'], inplace=True)
                dframe_2018 = dframe_2018[dframe_2018['date'] < end_date]
                dframe_2018 = dframe_2018[dframe_2018['date'] > start_date]
                dframe_2018.set_index('date', inplace=True)
                dframe_2018.rename(columns={compound: f'{compound}_mr'}, inplace=True)
                if end_year >= 2019:
                    dframe_2019 = pd.read_excel(
                        r'C:\Users\ARL\Desktop\Summit_GC_2019\NMHC_results\TrapBlanks_2019.xlsx',
                                                header=None)
                    dframe_2019.set_index(0, inplace=True)
                    dframe_transposed = dframe_2019.T
                    dframe_2019 = dframe_transposed.loc[:, [compound]]
                    dframe_2019 = dframe_2019.iloc[:,
                                  [j for j, c in enumerate(dframe_2019.columns) if j not in [0, 2, 3]]]

                    dframe_2019['file'] = dframe_transposed.iloc[:, 0]
                    dframe_2019['decimal_date'] = dframe_transposed.iloc[:, 39]
                    dframe_2019.dropna(inplace=True, subset=['file'])
                    dframe_2019['decmial_date_year'] = [(2019 + (float(row[0]) - 1) / 365) for row in
                                                        dframe_2019[['decimal_date']].values]
                    dframe_2019['Year'] = dframe_2019['file'].apply(lambda x: int(str(x)[0:4]))
                    dframe_2019['Yearly_Day'] = dframe_2019['file'].apply(lambda x: int(str(x)[4:7]))
                    dframe_2019['Hour'] = dframe_2019['file'].apply(lambda x: int(str(x)[7:9]))
                    dframe_2019['Minute'] = dframe_2019['file'].apply(lambda x: int(str(x)[9:11]))
                    dframe_2019['Second'] = dframe_2019['file'].apply(lambda x: int(str(x)[11:13]))

                    base_date = datetime(year=2019, month=1, day=1)
                    dframe_2019['date'] = [
                        base_date + timedelta(days=int(row[0] - 1), hours=int(row[1]), minutes=int(row[2]),
                                              seconds=int(row[3])
                                              ) for row in
                        dframe_2019[[
                            'Yearly_Day',
                            'Hour',
                            'Minute',
                            'Second']].values]

                    dframe_2019.drop(columns=['file', 'Year', 'Hour', 'Minute', 'Yearly_Day', 'Second'], inplace=True)
                    dframe_2019 = dframe_2019[dframe_2019['date'] < end_date]
                    dframe_2019 = dframe_2019[dframe_2019['date'] > start_date]
                    dframe_2019.set_index('date', inplace=True)
                    dframe_2019.rename(columns={compound: f'{compound}_mr'}, inplace=True)
    elif start_year == 2018:
        if end_year >= 2018:
            dframe_2018 = pd.read_excel(r'Z:\Data\Summit_GC\Summit_GC_2018\NMHC_results\TrapBlanks_2018.xlsx',
                                        header=None)
            dframe_2018.set_index(0, inplace=True)
            dframe_transposed = dframe_2018.T
            dframe_2018 = dframe_transposed.loc[:, [compound]]
            dframe_2018 = dframe_2018.iloc[:, [j for j, c in enumerate(dframe_2018.columns) if j not in [0, 2, 3]]]

            dframe_2018['file'] = dframe_transposed.iloc[:, 0]
            dframe_2018['decimal_date'] = dframe_transposed.iloc[:, 39]
            dframe_2018.dropna(inplace=True, subset=['file'])
            dframe_2018['decmial_date_year'] = [(2018 + (float(row[0]) - 1) / 365) for row in
                                                dframe_2018[['decimal_date']].values]
            dframe_2018['Year'] = dframe_2018['file'].apply(lambda x: int(str(x)[0:4]))
            dframe_2018['Yearly_Day'] = dframe_2018['file'].apply(lambda x: int(str(x)[4:7]))
            dframe_2018['Hour'] = dframe_2018['file'].apply(lambda x: int(str(x)[7:9]))
            dframe_2018['Minute'] = dframe_2018['file'].apply(lambda x: int(str(x)[9:11]))
            dframe_2018['Second'] = dframe_2018['file'].apply(lambda x: int(str(x)[11:13]))

            base_date = datetime(year=2018, month=1, day=1)
            dframe_2018['date'] = [base_date + timedelta(days=int(row[0] - 1), hours=int(row[1]), minutes=int(row[2]),
                                                         seconds=int(row[3])
                                                         ) for row in
                                   dframe_2018[[
                                       'Yearly_Day',
                                       'Hour',
                                       'Minute',
                                       'Second']].values]

            dframe_2018.drop(columns=['file', 'Year', 'Hour', 'Minute', 'Yearly_Day', 'Second'], inplace=True)
            dframe_2018 = dframe_2018[dframe_2018['date'] < end_date]
            dframe_2018 = dframe_2018[dframe_2018['date'] > start_date]
            dframe_2018.set_index('date', inplace=True)
            dframe_2018.rename(columns={compound: f'{compound}_mr'}, inplace=True)
            if end_year >= 2019:
                dframe_2019 = pd.read_excel(r'C:\Users\ARL\Desktop\Summit_GC_2019\NMHC_results\TrapBlanks_2019.xlsx',
                                            header=None)
                dframe_2019.set_index(0, inplace=True)
                dframe_transposed = dframe_2019.T
                dframe_2019 = dframe_transposed.loc[:, [compound]]
                dframe_2019 = dframe_2019.iloc[:, [j for j, c in enumerate(dframe_2019.columns) if j not in [0, 2, 3]]]

                dframe_2019['file'] = dframe_transposed.iloc[:, 0]
                dframe_2019['decimal_date'] = dframe_transposed.iloc[:, 39]
                dframe_2019.dropna(inplace=True, subset=['file'])
                dframe_2019['decmial_date_year'] = [(2019 + (float(row[0]) - 1) / 365) for row in
                                                    dframe_2019[['decimal_date']].values]
                dframe_2019['Year'] = dframe_2019['file'].apply(lambda x: int(str(x)[0:4]))
                dframe_2019['Yearly_Day'] = dframe_2019['file'].apply(lambda x: int(str(x)[4:7]))
                dframe_2019['Hour'] = dframe_2019['file'].apply(lambda x: int(str(x)[7:9]))
                dframe_2019['Minute'] = dframe_2019['file'].apply(lambda x: int(str(x)[9:11]))
                dframe_2019['Second'] = dframe_2019['file'].apply(lambda x: int(str(x)[11:13]))

                base_date = datetime(year=2019, month=1, day=1)
                dframe_2019['date'] = [
                    base_date + timedelta(days=int(row[0] - 1), hours=int(row[1]), minutes=int(row[2]),
                                          seconds=int(row[3])
                                          ) for row in
                    dframe_2019[[
                        'Yearly_Day',
                        'Hour',
                        'Minute',
                        'Second']].values]

                dframe_2019.drop(columns=['file', 'Year', 'Hour', 'Minute', 'Yearly_Day', 'Second'], inplace=True)
                dframe_2019 = dframe_2019[dframe_2019['date'] < end_date]
                dframe_2019 = dframe_2019[dframe_2019['date'] > start_date]
                dframe_2019.set_index('date', inplace=True)
                dframe_2019.rename(columns={compound: f'{compound}_mr'}, inplace=True)
    elif start_year == 2019:
        dframe_2019 = pd.read_excel(r'C:\Users\ARL\Desktop\Summit_GC_2019\NMHC_results\TrapBlanks_2019.xlsx',
                                    header=None)
        dframe_2019.set_index(0, inplace=True)
        dframe_transposed = dframe_2019.T
        dframe_2019 = dframe_transposed.loc[:, [compound]]
        dframe_2019 = dframe_2019.iloc[:, [j for j, c in enumerate(dframe_2019.columns) if j not in [0, 2, 3]]]

        dframe_2019['file'] = dframe_transposed.iloc[:, 0]
        dframe_2019['decimal_date'] = dframe_transposed.iloc[:, 39]
        dframe_2019.dropna(inplace=True, subset=['file'])
        dframe_2019['decmial_date_year'] = [(2019 + (float(row[0]) - 1) / 365) for row in
                                            dframe_2019[['decimal_date']].values]
        dframe_2019['Year'] = dframe_2019['file'].apply(lambda x: int(str(x)[0:4]))
        dframe_2019['Yearly_Day'] = dframe_2019['file'].apply(lambda x: int(str(x)[4:7]))
        dframe_2019['Hour'] = dframe_2019['file'].apply(lambda x: int(str(x)[7:9]))
        dframe_2019['Minute'] = dframe_2019['file'].apply(lambda x: int(str(x)[9:11]))
        dframe_2019['Second'] = dframe_2019['file'].apply(lambda x: int(str(x)[11:13]))

        base_date = datetime(year=2019, month=1, day=1)
        dframe_2019['date'] = [base_date + timedelta(days=int(row[0] - 1), hours=int(row[1]), minutes=int(row[2]),
                                                     seconds=int(row[3])
                                                     ) for row in
                               dframe_2019[[
                                   'Yearly_Day',
                                   'Hour',
                                   'Minute',
                                   'Second']].values]

        dframe_2019.drop(columns=['file', 'Year', 'Hour', 'Minute', 'Yearly_Day', 'Second'], inplace=True)
        dframe_2019 = dframe_2019[dframe_2019['date'] < end_date]
        dframe_2019 = dframe_2019[dframe_2019['date'] > start_date]
        dframe_2019.set_index('date', inplace=True)
        dframe_2019.rename(columns={compound: f'{compound}_mr'}, inplace=True)

    dframe = pd.concat([dframe_2017, dframe_2018, dframe_2019])
    dframe = dframe.loc[dframe.index < end_date]
    dframe = dframe.loc[dframe.index > start_date]
    dframe.fillna(value=99999, inplace=True)

    return dframe

def excel_rf_BH(compound, start, end):
    start_date = datetime.strptime(start, '%Y-%m-%d %H:%M:%S')
    end_date = datetime.strptime(end, '%Y-%m-%d %H:%M:%S')
    start_year = int(start_date.year)
    end_year = int(end_date.year)

    dframe_2017 = pd.DataFrame()
    dframe_2018 = pd.DataFrame()
    dframe_2019 = pd.DataFrame()

    if start_year == 2017:
        if end_year >= 2017:
            dframe_2017 = pd.read_excel(r'Z:\Data\Summit_GC\Summit_GC_2017\NMHC_results\BH_STD_2017.xlsx',
                                        header=None)
            dframe_2017.set_index(0, inplace=True)
            dframe_transposed = dframe_2017.T
            dframe_2017 = dframe_transposed.loc[:, [compound]]
            dframe_2017 = dframe_2017.iloc[:, [j for j, c in enumerate(dframe_2017.columns) if j not in [0, 2, 3]]]

            dframe_2017['file'] = dframe_transposed.iloc[:, 0]
            dframe_2017['decimal_date'] = dframe_transposed.iloc[:, 39]
            dframe_2017.dropna(inplace=True, subset=['file'])
            dframe_2017['decmial_date_year'] = [(2017 + (float(row[0]) - 1) / 365) for row in
                                                dframe_2017[['decimal_date']].values]
            dframe_2017['Year'] = dframe_2017['file'].apply(lambda x: int(str(x)[0:4]))
            dframe_2017['Yearly_Day'] = dframe_2017['file'].apply(lambda x: int(str(x)[4:7]))
            dframe_2017['Hour'] = dframe_2017['file'].apply(lambda x: int(str(x)[7:9]))
            dframe_2017['Minute'] = dframe_2017['file'].apply(lambda x: int(str(x)[9:11]))
            dframe_2017['Second'] = dframe_2017['file'].apply(lambda x: int(str(x)[11:13]))

            base_date = datetime(year=2017, month=1, day=1)
            dframe_2017['date'] = [base_date + timedelta(days=int(row[0] - 1), hours=int(row[1]), minutes=int(row[2]),
                                                         seconds=int(row[3])
                                                         ) for row in
                                   dframe_2017[[
                                       'Yearly_Day',
                                       'Hour',
                                       'Minute',
                                       'Second']].values]

            dframe_2017.drop(columns=['file', 'Year', 'Hour', 'Minute', 'Yearly_Day', 'Second'], inplace=True)
            dframe_2017 = dframe_2017[dframe_2017['date'] < end_date]
            dframe_2017 = dframe_2017[dframe_2017['date'] > start_date]
            dframe_2017.set_index('date', inplace=True)
            dframe_2017.rename(columns={compound: f'{compound}_mr'}, inplace=True)
            if end_year >= 2018:
                dframe_2018 = pd.read_excel(r'Z:\Data\Summit_GC\Summit_GC_2018\NMHC_results\BH_STD_2018.xlsx',
                                            header=None)
                dframe_2018.set_index(0, inplace=True)
                dframe_transposed = dframe_2018.T
                dframe_2018 = dframe_transposed.loc[:, [compound]]
                dframe_2018 = dframe_2018.iloc[:, [j for j, c in enumerate(dframe_2018.columns) if j not in [0, 2, 3]]]

                dframe_2018['file'] = dframe_transposed.iloc[:, 0]
                dframe_2018['decimal_date'] = dframe_transposed.iloc[:, 39]
                dframe_2018.dropna(inplace=True, subset=['file'])
                dframe_2018['decmial_date_year'] = [(2018 + (float(row[0]) - 1) / 365) for row in
                                                    dframe_2018[['decimal_date']].values]
                dframe_2018['Year'] = dframe_2018['file'].apply(lambda x: int(str(x)[0:4]))
                dframe_2018['Yearly_Day'] = dframe_2018['file'].apply(lambda x: int(str(x)[4:7]))
                dframe_2018['Hour'] = dframe_2018['file'].apply(lambda x: int(str(x)[7:9]))
                dframe_2018['Minute'] = dframe_2018['file'].apply(lambda x: int(str(x)[9:11]))
                dframe_2018['Second'] = dframe_2018['file'].apply(lambda x: int(str(x)[11:13]))

                base_date = datetime(year=2018, month=1, day=1)
                dframe_2018['date'] = [
                    base_date + timedelta(days=int(row[0] - 1), hours=int(row[1]), minutes=int(row[2]),
                                          seconds=int(row[3])
                                          ) for row in
                    dframe_2018[[
                        'Yearly_Day',
                        'Hour',
                        'Minute',
                        'Second']].values]

                dframe_2018.drop(columns=['file', 'Year', 'Hour', 'Minute', 'Yearly_Day', 'Second'], inplace=True)
                dframe_2018 = dframe_2018[dframe_2018['date'] < end_date]
                dframe_2018 = dframe_2018[dframe_2018['date'] > start_date]
                dframe_2018.set_index('date', inplace=True)
                dframe_2018.rename(columns={compound: f'{compound}_mr'}, inplace=True)
                if end_year >= 2019:
                    dframe_2019 = pd.read_excel(
                        r'C:\Users\ARL\Desktop\Summit_GC_2019\NMHC_results\BH_STD_2019.xlsx',
                        header=None)
                    dframe_2019.set_index(0, inplace=True)
                    dframe_transposed = dframe_2019.T
                    dframe_2019 = dframe_transposed.loc[:, [compound]]
                    dframe_2019 = dframe_2019.iloc[:,
                                  [j for j, c in enumerate(dframe_2019.columns) if j not in [0, 2, 3]]]

                    dframe_2019['file'] = dframe_transposed.iloc[:, 0]
                    dframe_2019['decimal_date'] = dframe_transposed.iloc[:, 39]
                    dframe_2019.dropna(inplace=True, subset=['file'])
                    dframe_2019['decmial_date_year'] = [(2019 + (float(row[0]) - 1) / 365) for row in
                                                        dframe_2019[['decimal_date']].values]
                    dframe_2019['Year'] = dframe_2019['file'].apply(lambda x: int(str(x)[0:4]))
                    dframe_2019['Yearly_Day'] = dframe_2019['file'].apply(lambda x: int(str(x)[4:7]))
                    dframe_2019['Hour'] = dframe_2019['file'].apply(lambda x: int(str(x)[7:9]))
                    dframe_2019['Minute'] = dframe_2019['file'].apply(lambda x: int(str(x)[9:11]))
                    dframe_2019['Second'] = dframe_2019['file'].apply(lambda x: int(str(x)[11:13]))

                    base_date = datetime(year=2019, month=1, day=1)
                    dframe_2019['date'] = [
                        base_date + timedelta(days=int(row[0] - 1), hours=int(row[1]), minutes=int(row[2]),
                                              seconds=int(row[3])
                                              ) for row in
                        dframe_2019[[
                            'Yearly_Day',
                            'Hour',
                            'Minute',
                            'Second']].values]

                    dframe_2019.drop(columns=['file', 'Year', 'Hour', 'Minute', 'Yearly_Day', 'Second'], inplace=True)
                    dframe_2019 = dframe_2019[dframe_2019['date'] < end_date]
                    dframe_2019 = dframe_2019[dframe_2019['date'] > start_date]
                    dframe_2019.set_index('date', inplace=True)
                    dframe_2019.rename(columns={compound: f'{compound}_mr'}, inplace=True)
    elif start_year == 2018:
        if end_year >= 2018:
            dframe_2018 = pd.read_excel(r'Z:\Data\Summit_GC\Summit_GC_2018\NMHC_results\BH_STD_2018.xlsx',
                                        header=None)
            dframe_2018.set_index(0, inplace=True)
            dframe_transposed = dframe_2018.T
            dframe_2018 = dframe_transposed.loc[:, [compound]]
            dframe_2018 = dframe_2018.iloc[:, [j for j, c in enumerate(dframe_2018.columns) if j not in [0, 2, 3]]]

            dframe_2018['file'] = dframe_transposed.iloc[:, 0]
            dframe_2018['decimal_date'] = dframe_transposed.iloc[:, 39]
            dframe_2018.dropna(inplace=True, subset=['file'])
            dframe_2018['decmial_date_year'] = [(2018 + (float(row[0]) - 1) / 365) for row in
                                                dframe_2018[['decimal_date']].values]
            dframe_2018['Year'] = dframe_2018['file'].apply(lambda x: int(str(x)[0:4]))
            dframe_2018['Yearly_Day'] = dframe_2018['file'].apply(lambda x: int(str(x)[4:7]))
            dframe_2018['Hour'] = dframe_2018['file'].apply(lambda x: int(str(x)[7:9]))
            dframe_2018['Minute'] = dframe_2018['file'].apply(lambda x: int(str(x)[9:11]))
            dframe_2018['Second'] = dframe_2018['file'].apply(lambda x: int(str(x)[11:13]))

            base_date = datetime(year=2018, month=1, day=1)
            dframe_2018['date'] = [base_date + timedelta(days=int(row[0] - 1), hours=int(row[1]), minutes=int(row[2]),
                                                         seconds=int(row[3])
                                                         ) for row in
                                   dframe_2018[[
                                       'Yearly_Day',
                                       'Hour',
                                       'Minute',
                                       'Second']].values]

            dframe_2018.drop(columns=['file', 'Year', 'Hour', 'Minute', 'Yearly_Day', 'Second'], inplace=True)
            dframe_2018 = dframe_2018[dframe_2018['date'] < end_date]
            dframe_2018 = dframe_2018[dframe_2018['date'] > start_date]
            dframe_2018.set_index('date', inplace=True)
            dframe_2018.rename(columns={compound: f'{compound}_mr'}, inplace=True)
            if end_year >= 2019:
                dframe_2019 = pd.read_excel(r'C:\Users\ARL\Desktop\Summit_GC_2019\NMHC_results\BH_STD_2019.xlsx',
                                            header=None)
                dframe_2019.set_index(0, inplace=True)
                dframe_transposed = dframe_2019.T
                dframe_2019 = dframe_transposed.loc[:, [compound]]
                dframe_2019 = dframe_2019.iloc[:, [j for j, c in enumerate(dframe_2019.columns) if j not in [0, 2, 3]]]

                dframe_2019['file'] = dframe_transposed.iloc[:, 0]
                dframe_2019['decimal_date'] = dframe_transposed.iloc[:, 39]
                dframe_2019.dropna(inplace=True, subset=['file'])
                dframe_2019['decmial_date_year'] = [(2019 + (float(row[0]) - 1) / 365) for row in
                                                    dframe_2019[['decimal_date']].values]
                dframe_2019['Year'] = dframe_2019['file'].apply(lambda x: int(str(x)[0:4]))
                dframe_2019['Yearly_Day'] = dframe_2019['file'].apply(lambda x: int(str(x)[4:7]))
                dframe_2019['Hour'] = dframe_2019['file'].apply(lambda x: int(str(x)[7:9]))
                dframe_2019['Minute'] = dframe_2019['file'].apply(lambda x: int(str(x)[9:11]))
                dframe_2019['Second'] = dframe_2019['file'].apply(lambda x: int(str(x)[11:13]))

                base_date = datetime(year=2019, month=1, day=1)
                dframe_2019['date'] = [
                    base_date + timedelta(days=int(row[0] - 1), hours=int(row[1]), minutes=int(row[2]),
                                          seconds=int(row[3])
                                          ) for row in
                    dframe_2019[[
                        'Yearly_Day',
                        'Hour',
                        'Minute',
                        'Second']].values]

                dframe_2019.drop(columns=['file', 'Year', 'Hour', 'Minute', 'Yearly_Day', 'Second'], inplace=True)
                dframe_2019 = dframe_2019[dframe_2019['date'] < end_date]
                dframe_2019 = dframe_2019[dframe_2019['date'] > start_date]
                dframe_2019.set_index('date', inplace=True)
                dframe_2019.rename(columns={compound: f'{compound}_mr'}, inplace=True)
    elif start_year == 2019:
        dframe_2019 = pd.read_excel(r'C:\Users\ARL\Desktop\Summit_GC_2019\NMHC_results\BH_STD_2019.xlsx',
                                    header=None)
        dframe_2019.set_index(0, inplace=True)
        dframe_transposed = dframe_2019.T
        dframe_2019 = dframe_transposed.loc[:, [compound]]
        dframe_2019 = dframe_2019.iloc[:, [j for j, c in enumerate(dframe_2019.columns) if j not in [0, 2, 3]]]

        dframe_2019['file'] = dframe_transposed.iloc[:, 0]
        dframe_2019['decimal_date'] = dframe_transposed.iloc[:, 39]
        dframe_2019.dropna(inplace=True, subset=['file'])
        dframe_2019['decmial_date_year'] = [(2019 + (float(row[0]) - 1) / 365) for row in
                                            dframe_2019[['decimal_date']].values]
        dframe_2019['Year'] = dframe_2019['file'].apply(lambda x: int(str(x)[0:4]))
        dframe_2019['Yearly_Day'] = dframe_2019['file'].apply(lambda x: int(str(x)[4:7]))
        dframe_2019['Hour'] = dframe_2019['file'].apply(lambda x: int(str(x)[7:9]))
        dframe_2019['Minute'] = dframe_2019['file'].apply(lambda x: int(str(x)[9:11]))
        dframe_2019['Second'] = dframe_2019['file'].apply(lambda x: int(str(x)[11:13]))

        base_date = datetime(year=2019, month=1, day=1)
        dframe_2019['date'] = [base_date + timedelta(days=int(row[0] - 1), hours=int(row[1]), minutes=int(row[2]),
                                                     seconds=int(row[3])
                                                     ) for row in
                               dframe_2019[[
                                   'Yearly_Day',
                                   'Hour',
                                   'Minute',
                                   'Second']].values]

        dframe_2019.drop(columns=['file', 'Year', 'Hour', 'Minute', 'Yearly_Day', 'Second'], inplace=True)
        dframe_2019 = dframe_2019[dframe_2019['date'] < end_date]
        dframe_2019 = dframe_2019[dframe_2019['date'] > start_date]
        dframe_2019.set_index('date', inplace=True)
        dframe_2019.rename(columns={compound: f'{compound}_mr'}, inplace=True)

    dframe = pd.concat([dframe_2017, dframe_2018, dframe_2019])
    dframe = dframe.loc[dframe.index < end_date]
    dframe = dframe.loc[dframe.index > start_date]
    dframe.fillna(value=99999, inplace=True)

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
    dframe.fillna(value=99999, inplace=True)
    dframe.set_index('date', inplace=True)
    dframe.rename(columns={compound: f'{compound}_rf'}, inplace=True)

    return dframe

def excel_rf_BA(compound, start, end):
    start_date = datetime.strptime(start, '%Y-%m-%d %H:%M:%S')
    end_date = datetime.strptime(end, '%Y-%m-%d %H:%M:%S')
    start_year = int(start_date.year)
    end_year = int(end_date.year)

    dframe_2017 = pd.DataFrame()
    dframe_2018 = pd.DataFrame()
    dframe_2019 = pd.DataFrame()

    if start_year == 2017:
        if end_year >= 2017:
            dframe_2017 = pd.read_excel(r'Z:\Data\Summit_GC\Summit_GC_2017\NMHC_results\BA_STD_2017.xlsx',
                                        sheet_name='BA 2017 data',
                                        header=None)
            dframe_2017.set_index(0, inplace=True)
            dframe_transposed = dframe_2017.T
            dframe_2017 = dframe_transposed.loc[:, [compound]]
            dframe_2017 = dframe_2017.iloc[:, [j for j, c in enumerate(dframe_2017.columns) if j not in [0, 2, 3]]]

            dframe_2017['file'] = dframe_transposed.iloc[:, 0]
            dframe_2017['decimal_date'] = dframe_transposed.iloc[:, 39]
            dframe_2017.dropna(inplace=True, subset=['file'])
            dframe_2017['decmial_date_year'] = [(2017 + (float(row[0]) - 1) / 365) for row in
                                                dframe_2017[['decimal_date']].values]
            dframe_2017['Year'] = dframe_2017['file'].apply(lambda x: int(str(x)[0:4]))
            dframe_2017['Yearly_Day'] = dframe_2017['file'].apply(lambda x: int(str(x)[4:7]))
            dframe_2017['Hour'] = dframe_2017['file'].apply(lambda x: int(str(x)[7:9]))
            dframe_2017['Minute'] = dframe_2017['file'].apply(lambda x: int(str(x)[9:11]))
            dframe_2017['Second'] = dframe_2017['file'].apply(lambda x: int(str(x)[11:13]))

            base_date = datetime(year=2017, month=1, day=1)
            dframe_2017['date'] = [base_date + timedelta(days=int(row[0] - 1), hours=int(row[1]), minutes=int(row[2]),
                                                         seconds=int(row[3])
                                                         ) for row in
                                   dframe_2017[[
                                       'Yearly_Day',
                                       'Hour',
                                       'Minute',
                                       'Second']].values]

            dframe_2017.drop(columns=['file', 'Year', 'Hour', 'Minute', 'Yearly_Day', 'Second'], inplace=True)
            dframe_2017 = dframe_2017[dframe_2017['date'] < end_date]
            dframe_2017 = dframe_2017[dframe_2017['date'] > start_date]
            dframe_2017.set_index('date', inplace=True)
            dframe_2017.rename(columns={compound: f'{compound}_mr'}, inplace=True)
            if end_year >= 2018:
                dframe_2018 = pd.read_excel(r'Z:\Data\Summit_GC\Summit_GC_2018\NMHC_results\BA_STD_2018.xlsx',
                                            header=None, sheet_name='BA 2018 data')
                dframe_2018.set_index(0, inplace=True)
                dframe_transposed = dframe_2018.T
                dframe_2018 = dframe_transposed.loc[:, [compound]]
                dframe_2018 = dframe_2018.iloc[:, [j for j, c in enumerate(dframe_2018.columns) if j not in [0, 2, 3]]]

                dframe_2018['file'] = dframe_transposed.iloc[:, 0]
                dframe_2018['decimal_date'] = dframe_transposed.iloc[:, 39]
                dframe_2018.dropna(inplace=True, subset=['file'])
                dframe_2018['decmial_date_year'] = [(2018 + (float(row[0]) - 1) / 365) for row in
                                                    dframe_2018[['decimal_date']].values]
                dframe_2018['Year'] = dframe_2018['file'].apply(lambda x: int(str(x)[0:4]))
                dframe_2018['Yearly_Day'] = dframe_2018['file'].apply(lambda x: int(str(x)[4:7]))
                dframe_2018['Hour'] = dframe_2018['file'].apply(lambda x: int(str(x)[7:9]))
                dframe_2018['Minute'] = dframe_2018['file'].apply(lambda x: int(str(x)[9:11]))
                dframe_2018['Second'] = dframe_2018['file'].apply(lambda x: int(str(x)[11:13]))

                base_date = datetime(year=2018, month=1, day=1)
                dframe_2018['date'] = [
                    base_date + timedelta(days=int(row[0] - 1), hours=int(row[1]), minutes=int(row[2]),
                                          seconds=int(row[3])
                                          ) for row in
                    dframe_2018[[
                        'Yearly_Day',
                        'Hour',
                        'Minute',
                        'Second']].values]

                dframe_2018.drop(columns=['file', 'Year', 'Hour', 'Minute', 'Yearly_Day', 'Second'], inplace=True)
                dframe_2018 = dframe_2018[dframe_2018['date'] < end_date]
                dframe_2018 = dframe_2018[dframe_2018['date'] > start_date]
                dframe_2018.set_index('date', inplace=True)
                dframe_2018.rename(columns={compound: f'{compound}_mr'}, inplace=True)
                if end_year >= 2019:
                    dframe_2019 = pd.read_excel(r'Z:\Data\Summit_GC\Summit_GC_2019\NMHC_results\BA_STD_2019.xlsx',
                                            header=None, sheet_name='BA 2019 data')
                    dframe_2019.set_index(0, inplace=True)
                    dframe_transposed = dframe_2019.T
                    dframe_2019 = dframe_transposed.loc[:, [compound]]
                    dframe_2019 = dframe_2019.iloc[:,
                                  [j for j, c in enumerate(dframe_2019.columns) if j not in [0, 2, 3]]]

                    dframe_2019['file'] = dframe_transposed.iloc[:, 0]
                    dframe_2019['decimal_date'] = dframe_transposed.iloc[:, 39]
                    dframe_2019.dropna(inplace=True, subset=['file'])
                    dframe_2019['decmial_date_year'] = [(2019 + (float(row[0]) - 1) / 365) for row in
                                                        dframe_2019[['decimal_date']].values]
                    dframe_2019['Year'] = dframe_2019['file'].apply(lambda x: int(str(x)[0:4]))
                    dframe_2019['Yearly_Day'] = dframe_2019['file'].apply(lambda x: int(str(x)[4:7]))
                    dframe_2019['Hour'] = dframe_2019['file'].apply(lambda x: int(str(x)[7:9]))
                    dframe_2019['Minute'] = dframe_2019['file'].apply(lambda x: int(str(x)[9:11]))
                    dframe_2019['Second'] = dframe_2019['file'].apply(lambda x: int(str(x)[11:13]))

                    base_date = datetime(year=2019, month=1, day=1)
                    dframe_2019['date'] = [
                        base_date + timedelta(days=int(row[0] - 1), hours=int(row[1]), minutes=int(row[2]),
                                              seconds=int(row[3])
                                              ) for row in
                        dframe_2019[[
                            'Yearly_Day',
                            'Hour',
                            'Minute',
                            'Second']].values]

                    dframe_2019.drop(columns=['file', 'Year', 'Hour', 'Minute', 'Yearly_Day', 'Second'], inplace=True)
                    dframe_2019 = dframe_2019[dframe_2019['date'] < end_date]
                    dframe_2019 = dframe_2019[dframe_2019['date'] > start_date]
                    dframe_2019.set_index('date', inplace=True)
                    dframe_2019.rename(columns={compound: f'{compound}_mr'}, inplace=True)
    elif start_year == 2018:
        if end_year >= 2018:
            dframe_2018 = pd.read_excel(r'Z:\Data\Summit_GC\Summit_GC_2018\NMHC_results\BA_STD_2018.xlsx',
                                            header=None, sheet_name='BA 2018 data')
            dframe_2018.set_index(0, inplace=True)
            dframe_transposed = dframe_2018.T
            dframe_2018 = dframe_transposed.loc[:, [compound]]
            dframe_2018 = dframe_2018.iloc[:, [j for j, c in enumerate(dframe_2018.columns) if j not in [0, 2, 3]]]

            dframe_2018['file'] = dframe_transposed.iloc[:, 0]
            dframe_2018['decimal_date'] = dframe_transposed.iloc[:, 39]
            dframe_2018.dropna(inplace=True, subset=['file'])
            dframe_2018['decmial_date_year'] = [(2018 + (float(row[0]) - 1) / 365) for row in
                                                dframe_2018[['decimal_date']].values]
            dframe_2018['Year'] = dframe_2018['file'].apply(lambda x: int(str(x)[0:4]))
            dframe_2018['Yearly_Day'] = dframe_2018['file'].apply(lambda x: int(str(x)[4:7]))
            dframe_2018['Hour'] = dframe_2018['file'].apply(lambda x: int(str(x)[7:9]))
            dframe_2018['Minute'] = dframe_2018['file'].apply(lambda x: int(str(x)[9:11]))
            dframe_2018['Second'] = dframe_2018['file'].apply(lambda x: int(str(x)[11:13]))

            base_date = datetime(year=2018, month=1, day=1)
            dframe_2018['date'] = [base_date + timedelta(days=int(row[0] - 1), hours=int(row[1]), minutes=int(row[2]),
                                                         seconds=int(row[3])
                                                         ) for row in
                                   dframe_2018[[
                                       'Yearly_Day',
                                       'Hour',
                                       'Minute',
                                       'Second']].values]

            dframe_2018.drop(columns=['file', 'Year', 'Hour', 'Minute', 'Yearly_Day', 'Second'], inplace=True)
            dframe_2018 = dframe_2018[dframe_2018['date'] < end_date]
            dframe_2018 = dframe_2018[dframe_2018['date'] > start_date]
            dframe_2018.set_index('date', inplace=True)
            dframe_2018.rename(columns={compound: f'{compound}_mr'}, inplace=True)
            if end_year >= 2019:
                dframe_2019 = pd.read_excel(r'Z:\Data\Summit_GC\Summit_GC_2019\NMHC_results\BA_STD_2019.xlsx',
                                            header=None, sheet_name='BA 2019 data')
                dframe_2019.set_index(0, inplace=True)
                dframe_transposed = dframe_2019.T
                dframe_2019 = dframe_transposed.loc[:, [compound]]
                dframe_2019 = dframe_2019.iloc[:, [j for j, c in enumerate(dframe_2019.columns) if j not in [0, 2, 3]]]

                dframe_2019['file'] = dframe_transposed.iloc[:, 0]
                dframe_2019['decimal_date'] = dframe_transposed.iloc[:, 39]
                dframe_2019.dropna(inplace=True, subset=['file'])
                dframe_2019['decmial_date_year'] = [(2019 + (float(row[0]) - 1) / 365) for row in
                                                    dframe_2019[['decimal_date']].values]
                dframe_2019['Year'] = dframe_2019['file'].apply(lambda x: int(str(x)[0:4]))
                dframe_2019['Yearly_Day'] = dframe_2019['file'].apply(lambda x: int(str(x)[4:7]))
                dframe_2019['Hour'] = dframe_2019['file'].apply(lambda x: int(str(x)[7:9]))
                dframe_2019['Minute'] = dframe_2019['file'].apply(lambda x: int(str(x)[9:11]))
                dframe_2019['Second'] = dframe_2019['file'].apply(lambda x: int(str(x)[11:13]))

                base_date = datetime(year=2019, month=1, day=1)
                dframe_2019['date'] = [
                    base_date + timedelta(days=int(row[0] - 1), hours=int(row[1]), minutes=int(row[2]),
                                          seconds=int(row[3])
                                          ) for row in
                    dframe_2019[[
                        'Yearly_Day',
                        'Hour',
                        'Minute',
                        'Second']].values]

                dframe_2019.drop(columns=['file', 'Year', 'Hour', 'Minute', 'Yearly_Day', 'Second'], inplace=True)
                dframe_2019 = dframe_2019[dframe_2019['date'] < end_date]
                dframe_2019 = dframe_2019[dframe_2019['date'] > start_date]
                dframe_2019.set_index('date', inplace=True)
                dframe_2019.rename(columns={compound: f'{compound}_mr'}, inplace=True)
    elif start_year == 2019:
        dframe_2019 = pd.read_excel(r'Z:\Data\Summit_GC\Summit_GC_2019\NMHC_results\BA_STD_2019.xlsx',
                                            header=None, sheet_name='BA 2019 data')
        dframe_2019.set_index(0, inplace=True)
        dframe_transposed = dframe_2019.T
        dframe_2019 = dframe_transposed.loc[:, [compound]]
        dframe_2019 = dframe_2019.iloc[:, [j for j, c in enumerate(dframe_2019.columns) if j not in [0, 2, 3]]]

        dframe_2019['file'] = dframe_transposed.iloc[:, 0]
        dframe_2019['decimal_date'] = dframe_transposed.iloc[:, 39]
        dframe_2019.dropna(inplace=True, subset=['file'])
        dframe_2019['decmial_date_year'] = [(2019 + (float(row[0]) - 1) / 365) for row in
                                            dframe_2019[['decimal_date']].values]
        dframe_2019['Year'] = dframe_2019['file'].apply(lambda x: int(str(x)[0:4]))
        dframe_2019['Yearly_Day'] = dframe_2019['file'].apply(lambda x: int(str(x)[4:7]))
        dframe_2019['Hour'] = dframe_2019['file'].apply(lambda x: int(str(x)[7:9]))
        dframe_2019['Minute'] = dframe_2019['file'].apply(lambda x: int(str(x)[9:11]))
        dframe_2019['Second'] = dframe_2019['file'].apply(lambda x: int(str(x)[11:13]))

        base_date = datetime(year=2019, month=1, day=1)
        dframe_2019['date'] = [base_date + timedelta(days=int(row[0] - 1), hours=int(row[1]), minutes=int(row[2]),
                                                     seconds=int(row[3])
                                                     ) for row in
                               dframe_2019[[
                                   'Yearly_Day',
                                   'Hour',
                                   'Minute',
                                   'Second']].values]

        dframe_2019.drop(columns=['file', 'Year', 'Hour', 'Minute', 'Yearly_Day', 'Second'], inplace=True)
        dframe_2019 = dframe_2019[dframe_2019['date'] < end_date]
        dframe_2019 = dframe_2019[dframe_2019['date'] > start_date]
        dframe_2019.set_index('date', inplace=True)
        dframe_2019.rename(columns={compound: f'{compound}_mr'}, inplace=True)

    dframe = pd.concat([dframe_2017, dframe_2018, dframe_2019])
    dframe = dframe.loc[dframe.index < end_date]
    dframe = dframe.loc[dframe.index > start_date]
    dframe.fillna(value=99999, inplace=True)

    return dframe

if __name__ == '__main__':
    print(excel_rf_BA('propane','2017-6-1 0:0:0', '2019-6-30 0:0:0'))





