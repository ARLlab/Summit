def sort_NMHC(loaddir, outputdir):
    """Sorts .chr files from the BUP folder into sorting folders by the sample type."""

    start_doy = 0
    end_doy = 365
    today = datetime.datetime.today()
    current_year = int(today.year)

    try:
        assert (loaddir.is_dir())
        assert (outputdir.is_dir())
        """fix these two for loops to only include new chr files"""
        new_files = []
        for f in loaddir.glob('**/*.txt'):  # recursively searches through all folders. Will not be necessary once the
            if f.is_file():  # matlab NMHC sort script is discontinued
                new_files.append(f.name)

        current_file_names = []
        for f in outputdir.glob('**/*.chr'):
            if f.is_file():
                current_file_names.append(f.name)


    except Exception as e:
        print(f'{e.args}')

    for f in new_files:
        try:
            name = f.name
            if int(name[4:7]) <= end_doy and int(name[4:7]) >= start_doy and int(
                    name[0:4]) == current_year:  # redundant?
                if name not in current_file_names:
                    dframe = pd.read_csv(f, delimiter='\t', header=None, index_col=0)
                    sample_type = dframe.iloc[2, 0]
                    sample_type = int(sample_type)

                    chrom_file_name = name[:-5] + "c" + ".chr"  # a bit weird, this needs to be changed at some point
                    chr_file = Path(rf"{loaddir}\{chrom_file_name}")

                    try:
                        src = chr_file
                        dst = fr"{outputdir}\sample_{str(sample_type)}"
                        shutil.copy(src, dst)
                    except FileNotFoundError:
                        print(f"the file {src} could not be found")

        except ValueError:
            continue

def convert_asc_to_txt(loaddir, outputdir):
    """
    This function converts a PeakSimple .ASC (ASCII) file from a typical chromatagram into a tab delimited text file
    that can be read by TERN integrating software

    Note: I've decided not to pass logger to these functions because they are only used during a manual process by the
    user, so any error can be handled on the spot!

    :param loaddir: the directory where the .asc files are kept
    :param outputdir: the directory where the .txt files should be output to
    :return: Boolean: did the function succeed
    """
    # try to load some libraries
    try:
        import numpy as np
        import pandas as pd
        from datetime import datetime, timedelta

    except ImportError as e:
        print('ImportError occured in convert_asc_to_txt() during manual integration')

    # try to load files, otherwise fail
    try:
        # verify that both directory paths are actually directories
        assert(loaddir.is_dir())
        assert(outputdir.is_dir())

        # for simplicity and because I can't figure out how to make Peak Simple do this, delete all unnecessary .chr
        # and .THU (.chr files are backed up and .THU are useless image compressions of the data)

        for f in loaddir.iterdir():
            if f.is_file():
                if f.suffix in ['.chr', '.THU']:
                    f.unlink()

        # load the files
        files = []
        for f in loaddir.iterdir():         # loop over files in dir
            if f.is_file():                 # verify it's a file
                if f.suffix in '.ASC':      # verify the ending is .asc
                    files.append(f)         # append to files

        # loop to select only new ASC files for converting
        existing_files = []
        for f in outputdir.iterdir():
            if f.is_file():
                if f.suffix in '.ASC':
                    existing_files.append(f)

    except Exception as e:
        print(f'Error {e.args} prevented loading of datafiles in convert_asc_to_txt')

    # try to convert files, otherwise fail
    for f in files:
        if f not in existing_files:   # select only new files
            try:
                dframe = pd.read_csv(f, names=['mq', 'na'])                         # load dataset
                dframe = dframe[~dframe['mq'].isin(['IPOINT=1', 'IPOINT=2'])]       # remove IPOINT rows

                # create the date
                filename = f.name
                year = int(filename[0:4])
                doy = int(filename[4:7])
                hour = int(filename[7:9])
                minute = int(filename[9:11])
                second = int(filename[11:13])
                base_date = datetime(year=year, month=1, day=1)
                date_time = base_date + timedelta(days=(doy - 1), hours=hour, minutes=minute,
                                                  seconds=second)
                date_time = datetime.strftime(date_time, '%m/%d/%Y %H:%M:%S')

                # a few clean up items
                dframe.dropna(axis=0, how='any', inplace=True)
                dframe = dframe.astype('int64')
                dframe.reset_index(drop=True, inplace=True)

                # format into TERN specified column structure
                tern_df = pd.DataFrame(columns=['retention', 'm/Q = 1'])
                tern_df['m/Q = 1'] = dframe['mq'] / 1000
                tern_df['retention'] = tern_df.index / 10
                tern_df.insert(0, 'date', date_time, allow_duplicates=True)


                # output to .csv
                tern_df.to_csv(rf'{outputdir}\{f.name.split(".")[0]}.txt', index=False)

            except Exception as e:
                exc_type, exc_obj, exc_tb = sys.exc_info()
                fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                print(f'Error {e.args} prevented file {f.name} from being converted')
                print(exc_type, fname, exc_tb.tb_lineno)

    # for the purposes of SUMMIT (experiments that will continually grow as the year progresses), we only want to
    # check if the number of ASC files is the same as the number of txt file. DO NOT DELETE OLD ASC FILES.
    try:
        # verify that both directory paths are actually directories

        # load the files
        checksum = len(files)
        checkfile = []
        for f in outputdir.iterdir():       # loop over files in dir
            if f.is_file():                 # verify it's a file
                if f.suffix in '.txt':      # verify the ending is .asc
                    checkfile.append(f)     # append to files
                    print(f'Text File {f} Created')

        if checksum != len(checkfile):
            print('WARNING: Number of saved text files is NOT the same as the number of ASC files')
            assert(checksum == len(checkfile))

        # # removed (4/18/20) because saving the ASC files saves hours of work in case of small file mistakes and
        # experimentation with this script
        # if checksum == len(checkfile):
        #     for f in loaddir.iterdir():     # loop over files in dir
        #         if f.is_file():             # verify it's a file
        #             #f.unlink()               remove the file
        #             pass                    #removed this line for testing

    except Exception as e:
        print(f'Error {e.args} prevented deleting of old files in convert_asc_to_txt')

if __name__ == "__main__":
    from pathlib import Path

    for type in ['0', '1', '2', '4', '6']:
        ascpath = Path(rf'C:\Users\ARL\Desktop\Summit_GC_2020\NMHC_results\temporary_sort\ASC\sample_{type}')
        txtpath = Path(rf'C:\Users\ARL\Desktop\Summit_GC_2020\NMHC_results\temporary_sort\text\sample_{type}')

        convert_asc_to_txt(ascpath, txtpath)

