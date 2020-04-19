

def sort_NMHC(loaddir, outputdir):
    """Sorts .chr files from the BUP folder into sorting folders by the sample type."""

    start_doy = 0
    end_doy = 365
    today = datetime.datetime.today()
    current_year = int(today.year)



    try:
        assert(loaddir.is_dir())
        assert(outputdir.is_dir())
        """fix these two for loops to only include new chr files"""
        new_files = []
        for f in loaddir.glob('**/*.txt'):   # recursively searches through all folders. Will not be necessary once the
            if f.is_file():                  # matlab NMHC sort script is discontinued
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
            if int(name[4:7]) <= end_doy and int(name[4:7]) >= start_doy and int(name[0:4]) == current_year:   # redundant?
                if name not in current_file_names:
                    dframe = pd.read_csv(f, delimiter='\t', header=None, index_col=0)
                    sample_type = dframe.iloc[2,0]
                    sample_type = int(sample_type)

                    chrom_file_name = name[:-5] + "c" + ".chr"     # a bit weird, this needs to be changed at some point
                    chr_file = Path(rf"{loaddir}\{chrom_file_name}")

                    try:
                        src = chr_file
                        dst = fr"{outputdir}\sample_{str(sample_type)}"
                        shutil.copy(src, dst)
                    except FileNotFoundError:
                        print(f"the file {src} could not be found")

        except ValueError:
            continue

if __name__ == '__main__':
    import pandas as pd
    import datetime
    import shutil
    from pathlib import Path

    loaddir = Path(r"C:\Users\ARL\Desktop\Summit_GC_2020\BUP\NMHC")
    outputdir = Path(r"C:\Users\ARL\Desktop\Summit_GC_2020\NMHC_results\temporary_sort\chr")
