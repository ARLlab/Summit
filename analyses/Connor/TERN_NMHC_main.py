from pathlib import Path

if __name__ == "__main__":
    chrpath = Path(r'C:\Users\ARL\Desktop\Summit_GC_2020\NMHC_results\temporary_sort\chr')
    ascpath = Path(r'C:\Users\ARL\Desktop\Summit_GC_2020\NMHC_results\temporary_sort\ASC')
    txtpath = Path(r'C:\Users\ARL\Desktop\Summit_GC_2020\NMHC_results\temporary_sort\text')

    method = input('If you are integrating continuously from last time, enter "cont" to sort and convert new '
                   'files. If reintegrating past data, enter "reint"')
    if method == "cont":
        # sort all files that are not stored in 'integraded.json'
        # append new integrated files to 'integrated.json'

    sort_NMHC

    sum_asc_file_convert(ascpath, txtpath)

    print('.asc files have been converted to text')
    input('Manually integrate files in TERN, and output .csv file')
