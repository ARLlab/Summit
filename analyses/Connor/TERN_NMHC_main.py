

try:
    from pathlib import Path
    from TERN_functions import sort_NMHC, convert_asc_to_txt

except ImportError as e:
    print('ImportError occured in TERN_NMHC_main.py')
    print(e.args)


bup_path = Path(r'C:\Users\ARL\Desktop\Summit_GC_2020\BUP\NMHC')
chr_sort_path = Path(r'C:\Users\ARL\Desktop\Summit_GC_2020\NMHC_results\temporary_sort\chr')

print('sorting CHR files into temporary sort')
sort_NMHC(bup_path, chr_sort_path)

user_input = input("Now convert chr files to ASC using PeakSimple. Enter 'ready' when done")
while user_input != "ready":
    user_input = input("please enter 'ready' again. (This is to make sure you don't accidentally start the "
                       "convert script before you are ready")
print('converting .asc to .txt')
for type in ['0', '1', '2', '4', '6']:
    asc_path = Path(rf'C:\Users\ARL\Desktop\Summit_GC_2020\NMHC_results\temporary_sort\ASC\sample_{type}')
    txt_path = Path(rf'C:\Users\ARL\Desktop\Summit_GC_2020\NMHC_results\temporary_sort\text\sample_{type}')

    convert_asc_to_txt(asc_path, txt_path)


print('.asc files have been converted to .txt. You are ready to use TERN.')
