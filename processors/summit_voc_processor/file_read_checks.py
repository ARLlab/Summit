import os

### DB CONNECTION ### - Working
homedir = r'C:\Users\arl\Desktop\Summit Processors\Summit VOC Processor'
os.chdir(homedir)
from summit_voc import connect_to_summit_db

engine, session, Base = connect_to_summit_db('sqlite:///summit_vocs.sqlite', homedir)
Base.metadata.create_all(engine)

### LOG TESTING ### - Working
os.chdir(r'C:\Users\arl\Desktop\Summit Processors\Summit VOC Processor\logs')
fn = '2019032081008l.txt'
from summit_voc import read_log_file

lf = read_log_file(fn)
from summit_voc import LogFile

newlog = LogFile(lf)

### CRF TESTING ### - Working
from summit_voc import read_crf_data

os.chdir(r'C:\Users\arl\Desktop\Summit Processors\Summit VOC Processor')
fn = 'summit_CRFs.txt'
rf_list = read_crf_data(fn)

### VOC LOG TESTING ### - Working
from summit_voc import read_pa_line

os.chdir(r'C:\Users\arl\Desktop\Summit Processors\Summit VOC Processor')
new_lines = []

pa_file = open('VOC.LOG').readlines()

for line in pa_file:
    new_lines.append(read_pa_line(line))
