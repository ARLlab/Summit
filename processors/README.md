## Per-Instrument Data Processors

## /methane

##### Processing for methane data from the GC-FID
Methane measurements are made roughly every two hours and record separate files on two computers that contain data necessary for quantifying the samples. 
##### summit_methane.py
Classes and functions that are specific to the methane data processing.
##### methane_main_loop.py
Async functions that can be loaded in and ran elsewhere as part of a full runtime for the system.

## /voc
##### Processing for the VOC data from the GC-FID
VOC measurements are taken roughly every two hours and record separate files on the same two computers as the methane processor, but in different formats.
##### summit_voc.py
Classes and functions that are specific to the methane data processing.
##### summit_daily.py
Classes and functions for processing the 30-minute logged parameter values for quality control and system monitoring. Data include overall room and sample inlet temperatures as well as instrument values relevant to only the methane and voc GC-FID processing.
##### voc_main_loop.py
Async functions that can be loaded in and ran elsewhere as part of a full runtime for the system. Includes the daily file processing.

## /picarro
##### Processing for the Picarro CO, CO2, and methane data
The Picarro runs continously, producing 5s averaged data that is written to a file continously. These are further averaged in post-processing before being posted. Calibration data is also tracked.
##### summit_picarro.py
Classes and functions for processing the Picarro data, including tracking for calibrations and tests.
##### picarro_main_loop.py
Async functions that can be imported and run elsewhere as part of the full runtime.
