# Summit

GEOSummit is a NSF-funded research station near the summit of the Greenland ice sheet, with many instruments from different labs. These analyses and realtime processing concern the Atmospheric Research Lab's ongoing project at GEOSummit for the measurement of trace gases (CO, CO2, Methane [CH4], and volatile organic chemicals [VOCs]). 

## /core
The core directory contains the project-as-a-whole components, like common functions for working with data from all or multiple instruments, S/FTP functionality, and the main script that is used at runtime. 
  #### summit.py
  This is _the_ runtime script. It imports individual processors, error handling, and manages the incoming data transfers. 
  #### summit_core.py
  This is the core module that contains project-wide functions like file transfers, directory setup, and project-wide configuration.


## /processors
Sorted by instrument and incoming data, the processors are the modules that run more or less indepently of the whole. They import directory information and common functions from summit_core.py, as well as error handling from processors/errors.

Individual processors are kept in their respective folders, and consist of a `"summit_[dataType].py"` file, and `"summit_[dataType]_main_loop.py"` file. The plain files contain all the functions used in that specific processor, which are imported by the main loop. The main loops contain the asynchronous runtime functions that do the actual work of loading, processing and plotting data, using functions imported from the plain files.

## /analyses
A bulk folder containing separate one-off or repeatable analyses that are not part of the runtime code.

## /website
Source files for the website which hosts the plots and information created by the processors.
