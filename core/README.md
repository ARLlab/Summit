#summit.py

This is the main script for all processing.
The main() functions of individual modules are loaded as whole processors, so VOCs are run all at once, the Picarro is
run all at once, etc.

Log and data files are sync to several folders in the FTP directory, and move_log_files() moves these to Summit/data/...
move_log_files() runs asynchronously, every five minutes, and is scheduled prior to main(), so it works independently.
It checks for and transfers any new files before sleeping for another five minutes.All other processors run sequentially 
(as defined in main()), every 10 minutes. Sleeps are blocked into 30s periods to permit keyboard interrupts and easy restarts of the whole processing sequence.

#summit_core.py

This contains project-wide functions and configurations such as directories and classes used by every processor. Directories are assigned when summit_core is run so that the directories can be references project-wide by importing them from summit_core.



