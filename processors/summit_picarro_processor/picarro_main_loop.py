import os, asyncio

"""
Things Needed:

File checking:
	Needs to:
		Check for new or updated files
		Read new files
		
Calibration Handling:
	Needs to:
		Search all data, then new data
		Pick out all calibration data
		Separate into low, middle, high standards based on valves
			Group in a single calibration event and calculate some things based on the regression, etc
		Potentially apply information from the above on data
		
Plotting:
	Needs to:
		Plot data if new data is available
		

"""


async def check_load_new_data():
	pass

