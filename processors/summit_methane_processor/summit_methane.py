"""
Methane injections are tricky. A 65-minute run contains 8 samples and 2 standards in the order:

sample x 2
std
sample x 4
std
sample x 2

PeakSimple records the start time of the run, which is ~3.5 minutes after the hour it begins on. From here,
the retention time of the peak is roughly 1 minute after the retention time of the peak. These number are subject to
change, but the rough formula will be:

sample_time = run_start_time + retention_time_in_minutes - 1_minute (sample concentration/equilibration before inject)

"""

class Peak():
	pass


class GcRun():
	"""
	A GcRun will contain all the peaks from that chromatogram, or line in the PeakSimple log. Usually, 8xsample, 2xstd.

	Peaks less than PA == 2, should be tossed (?)

	Standards within a run should have a stdev between the two, as well as some QC surrounding that.
	Samples should also have a run median and stdev that can be used for QC.

	If one standard in a run is poor, it should also re-quantify all samples from that run with the 'good' standard.


	"""
	pass





