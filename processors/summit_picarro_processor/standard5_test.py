# Import libraries & functions
import datetime as dt
import pandas as pd
import matplotlib.pyplot as plt


def main():

    """
    This function creates calibration events with a valve position of 5, and places them in the
    Picarro Database for future analysis.

    :param N/A -- Perhaps logger once incorperated into full code
    :return: boolean, did it run?

    !! Note: To view the plots, comment out line 36 and set a breakpoint directly after plt.show() on line 131
    """

    # Import Required Functions
    from summit_core import connect_to_db
    from summit_picarro import Base, Datum, CalEvent, find_cal_indices
    from summit_picarro import filter_postcal_data
    from matplotlib.pyplot import figure

    # Connect to the database
    rundir = r'C:\Users\ARL\Desktop'                                                    # location of DB
    engine, session = connect_to_db('sqlite:///JASHAN_summit_picarro.sqlite', rundir)   # Create eng & sess
    Base.metadata.create_all(engine)                                                    # Create base

    # Get any data with a valve position of 5
    standard_data = {}
    MPV = 5
    mpv_data = pd.DataFrame(session
                            .query(Datum.id, Datum.date)            # Gets the datum ID & Date
                            .filter(Datum.mpv_position == MPV)      # Filters them for valve pos #5
                            .filter(Datum.cal_id == None)           # only if not already any cal event
                            .all())                                 # actually gathers the data

    mpv_data['date'] = pd.to_datetime(mpv_data['date'])             # Convert to PD datetime version

    mpv_converter = {5: 'ch4_GC_std'}                               # TODO: Incorporate in larger project code
    standard_data[mpv_converter[MPV]] = mpv_data.sort_values(by=['date']).reset_index(drop=True)

    # Create a calc_event with the given name of this standard
    for standard, data in standard_data.items():
        indices = find_cal_indices(data['date'])                    # Gathers the indicies of each new cal event

        cal_events = []                                             # preallocation of cal events
        prev_ind = 0                                                # prev_ind is initially the first index

        # If indicies is empty, but there is still data, create a single event
        if not len(indices) and len(data):
            event_data = (session
                          .query(Datum)
                          .filter(Datum.id.in_(data['id']))
                          .all())
            cal_events.append(CalEvent(event_data, standard))

        # Seperate cal events from gathered indicies and place in cal_events
        for num, ind in enumerate(indices):
            event_data = (session
                          .query(Datum)                                             # Searches through all Datums
                          .filter(Datum.id.in_(data['id'].iloc[prev_ind:ind]))      # Data bewteen index and previous
                          .all())                                                   # actually gathers the data
            cal_events.append(CalEvent(event_data, standard))                       # appends cal events list

            if num == (len(indices) - 1):                                           # last index, gets the rest
                event_data = (session
                              .query(Datum)
                              .filter(Datum.id.in_(data['id'].iloc[ind:]))          # index to end of list
                              .all())
                cal_events.append(CalEvent(event_data, standard))                   # appends cal events list

                # Create a plot of this cal event
                coPlot, co2Plot, ch4Plot, datePlot = [], [], [], []
                for x in event_data:
                    coPlot.append(x.co); co2Plot.append(x.co2); ch4Plot.append(x.ch4);      # Raw Numbers
                    if datePlot == []:
                        timestep = x.date.timestamp()
                        datePlot.append(timestep)
                    else:
                        timestep = x.date.timestamp() - datePlot[0]            # likely a better way
                        datePlot.append(timestep)                                           # to do this

                ev = cal_events[len(indices)]
                for cpd in ['co', 'co2', 'ch4']:
                    time = (ev.date - ev.dates[0]).seconds
                    ev.calc_result(cpd, time)

                datePlot[0] = 0  # start it at 0

                figure(1)
                table_vals = [[list(ev.co_result.values())[0]], [list(ev.co_result.values())[1]],
                              [list(ev.co_result.values())[2]]]
                the_table = plt.table(cellText=table_vals, cellColours=None,
                                      cellLoc='right', colWidths=[0.3]*3,
                                      rowLabels=['mean', 'median', 'stdev'], rowColours=None, rowLoc='left',
                                      colLabels=['value'], colColours=None, colLoc='center',
                                      loc='lower right', bbox=None)
                plt.plot(datePlot, coPlot, label='co')
                plt.xlabel('Time since start of cal_event [seconds]')
                plt.ylabel('Compounds')
                plt.title('CO')

                figure(2)
                table_vals = [[list(ev.co2_result.values())[0]], [list(ev.co2_result.values())[1]],
                              [list(ev.co2_result.values())[2]]]
                the_table = plt.table(cellText=table_vals, cellColours=None,
                                      cellLoc='right', colWidths=[0.3] * 3,
                                      rowLabels=['mean', 'median', 'stdev'], rowColours=None, rowLoc='left',
                                      colLabels=['value'], colColours=None, colLoc='center',
                                      loc='upper right', bbox=None)
                plt.plot(datePlot, co2Plot, label='co2')
                plt.xlabel('Time since start of cal_event [seconds]')
                plt.ylabel('Compounds')
                plt.title('CO2')

                figure(3)
                table_vals = [[list(ev.ch4_result.values())[0]], [list(ev.ch4_result.values())[1]],
                              [list(ev.ch4_result.values())[2]]]
                the_table = plt.table(cellText=table_vals, cellColours=None,
                                      cellLoc='right', colWidths=[0.3] * 3,
                                      rowLabels=['mean', 'median', 'stdev'], rowColours=None, rowLoc='left',
                                      colLabels=['value'], colColours=None, colLoc='center',
                                      loc='upper right', bbox=None)
                plt.plot(datePlot, ch4Plot, label='ch4')
                plt.xlabel('Time since start of cal_event [seconds]')
                plt.ylabel('Compounds')
                plt.title('ch4')

                plt.show()

            prev_ind = ind                                                          # set previous index as current

        # Calculate the CO, CO2, and Methane results with Brendan's functions
        for ev in cal_events:
            filter_postcal_data(ev, session)                            # filter following min of ambient data

            if ev.date - ev.dates[0] < dt.timedelta(seconds=90):        # events under 90 seconds are dumped
                ev.standard_used = 'dump'                               # assign dump name
                session.merge(ev)                                       # merge results with session
            # otherwise, iterate over each compound and calculate results
            else:
                for cpd in ['co', 'co2', 'ch4']:
                    time = 21
                    ev.calc_result(cpd, time)                           # results are calced (time) seconds back

                session.merge(ev)                                       # merge results with session

        # Save to your local copy of the database & check results
        session.commit()                                                # commit results to session

    # Create a timeseries of the results to ascertain what portion of the data we want to keep

    # Integrate with Brendan's code once tested for errors

    return True


if __name__ == '__main__':
    main()