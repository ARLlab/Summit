# Import libraries & functions
import datetime as dt
import pandas as pd


def main():
    """
    This function creates calibration events with a valve position of 5, and places them in the
    Picarro Database for future analysis.

    :param N/A -- Perhaps logger once incorperated into full code
    :return: boolean, did it run?
    """

    # Import Required Functions
    from summit_core import connect_to_db
    from summit_picarro import Base, Datum, CalEvent, find_cal_indices
    from summit_picarro import filter_postcal_data

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

            if num == len(indices):                                                 # last index, gets the rest
                event_data = (session
                              .query(Datum)
                              .filter(Datum.id.in_(data['id'].iloc[ind:]))          # index to end of list
                              .all())
                cal_events.append(CalEvent(event_data, standard))                   # appends cal events list

            prev_ind = ind                                                          # set previous index as current

        # Calculate the CO, CO2, and Methane results with Brendan's functions
        for ev in cal_events:
            # TODO: Is this filtering actually necesarry - likely yes
            filter_postcal_data(ev, session)                            # filter following min of ambient data

            if ev.date - ev.dates[0] < dt.timedelta(seconds=90):        # events under 90 seconds are dumped
                ev.standard_used = 'dump'                               # assign dump name
                session.merge(ev)                                       # merge results with session
            # otherwise, iterate over each compound and calculate results
            else:
                for cpd in ['co', 'co2', 'ch4']:
                    time = 21                                           # TODO: Should time intervals be different?
                    ev.calc_result(cpd, time)                           # results are calced (time) seconds back

                session.merge(ev)                                       # merge results with session

        # Save to your local copy of the database & check results
        session.commit()                                                # commit results to session

    # Create a timeseries of the results to ascertain what portion of the data we want to keep

    # Integrate with Brendan's code once tested for errors

    return True


if __name__ == '__main__':
    main()