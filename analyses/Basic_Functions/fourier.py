import numpy as np


def fourier(tau, x, *a):
    """
    :param tau: Fourier series variable Tau, depends on data
    :param x: Independent Variable Data (example: timeseries)
    :param a: The required number of cos equations to fit the data
    :return: the full fourier equation
    """

    """
    Example of usage with SciPy 
    numEqs
    params, covar = sp.optimize.curve_fit(fourier, xdata, ydata, p0=([1.0] * numEqs))
    """

    eq = a[0] * np.cos(np.pi / tau * x)                         # general eq
    for deg in range(1, len(a)):                                # depending on length of a, create that many eq's
        eq += a[deg] * np.cos((deg+1) * np.pi / tau * x)        # add them up

    return eq                                                   # return the final fourier eq
