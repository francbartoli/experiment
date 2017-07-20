#!/usr/bin/env python
"""
This script auto-generates a RCM data sample on-disk dataset for testing.

"""
from __future__ import print_function

import numpy as np
import os
import pandas as pd
import xarray as xr

from experiment import Experiment, Case

PATH_TO_DATA = os.path.join(os.path.dirname(__file__), "RCM data")
SEPARATOR = "_"
cases = [
    Case("category", "Climate Data Category", ["Temperature Climate Data"]),
    Case("variable", "Climate Variable", ["Temperature",
                                          "Max Temperature",
                                          "Min Temperature"]),
    Case("period", "Time Period", ["1986-2005",
                                   "2046-2065",
                                   "2081-2100"]),
    Case("scenario", "Climate Scenario", ["RCP4.5",
                                          "RCP8.5"]),
    Case("model", "Climate Model", ["CNRM-CM5",
                                    "EC-EARTH",
                                    "GFDL-ESM2M"]),
    Case("domain", "Domain and Resolution", ["MNA-44"]),
    Case("orgmodel",
         "Climate Model and Organization",
         ["CNRM-CERFACS-CNRM-CM5", "ICHEC-EC-EARTH", "NOAA-GFDL-GFDL-ESM2M"]
         ),
    Case("historical", "Historical Climate Scenario", ["historicalandrcp45",
                                                       "historicalandrcp85"]),
    Case("rcm", "RCM Used", ["r1i1p1_SMHI-RCA4"
                             "r12i1p1_SMHI-RCA4"]),
    Case("correction", "Data Correction Used", ["v1-bc-dbs-wfdei"]),
    Case("frequency", "Frequency Time Unit", ["day"]),

]
exp = Experiment(
    "RCM data", cases, timeseries=True, data_dir=PATH_TO_DATA,
    # Temperature\ Climate\ Data/Temperature\ 1986-2005\ RCP8.5/EC-EARTH
    case_path="{category}/{variable} {period} {scenario}/{model}",
    output_prefix=SEPARATOR + "{domain}" + SEPARATOR + \
                              "{orgmodel}" + SEPARATOR + \
                              "{historical}" + SEPARATOR + \
                              "{rcm}" + SEPARATOR + \
                              "{correction}" + SEPARATOR + \
                              "{frequency}" + SEPARATOR,
    output_suffix=".nc", validate_data=False
)

VARS = ["tas", "tasmin", "tasmax"]
# VARS = ["tas", "tasmin", "tasmax", "pr", ]


# def _build_rcm_output_prefix(**case_kwargs):

#     rcm_prefix = SEPARATOR + "MNA-44" + SEPARATOR
#     for k, v in case_kwargs:
#         print k, v
#     return rcm_prefix
#     pass


def _make_dataset(varname, seed=None, **var_kws):
    rs = np.random.RandomState(seed)

    _dims = {'time': 10, 'x': 5, 'y': 5}
    _dim_keys = ('time', 'x', 'y')

    ds = xr.Dataset()
    ds['time'] = ('time', pd.date_range('2000-01-01', periods=_dims['time']))
    ds['x'] = np.linspace(0, 10, _dims['x'])
    ds['y'] = np.linspace(0, 10, _dims['y'])
    data = rs.normal(size=tuple(_dims[d] for d in _dim_keys))
    ds[varname] = (_dim_keys, data)

    ds.coords['numbers'] = ('time',
                            np.array(range(_dims['time']), dtype='int64'))

    return ds


if __name__ == "__main__":

    root = exp.data_dir

    for path, case_kws in exp._walk_cases(with_kws=True):
        full_path = os.path.join(root, path)
        try:
            os.makedirs(full_path)
        except OSError as e:
            # if e.errno != errno.EEXIST:
            print e
            pass

        # _build_rcm_output_prefix(**case_kws)
        prefix = exp.case_prefix(**case_kws)
        suffix = exp.output_suffix

        for v in VARS:
            fn = v + prefix + suffix
            absolute_filename = os.path.join(full_path, fn)

            print(absolute_filename)
            ds = _make_dataset(v)
            ds.to_netcdf(absolute_filename)

    exp.to_yaml(os.path.join(PATH_TO_DATA, "rcm_data.yaml"))
