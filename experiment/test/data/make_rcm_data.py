#!/usr/bin/env python
"""
This script auto-generates a RCM data sample on-disk dataset for testing.

"""
from __future__ import print_function
from collections import namedtuple

import numpy as np
import os
import pandas as pd
import xarray as xr
import arrow as rrow

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
    Case("domain", "Domain and Resolution", ["MNA-44", "MNA-22"]),
    Case("orgmodel",
         "Climate Model and Organization",
         ["CNRM-CERFACS-CNRM-CM5", "ICHEC-EC-EARTH", "NOAA-GFDL-GFDL-ESM2M"]
         ),
    Case("historical", "Historical Climate Scenario", ["historicalandrcp45",
                                                       "historicalandrcp85"]),
    Case("rcm", "RCM Used", ["r1i1p1_SMHI-RCA4",
                             "r12i1p1_SMHI-RCA4"]),
    Case("correction", "Data Correction Used", ["v1-bc-dbs-wfdei"]),
    Case("frequency", "Frequency Time Unit", ["day"]),
    Case("fixedperiod", "Fixed period of entire Dataset", ["19510101-21001231"])
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
                              "{frequency}" + SEPARATOR + \
                              "{fixedperiod}" + SEPARATOR,
    output_suffix=".nc", validate_data=False
)

VARS = ["tas", "tasmin", "tasmax"]
# VARS = ["tas", "tasmin", "tasmax", "pr", ]


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


def _unmatched_args(**kwargs):
    if ((kwargs['historical'].replace(
            'historicaland', '') == kwargs['scenario'].lower().replace(
            '.', '')) and (kwargs['model'] in kwargs['orgmodel'])):
        return False
    return True


def _build_timerange(period):
    try:
        yran = period.split('-')
    except IndexError:
        print("Damn! No - symbols exist!")
    startdate = rrow.Arrow(int(yran[0]), 1, 1)
    enddate = rrow.Arrow(int(yran[1]), 12, 31)
    Timerange = namedtuple('Timerange', 'start end range')
    timerange = Timerange(startdate,
                          enddate,
                          rrow.Arrow.span_range('year',
                                                startdate,
                                                enddate))
    return timerange


if __name__ == "__main__":

    root = exp.data_dir

    for path, case_kws in exp._walk_cases(with_kws=True):
        full_path = os.path.join(root, path)
        try:
            os.makedirs(full_path)
        except OSError as e:
            # if e.errno != errno.EEXIST:
            print(e)
            pass

        # skip if scenario and historical cases don't match
        if _unmatched_args(**case_kws):
            continue
        if not case_kws['fixedperiod']:
            prefix = exp.case_prefix(**case_kws) + _build_timerange(
                case_kws['period']).start.format('YYYYMMDD') + \
                "-" + _build_timerange(
                case_kws['period']).end.format('YYYYMMDD')
        prefix = exp.case_prefix(**case_kws)
        # print(_build_timerange(case_kws['period']).start.format('YYYYMMDD'))
        suffix = exp.output_suffix

        for v in VARS:
            for r in _build_timerange(case_kws['period']).range:
                fn = v + prefix + \
                    r[1].ceil('year').format('YYYY') + SEPARATOR + \
                    r[1].ceil('year').format('YYYY') + suffix
                absolute_filename = os.path.join(full_path, fn)

                print(absolute_filename)
                ds = _make_dataset(v)
                ds.to_netcdf(absolute_filename)

    exp.to_yaml(os.path.join(PATH_TO_DATA, "rcm_data.yaml"))
