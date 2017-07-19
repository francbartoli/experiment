#!/usr/bin/env python
"""
This script auto-generates a sample on-disk dataset for testing.

"""

import numpy as np
import os
import pandas as pd
import xarray as xr

from experiment import Experiment, Case

PATH_TO_DATA = os.path.join(os.path.dirname(__file__), "RCM data")
cases = [
    Case("param1", "Parameter 1", ["Temperature Climate Data"]),
    Case("param2", "Parameter 2", ["Temperature",
                                   "Max Temperature",
                                   "Min Temperature"]),
    Case("param3", "Parameter 3", ["1986-2005", "2046-2065", "2081-2100"]),
    Case("param4", "Parameter 4", ["RCP4.5", "RCP8.5"]),
    Case("param5", "Parameter 5", ["CNRM-CM5", "EC-EARTH", "GFDL-ESM2M"]),
]
exp = Experiment(
    "RCM data", cases, timeseries=True, data_dir=PATH_TO_DATA,
    # Temperature\ Climate\ Data/Temperature\ 1986-2005\ RCP8.5/EC-EARTH
    case_path="{param1}/{param2} {param3} {param4}/{param5}",
    output_prefix="{param1}.{param2}.{param3}.{param4}-{param5}",
    output_suffix=".nc", validate_data=False
)

VARS = ["tas", "tasmin", "tasmax"]


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

        prefix = exp.case_prefix(**case_kws)
        suffix = exp.output_suffix

        for v in VARS:
            fn = v + prefix + suffix
            absolute_filename = os.path.join(full_path, fn)

            print(absolute_filename)
            ds = _make_dataset(v)
            ds.to_netcdf(absolute_filename)

    exp.to_yaml(os.path.join(root, "rcm_data.yaml"))
