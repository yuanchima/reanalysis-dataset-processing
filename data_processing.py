import os
import datetime 
import numpy as np 
import pandas as pd
from netCDF4 import Dataset


def load_meta_data(data_root='data'):
    file_name = os.listdir(data_root)[3]
    dst = Dataset(os.path.join(data_root, file_name), mode='r', format="NETCDF4")
    # all_vars_info = list(dst.variables.items())
    all_vars_name = list(dst.variables.keys())
    all_vars_long_name = [dst.variables[key].long_name for key in all_vars_name]
    all_vars_units = [dst.variables[key].units for key in all_vars_name]
    all_vars_shape = [dst.variables[key].shape for key in all_vars_name]

    df_vars_info = pd.DataFrame(all_vars_name,columns = ['name'])
    df_vars_info['long_name'] = all_vars_long_name
    df_vars_info['units'] = all_vars_units
    df_vars_info['shape'] = all_vars_shape

    time = dst.variables['time'][:].data
    lon = dst.variables['lon'][:].data
    lat = dst.variables['lat'][:].data
    # lev = dst.variables['lev'][:].data

    return df_vars_info, time, lon, lat

def load_data(vars_name, data_root='data'):
    lst = list()
    for file_name in os.listdir(data_root):
        try:
            dst = Dataset(os.path.join(data_root, file_name), mode='r', format="NETCDF4")
        except:
            continue
        time = dst.variables['time'][:].data
        since = dst.variables['time'].units[-19:]
        since = datetime.datetime.strptime(since, r'%Y-%m-%d %H:%M:%S')
        time = [datetime.timedelta(minutes=int(t)) for t in time]
        dt = [since + t for t in time]
        
        df = pd.DataFrame(dt, columns=['time'])
        df['lon'] = dst.variables['lon'][:].data[0]
        df['lat'] = dst.variables['lat'][:].data[0]
        for key in vars_name:
            df[key] = dst.variables[key][:].data[:, 0, 0]

        lst.append(df)
    df = pd.concat(lst)
    return df

if  __name__ == "__main__":
    df_vars_info, time, lon, lat= load_meta_data()
    print(df_vars_info)
    print(f'time: {time}')
    vars_name = ['PS', 'TS', 'U10M', 'V10M', 'U50M', 'V50M', 'T2M', 'T10M', 'U850', 'V850', 'T850']
    # surface_pressure,surface_skin_temperature,u_10,v_10,u_50,v_50,temp_2m,temp_10m,u_850,v_850,temp_850,ws_50m,dens_50m

    df = load_data(vars_name, data_root='data')
    
    print(df.head)
    print(df.size)
    df.to_csv('output.csv', index=False)
