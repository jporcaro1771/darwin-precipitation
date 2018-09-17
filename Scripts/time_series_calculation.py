import xarray as xr
import pandas as pd
from copy import deepcopy
from datetime import timedelta, datetime, date
import time
import netCDF4
from mpl_toolkits import basemap
from cartopy.mpl.ticker import LongitudeFormatter, LatitudeFormatter
from cartopy.mpl.gridliner import LONGITUDE_FORMATTER, LATITUDE_FORMATTER
from cartopy.io.img_tiles import StamenTerrain
import cartopy.crs as ccrs
import cartopy.feature as cfeature
import cartopy.io.img_tiles as cimgt
import cartopy
from distributed import Client, LocalCluster
from dask import bag as db
import csv
import gc

cpol_allyears_path = '/lcrc/group/earthscience/radar/CPOL_level_1b/GRIDDED/GRID_70km_1000m/'
out_dir = '/home/jporcaro/cpol_analysis/cpol_out_allyears1/'

def get_col_stats(radar_file_path):
    """Calculates spatial mean and SD over 20 subdomains of an inputed radar file.
       
    Args:
        path: The file path of a .nc radar file         
    Returns:
        A tuple of 20 tuples (one for each subdomain),
        each with an array of means, SDs, and a datetime
    """
    radar_file = pyart.io.read_grid(radar_file_path)

    lon, lat = radar_file.get_point_longitude_latitude()
    height = radar_file.point_z['data'][:,0,0]
    time = np.array([netCDF4.num2date(radar_file.time['data'], radar_file.time['units'])])

    ds = xr.Dataset()

    for this_field in radar_file.fields.keys():
        this_data = radar_file.fields[this_field]['data']
        my_data = xr.DataArray(np.ma.expand_dims(this_data,0),
                                    dims = ('time', 'z', 'y', 'x'),
                                    coords = {'time' : time[0],
                                                 'z' : height,
                                                  'y' : lat[:,0],
                                                  'x' : lon[0,:]})

        for this_meta in list(radar_file.fields[this_field].keys()):
            if this_meta is not 'data':
                my_data.attrs.update({this_meta: radar_file.fields[this_field][this_meta]})

        ds[this_field] = my_data

    ds.z.attrs['long_name'] = "height above sea sea level"
    ds.z.attrs['units'] = "m"
    ds.z.encoding['_FillValue'] = None

    twenty_col = [(-11.75, -11.50, 130.50, 130.75), # A
                  (-11.75, -11.50, 130.75, 131.00), # B
                  (-11.75, -11.50, 131.00, 131.25), # C
                  (-11.75, -11.50, 131.25, 131.50), # D
                  (-12.00, -11.75, 130.50, 130.75), # E
                  (-12.00, -11.75, 130.75, 131.00), # F
                  (-12.00, -11.75, 131.00, 131.25), # G
                  (-12.00, -11.75, 131.25, 131.50), # H
                  (-12.25, -12.00, 130.50, 130.75), # I
                  (-12.25, -12.00, 130.75, 131.00), # J
                  (-12.25, -12.00, 131.00, 131.25), # K
                  (-12.25, -12.00, 131.25, 131.50), # L
                  (-12.50, -12.25, 130.50, 130.75), # M
                  (-12.50, -12.25, 130.75, 131.00), # N
                  (-12.50, -12.25, 131.00, 131.25), # O
                  (-12.50, -12.25, 131.25, 131.50), # P
                  (-12.75, -12.50, 130.50, 130.75), # Q
                  (-12.75, -12.50, 130.75, 131.00), # R
                  (-12.75, -12.50, 131.00, 131.25), # S
                  (-12.75, -12.50, 131.25, 131.50), # T
                 ]

    mean_array = []
    dev_array = []

    for i in twenty_col:
        rain_rate = ds.radar_estimated_rain_rate[0].sel(z=4, method='nearest').sel(
            y=slice(i[0], i[1]), x=slice(i[2], i[3]))

        rain_rate = rain_rate.fillna(0.0)

        rainfall_mean = rain_rate.mean()
        rainfall_dev = rain_rate.std()

        mean_array.append(rainfall_mean)
        dev_array.append(rainfall_dev)

        new_means = np.concatenate([mean_array])
        new_sds = np.concatenate([dev_array])

    subdomains = list(string.ascii_uppercase[:20])
    out_df = pd.DataFrame({
        'dt': [time[0][0] for i in range(len(subdomains))],
        'subd': subdomains,
        'mean': new_means,
        'sd': new_sds
     })

    fn_fmt = '%Y%m%d_%H%M.csv'
    fn = time[0][0].strftime(fn_fmt)
    fn_abs = os.path.join(out_dir_2011, fn)
    out_df.set_index(['dt', 'subd'], inplace=True)
    out_df.to_csv(fn_abs, index=True)

    del out_df
    del radar_file
    
def main():
    cluster = LocalCluster(processes=True)
    the_client = Client(cluster)

    file_list = glob.glob(cpol_2011_path + '/**/*.nc', recursive=True)
    file_list = sorted(file_list)

    the_bag = db.from_sequence(file_list)
    results = the_bag.map(get_col_stats).compute()
    the_client.shutdown()
    print(file_list)


if __name__ == '__main__':
    main()

gc.collect()
print("task complete")

