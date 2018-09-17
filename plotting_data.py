import pyart
import matplotlib.pyplot as plt
from matplotlib import rcParams
import matplotlib.ticker as mticker
import glob
import numpy as np
import xarray
import pandas as pd
from copy import deepcopy
from datetime import timedelta, datetime
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
import plotly
%pylab inline
# B = Tiwi Islands, I = Over Ocean, N = Darwin

df = pd.read_pickle('darwin_cpol_df')

hour_group = df.groupby(['hour']).mean()
month_group = df.groupby(['month']).mean()

year_month_hour_group = df.groupby(['year', 'month', 'hour']).mean()
year_month_group = df.groupby(['year', 'month']).mean()
rainfall_cols = df.groupby(['subd', 'year', 'month', 'hour']).mean()

mean_rainfall_cols = rainfall_cols['rainfall vals']
sd_rainfall_cols = rainfall_cols['sd']

mean_rainfall_months = month_group['rainfall vals']
sd_rainfall_months = month_group['sd']

mean_rainfall_ymh = year_month_hour_group['rainfall vals']
sd_rainfall_ymh = year_month_hour_group['sd']

mean_rainfall_ym = year_month_group['rainfall vals']
sd_rainfall_ym = year_month_group['sd']


# plotting mean diurnal cycle rainfall rate
fig, ax = plt.subplots(figsize=[15, 5])

# MEAN subselected subdomains
plotDarwin_mean = list(mean_rainfall_cols.loc['H', :, :, :])
plotWater_mean = list(mean_rainfall_cols.loc['T', :, :, :])

# over land
plotcolA = list(mean_rainfall_cols.loc['A', :, :, :])
plotcolB = list(mean_rainfall_cols.loc['B', :, :, :])
plotcolO = list(mean_rainfall_cols.loc['O', :, :, :])
plotcolP = list(mean_rainfall_cols.loc['P', :, :, :])
plotcolQ = list(mean_rainfall_cols.loc['Q', :, :, :])
plotcolR = list(mean_rainfall_cols.loc['R', :, :, :])

# over ocean
plotcolH = list(mean_rainfall_cols.loc['H', :, :, :])
plotcolI = list(mean_rainfall_cols.loc['I', :, :, :])
plotcolJ = list(mean_rainfall_cols.loc['J', :, :, :])

over_lands = np.array([plotcolA, plotcolB, plotcolO, plotcolP,
                           plotcolQ, plotcolR])
over_oceans = np.array([plotcolH, plotcolI, plotcolJ])
land_and_ocean = np.array([plotcolA, plotcolB, plotcolO, plotcolP, plotcolQ, plotcolR, plotcolH, plotcolI, plotcolJ])

land_mean = np.average(over_lands, axis=0)
ocean_mean = np.average(over_oceans, axis=0)
land_ocean_mean = np.average(land_and_ocean, axis=0)

ax.plot(land_mean, linewidth=2.5, c='green')
ax.plot(ocean_mean, linewidth=2.5, c='blue')
ax.plot(land_ocean_mean, linewidth=2.8, c='black')

ax.set_xlim(0, 24)
ax.set_ylim(0.0, 2.0)
ax.set_xticks(np.arange(0,24))
ax.set_xlabel('Hour of Day')
ax.set_ylabel('Rainfall Rate (mm/hr)')
matplotlib.rc('xtick', labelsize=18)
matplotlib.rc('ytick', labelsize=18)
plt.legend(['Over Land', 'Over Ocean', 'Land + Ocean'])
ax.set_title('Mean Diurnal Cycle Rainfall Rate, 1999-2016', fontsize=30, ha='center')
[i.set_linewidth(1.5) for i in ax.spines.values()]


# Plotting mean rainfall rate by month

plot_Jan = list(mean_rainfall_ymh.loc[:, 10, :])
plot_Feb = list(mean_rainfall_ymh.loc[:, 11, :])
plot_Mar = list(mean_rainfall_ymh.loc[:, 12, :])
plot_Apr = list(mean_rainfall_ymh.loc[:, 1, :])
plot_May = list(mean_rainfall_ymh.loc[:, 2, :])
plot_Oct = list(mean_rainfall_ymh.loc[:, 3, :])
plot_Nov = list(mean_rainfall_ymh.loc[:, 4, :])
plot_Dec = list(mean_rainfall_ymh.loc[:, 5, :])

plot_months = [plot_Jan, plot_Feb, plot_Mar, plot_Apr, plot_May, plot_Oct, plot_Nov, plot_Dec]
plot_months_out = []
monthsAll = []
for i in plot_months:
    i = np.array(i)
    plot_months_out.append(i)
for j in plot_months_out:
    j = np.average(j, axis=0)
    monthsAll.append(j)

monthticks = ['placeholder', 'October', 'November', 'December', 'January', 'February', 'March', 'April', 'May']

fig, ax = plt.subplots(figsize=[15, 5])
ax.plot(monthsAll, linewidth=2.5)
ax.set_xticklabels(monthticks)
ax.set_ylim(0.0, 0.55)
ax.set_xlabel('Month')
ax.set_ylabel('Rainfall Rate (mm/hr)')
matplotlib.rc('xtick', labelsize=18)
matplotlib.rc('ytick', labelsize=18)
ax.set_title('Mean Rainfall Rate by Month (1998-2016)', fontsize=30, ha='center')
