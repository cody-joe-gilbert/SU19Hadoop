# -*- coding: utf-8 -*-
"""
Created on Sun Jul 21 08:20:13 2019

@author: Cody Gilbert
"""

import geopandas
import geoplot
import geoplot.crs as gcrs
from shapely.geometry import Point
from matplotlib.backends.backend_pdf import PdfPages
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np


data = r"C:\Users\Cody Gilbert\Desktop\HadoopClass\Project\SU19Hadoop\tools\voronoiPlotting\cosineSimResults.txt"
soldf = pd.read_csv(data, sep='\t', header=None)
# Drop lowest few values
soldf['lat'] = soldf.apply(lambda x: float(x[0].split(':')[0]), axis=1)
soldf['lon'] = soldf.apply(lambda x: float(x[0].split(':')[1]), axis=1)


soldf = soldf.sort_values(by=[1], ascending=False).iloc[:-10,:]

#%%

minval = min(soldf[1])
napa = float(soldf[soldf[0] == '38.490:-122.340'][1])
soldf['CosSimScaled'] = soldf.apply(lambda x: 100*float(x[1] - minval)/(napa - minval), axis=1)
geometry = [Point(xy) for xy in zip(soldf.lon, soldf.lat)]
crs = {'init': 'epsg:4326'}
gdf = geopandas.GeoDataFrame(soldf, crs=crs, geometry=geometry)
gdf.columns = ['key','CosSim', 'lat', 'lon', 'CosSimScaled', 'geometry']

world = geopandas.read_file(
    geopandas.datasets.get_path('naturalearth_lowres')
)
USA = world.query('name == "United States"')
contiguous_usa = geopandas.read_file(geoplot.datasets.get_path('contiguous_usa'))
proj = gcrs.AlbersEqualArea(central_longitude=-98, central_latitude=39.5)

ax = geoplot.voronoi(gdf.head(3150), projection=proj,
                     clip=USA.simplify(0.01),
                     hue='CosSimScaled', figsize=(15, 15),
                     cmap='Reds', k=None,
                     legend=True,
                     edgecolor='white',
                     linewidth=0.01)
geoplot.polyplot(USA, edgecolor='black', zorder=1, ax=ax,
                 linewidth=1,
                 extent=contiguous_usa.total_bounds)

with PdfPages('mapPlot.pdf') as pdf:
    plt.savefig(pdf, format='pdf')

