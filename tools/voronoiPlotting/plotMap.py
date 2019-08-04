# -*- coding: utf-8 -*-
"""
Given the final combined cosine similarity values in the path given by the
`data` variable and the cosine similarity values in each of the columns
listed in `time_ranges` variable, will plot a Voronoi Plot over the
continental united states for each input.

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

time_ranges = ['comp_sim_2000',
               'comp_sim_2010',
               'comp_sim_future']
data = r"C:\Users\Cody Gilbert\Desktop\HadoopClass\Project\SU19Hadoop\composite\data\results\composite_sim_vF.csv"
soldf = pd.read_csv(data)

# Drop lowest few values
#soldf = soldf.sort_values(by=[1], ascending=False).iloc[:-10,:]
#minval = min(soldf[1])
#napa = float(soldf[soldf['solar_region_key'] == '38.490:-122.340'][1])
#soldf['CosSimScaled'] = soldf.apply(lambda x: 100*float(x[1] - minval)/(napa - minval), axis=1)

geometry = [Point(xy) for xy in zip(soldf.longitude, soldf.latitude)]
crs = {'init': 'epsg:4326'}
gdf = geopandas.GeoDataFrame(soldf, crs=crs, geometry=geometry)


world = geopandas.read_file(
    geopandas.datasets.get_path('naturalearth_lowres')
)
USA = world.query('name == "United States"')
contiguous_usa = geopandas.read_file(geoplot.datasets.get_path('contiguous_usa'))
proj = gcrs.AlbersEqualArea(central_longitude=-98, central_latitude=39.5)


with PdfPages('mapPlot.pdf') as pdf:
    for t in time_ranges:
        ax = geoplot.voronoi(gdf, projection=proj,
                             clip=USA.simplify(0.01),
                             hue=t, figsize=(10, 10),
                             cmap='Reds', k=None,
                             legend=True,
                             edgecolor='white',
                             linewidth=0.01)
        #plt.title('Predicted Future Similarity')
        plt.title(t)
        geoplot.polyplot(USA, edgecolor='black', zorder=1, ax=ax,
                         linewidth=1,
                         extent=contiguous_usa.total_bounds)

        plt.savefig(pdf, format='pdf')

