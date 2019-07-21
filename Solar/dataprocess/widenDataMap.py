"""
CSCI-GA.3033-003 - Project: Finding Napa
R. Feng, C. Gilbert, G. Ng

Map script for widening the data as part of the dataProcess.py execution.
This map script extracts the lat-lon as a regional key, and extracts GHI and
month as values.

~Input~
The schema of the solarData.csv file

~Output~
"key GHI Month" string where
key: latitude + ":" + longitude
GHI: GHI element of the solardata.csv record
Month: Month element of the solardata.csv record

@author: Cody Gilbert
"""
import sys
schema = {1: "Year",
          2: "Month",
          3: "Day",
          4: "Hour",
          5: "Minute",
          6: "GHI",
          7: "DHI",
          8: "DNI",
          9: "Wind Speed",
          10: "Temperature",
          11: "Solar Zenith Angle",
          12: "Source",
          13: "Location ID",
          14: "City",
          15: "State",
          16: "Country",
          17: "Latitude",
          18: "Longitude",
          19: "Time Zone",
          20: "Elevation",
          21: "Local Time Zone",
          22: "Clearsky DHI Units",
          23: "Clearsky DNI Units",
          24: "Clearsky GHI Units",
          25: "Dew Point Units",
          26: "DHI Units",
          27: "DNI Units",
          28: "GHI Units",
          29: "Solar Zenith Angle Units",
          30: "Temperature Units",
          31: "Pressure Units",
          32: "Relative Humidity Units",
          33: "Precipitable Water Units",
          34: "Wind Direction Units",
          35: "Cloud Type -15",
          36: "Cloud Type 0",
          37: "Cloud Type 1",
          38: "Cloud Type 2",
          39: "Cloud Type 3",
          40: "Cloud Type 4",
          41: "Cloud Type 5",
          42: "Cloud Type 6",
          43: "Cloud Type 7",
          44: "Cloud Type 8",
          45: "Cloud Type 9",
          46: "Cloud Type 10",
          47: "Cloud Type 11",
          48: "Cloud Type 12",
          49: "Fill Flag 0",
          50: "Fill Flag 1",
          51: "Fill Flag 2",
          52: "Fill Flag 3",
          53: "Fill Flag 4",
          54: "Fill Flag 5",
          55: "Surface Albedo Units",
          56: "Version"}

months = {1: "JAN",
          2: "FEB",
          3: "MAR",
          4: "APR",
          5: "MAY",
          6: "JUN",
          7: "JUL",
          8: "AUG",
          9: "SEP",
          10: "OCT",
          11: "NOV",
          12: "DEC"}
for line in sys.stdin:
    sline = line.strip().split(",")
    for i, elem in enumerate(sline):
        if schema[i + 1] == "Month":
            m = elem
        elif schema[i + 1] == "GHI":
            ghi = elem
        elif schema[i + 1] == "Latitude":
            lat = "%.3f" % float(elem)
        elif schema[i + 1] == "Longitude":
            lon = "%.3f" % float(elem)
        else:
            continue
    print("{key}\t{g}\t{m}".format(key=(lat + ":" + lon), g=ghi, m=m))
