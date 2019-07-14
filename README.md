# SU19Hadoop
Team project directory for the Summer 2019 Hadoop class.


# Weather HDFS filepaths:

full dataset: /user/rf1316/combined.txt
10000 row extract: /user/rf1316/subset.txt
station definitions: /user/rf1316/station_ids.txt

sample line: 
2019 06 29 21   227    65 10180   310    15     0 -9999 -9999 ./720997-99999-2019

format by multispace delimit:
Year
Month
Day
Hour
Temperature (Celsius x 10)
Dew Point Temp (Celsius x 10)
Sea Level Pressur (Hectopascals x 10)
Wind Direction (Angle measured in a clockwise direction from true north)
Wind Speed (meters / second)
Sky Condition Total Coverage (the higher the number the more the overcast)
Liquid Precipitation Depth (1hr)
Liquid Precipitation Depth (6hr)
Station Identifier and Year (see station_id.txt for station lat/lon)
