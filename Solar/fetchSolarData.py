# -*- coding: utf-8 -*-
"""
CSCI-GA.3033-003 - Project: Finding Napa
R. Feng, C. Gilbert, G. Ng


Script to fetch and save NREL data.
Requirements: The APIKeys module that contains the API key dictionaries within
the solarAccess list.

"""
import time
import sys
import os
import urllib.request
import pandas as pd
from APIKeys import solarAccess as sa

att = 'ghi,dhi,dni,wind_speed,air_temperature,solar_zenith_angle'
year = 2017
csvFile = 'solarData.csv'
regionsFile = 'soil_regions.csv'
compList = 'completed.log'

# Some operations need Python 2.7 to run without errors
if float(str(sys.version_info[0]) + "." + str(sys.version_info[1])) < 2.7:
    message = ("Must be using Python 2.7 or above. \n" +
               "For Dumbo users, execute \"module load python/gnu/2.7.11\"")
    raise Exception(message)


def getRangeURL(accessInfo, minx, miny, maxx, maxy, year, att, interval='60'):
    '''
    Fetches the data available in NREL for a given rectangle. Not used.
    '''
    url = ('http://developer.nrel.gov/api/solar/nsrdb_data_query.json?' +
           'wkt=MULTIPOINT' +
           '({minx}%20{miny}'.format(minx=minx, miny=miny) +
           '%2C{minx}%20{maxy}'.format(minx=minx, maxy=maxy) +
           '%2C{maxx}%20{maxy}'.format(maxx=maxx, maxy=maxy) +
           '%2C{maxx}%20{miny})&'.format(maxx=maxx, miny=miny) +
           'names={year}&'.format(year=year) +
           'leap_day={leap}&'.format(leap=accessInfo["leap"]) +
           'interval={interval}&'.format(interval=interval) +
           'utc={utc}&'.format(utc=accessInfo["utc"]) +
           'full_name={name}&'.format(name=accessInfo["name"]) +
           'email={email}&'.format(email=accessInfo["email"]) +
           'affiliation={aff}&'.format(aff=accessInfo["affiliation"]) +
           'mailing_list={ml}&'.format(ml=accessInfo["mailList"]) +
           'reason={reason}&'.format(reason=accessInfo["reason"]) +
           'api_key={api}&'.format(api=accessInfo["key"]) +
           'attributes={attr}'.format(attr=att))
    return(url)


def getURL(i, accessInfo, latitude, longitude, year, att, interval='60'):
    '''
    Iterates over NREL API access keys to create a CSV retrieval URL.
    ~Inputs~
    i: Iteration number. Used to cycle through keys
    accessInfo: list of API access dictionaries. url variable for contents
    latitute: Latitude used to access the closest NREL lat point
    longitude: Latitude used to access the closest NREL lat point
    year: NREL record year
    att: string of NREL attributes to retrieve from NREL
    interval: minute interval of the retrieved data. 30 or 60
    ~Outputs~
    url: URL string to retrieve the NREL data
    '''
    noKeys = len(accessInfo)
    keyIndex = i % noKeys
    curInfo = accessInfo[keyIndex]
    url = ('http://developer.nrel.gov/api/solar/nsrdb_psm3_download.csv?' +
           'wkt=POINT({lon}+{lat})&'.format(lat=latitude, lon=longitude) +
           'names={year}&'.format(year=year) +
           'leap_day={leap}&'.format(leap=curInfo["leap"]) +
           'interval={interval}&'.format(interval=interval) +
           'utc={utc}&'.format(utc=curInfo["utc"]) +
           'full_name={name}&'.format(name=curInfo["name"]) +
           'email={email}&'.format(email=curInfo["email"]) +
           'affiliation={aff}&'.format(aff=curInfo["affiliation"]) +
           'mailing_list={ml}&'.format(ml=curInfo["mailList"]) +
           'reason={reason}&'.format(reason=curInfo["reason"]) +
           'api_key={api}&'.format(api=curInfo["key"]) +
           'attributes={attr}'.format(attr=att))
    return(url)


# If the csv file already exists, remove the old copy
if os.path.exists(csvFile):
    print("Output CSV file {csv} exists. Removing...".format(csv=csvFile))
    os.remove(csvFile)
# If there isn't already a list of completed regions, start one.
if not os.path.exists(compList):
    with open(compList, 'w') as comp:
        comp.write('SoilLat' + '\t' +
                   'SoilLon' + '\t' +
                   'SolarLat' + '\t' +
                   'SolarLon' + '\t' +
                   'Year' + '\n')
# Read in lists of lat/longs:
regions = pd.read_csv(regionsFile)

# if the list of completed lat/longs exist, load in to verify no overwrites
doneList = set([])  # Set of completed lat:long pairs
if os.path.exists(compList):
    with open(compList, 'r') as cf:
        for line in cf:
            sline = line.split('\t')
            if sline[0] == 'SoilLat':
                continue
            oldKey = sline[0] + ":" + sline[1]
            doneList.add(oldKey)
access = 1
for i in range(len(regions)):
    lat_in = round(regions.loc[i, "latitude"], 3)
    lon_in = round(regions.loc[i, "longitude"], 3)
    # If the value has already been added to the data, skip it
    if (str(lat_in) + ':' + str(lon_in)) in doneList:
        print("Lat: {lat}".format(lat=lat_in) +
              " Long: {lon}".format(lon=lon_in) +
              " already completed. Skipping...")
        continue
    # Some Alaska values are outside the range of applicability
    if lat_in > 60:
        print("Lat: {lat}".format(lat=lat_in) +
              " Long: {lon}".format(lon=lon_in) +
              " outside NREL US Data range. Skipping...")
        continue
    # Wait 3 second to follow NREL rules
    print("Waiting for next access...")
    time.sleep(3)
    print('Fetching NREL data for \n' +
          'Latitude: {lat} \n'.format(lat=round(lat_in, 4)) +
          'Longitude: {lon} \n'.format(lon=round(lon_in, 4)) +
          'Year: {yr} \n'.format(yr=year) +
          'Area: {a} \n'.format(a=regions.loc[i, "areaname"]))
    url = getURL(access, sa, lat_in, lon_in, year, att)
    access += 1
    urllib.request.urlretrieve(url, 'workingFile.csv')
    info = pd.read_csv('workingFile.csv', nrows=1, warn_bad_lines=True)
    rows = pd.read_csv('workingFile.csv', skiprows=2, warn_bad_lines=True)
    for i, v in info.T.squeeze().items():
        rows[i] = v
    rows.to_csv(csvFile, mode='a+', header=False, index=False)
    lat_out, lon_out = info.loc[0, "Latitude"], info.loc[0, "Longitude"]

    with open(compList, 'a+') as comp:
        comp.write(str(lat_in) + '\t' +
                   str(lon_in) + '\t' +
                   str(lat_out) + '\t' +
                   str(lon_out) + '\t' +
                   str(year) + '\n')
    break
# Save the data schema for later use
pd.DataFrame(rows.columns).to_csv('solarSchema.csv', header=False, index=False)
