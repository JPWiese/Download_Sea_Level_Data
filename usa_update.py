#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Code to download monthly means from NOAA's data API

Created on Thu Jan 10 16:53:13 2019

@author: antt
"""

import requests
import csv
import os
from psmsl.msl.monthly import MonthlyData
from psmsl.msl.monthly import save_task_file

def get_station_list(update_file):
    '''
    Gets a list of NOAA stations to be updated 
    Returns an array of a dictionary with the following fields:
        psmsl: psmsl id
        noaa:  noaa id
    '''
    station_list = []
    with open(update_file, 'r') as csvfile:
        csv_reader = csv.reader(csvfile)
        for row in csv_reader:
            psmsl = int(row[0])
            noaa = int(row[1])
            station_list.append({'psmsl': psmsl, 'noaa': noaa})
    return station_list

def download_station(noaa_id, start_year, end_year):
    '''
    Returns monthly data for a given station between given years
    Returns an array of a MonthlyData objects
    '''
    data = []
    for year in range(start_year, end_year+1):
#         print "    Downloading {}".format(year)
        this_data = download_year(noaa_id, year)
        for row in this_data:
            if len(row['height']) == 0:
                # No data for this case - skip
                continue
            this_year = int(row['year'])
            this_month = int(row['month'])
            this_height = float(row['height'])
            data.append(MonthlyData(this_year,this_month,this_height))
    return data
    

def download_year(noaa_id, year):
    '''
    Returns monthly data for a given station and year
    Returns an array of a dictionary with the following fields:
        year: year of monthly data
        month: month of monthly data
        height: mean sea level height, in metres
        inferred: is this value based on inferred data?
    '''
    # The main url:
    url = "http://tidesandcurrents.noaa.gov/api/datagetter"
    # The parameters:
    station = str(noaa_id)
    product = "monthly_mean"
    datum = "STND"
    units = "metric"
    time_zone = "gmt"
    begin_date = str(year) + "0101"
    end_date = str(year) + "1231"
    fmt = 'json'
    application = "NameOfYourOrganization" # They ask you provide organisation name when using api

    # Form payload:
    payload = {
        'station': station,
        'product': product,
        'datum': datum,
        'units': units,
        'time_zone': time_zone,
        'begin_date': begin_date,
        'end_date': end_date,
        'format': fmt,
        'application': application
        }
    
    # Request the data from NOAA
    r = requests.get(url, params = payload)
    # Decode json output
    json = r.json()
    
    out = []
    
    # Look for error & message
    if 'error' in json:
        # Do nothing - probably no data
        # TODO - check this?
        pass
#         print json['error']['message']
    else:
#         # Extract metadata
#         metadata = json['metadata']
#         # Print metadata stuff:
#         this_id = metadata['id']
#         this_name = metadata['name']
#         this_latitude = metadata['lat']
#         this_longitude = metadata['lon']
        # Extract data
        data = json['data']
        # Parse each line of data
        for line in data:
            this_month = line['month']
            this_year = line['year']
            this_msl = line['MSL']
            this_inferred = line['inferred']
            out.append({
                        'year': this_year,
                        'month': this_month,
                        'height': this_msl,
                        'inferred': this_inferred
                        })
            
    return out
            
output_directory = '/work/antt/psmsl/usa/2020/data'
update_file = '/work/antt/psmsl/usa/2020/idList.txt'
station_list = get_station_list(update_file)
start_year = 2015
end_year = 2019

for station in station_list:

   psmsl_id = station['psmsl']
   noaa_id = station['noaa']

   noaa_data = download_station(noaa_id, start_year, end_year)
   output_file = os.path.join(output_directory, str(psmsl_id) + '_' + str(noaa_id) + '.tsk')
   if len(noaa_data) == 0:
      print('No data for station {0} [{1}]'.format(psmsl_id,noaa_id))
   else:
      print('Got data for station {0} [{1}], last data: {2}'.format(psmsl_id,noaa_id,noaa_data[-1].date))
   save_task_file(noaa_data, output_file)

print('Finished')
