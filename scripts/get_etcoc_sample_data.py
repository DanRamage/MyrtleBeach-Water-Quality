import sys
sys.path.append('../commonfiles/python')
import os
import logging.config

from datetime import datetime, timedelta
from pytz import timezone
import logging.config
import optparse
import configparser as ConfigParser

from suds.client import Client
from suds.xsd.doctor import Import, ImportDoctor

from mb_wq_data import mb_sample_sites
from search_utils import contains

from dhecBeachAdvisoryReader import waterQualityAdvisory

"""
Function: __scrapeResults
Purpose: This is the function that loops through the station list, queries the webpage for the results creating the individual
  station geoJSON files, as well as a comprehensive  geoJSON file for all the stations with the most recent results.
Parameters:
  stationNfoList - A FeatureCollection object that has geoJSON data for all the stations.
Return:
  None
"""

def query_sample_data(data_dict, year, url, schema_url, sample_sites):
  logger = logging.getLogger(__name__)
  #results = {}
  if logger:
    logging.getLogger('suds.client').setLevel(logging.DEBUG)
    logger.info("SOAP request for beach data.")
  try:
    '''
    #schema_url = "http://gis.dhec.sc.gov/beachservice/beachservice.asmx?schema=beach"
    schema_import = Import(schema_url)
    schema_doctor = ImportDoctor(schema_import)

    soap_client = Client(url=url, doctor=schema_doctor)
    logger.debug("Client: %s" % (soap_client))
    logger.debug("SOAP GetBeachData request for year: %d" % (year.year))
    response = soap_client.service.GetBeachData(year=year.year)
    '''
    schema_url = "http://gis.dhec.sc.gov/beachservice/beachservice.asmx?schema=beach"
    schema_import = Import(schema_url)
    schema_doctor = ImportDoctor(schema_import)

    # soap_client = Client(url=self.baseUrl, doctor=schema_doctor)
    soap_client = Client(url=url, doctor=schema_doctor)
    logger.debug("Client: %s" % (soap_client))
    logger.debug("SOAP GetBeachData request for year: %d" % (year.year))
    response = soap_client.service.GetBeachData(year=year.year)

    for dgram in response.diffgram:
      for DocumentElement in dgram.DocumentElement:
        data_table = None
        if 'beachTable' in DocumentElement:
          data_table = DocumentElement.beachTable
        else:
          data_table = DocumentElement.BeachTable
        if data_table is not None:
          for beachTable in data_table:
            data = {
              'date': "",
              'value': ""
            }
            #Verify station is in sample sites.
            if contains(sample_sites, lambda site: site.name == beachTable.Name[0]):
              data['station'] = beachTable.Name[0]
              if data['station'] not in data_dict:
                data_dict[data['station']] = {'results': []}

              date_parts = beachTable.SamplingDate[0].split('T')
              time_parts = date_parts[1].split('-')

              date_rec = datetime.strptime("%s %s" % (date_parts[0], time_parts[0]), '%Y-%m-%d %H:%M:%S')
              #date_parts = beachTable.SamplingDate[0].split('T')
              data['date'] = date_rec.strftime('%Y-%m-%d %H:%M:%S')
              data['value'] = beachTable.ETCOC[0]
              data_dict[data['station']]['results'].append(data)
  except Exception as e:
    if logger:
      logger.exception(e)
    return False
  logger.debug("SOAP request completed.")
  return True

def get_historical_samples(date_list, sample_sites, schema_url, base_url):
  data_dict = {}
  for year in date_list:
    year_obj = datetime.strptime(year, '%Y')
    query_sample_data(data_dict, year_obj, base_url, schema_url, sample_sites)
  for station_name in data_dict:
    station_data = data_dict[station_name]
    sorted_data = sorted(station_data['results'], key=lambda rec: rec['date'])
    station_data['results'] = sorted_data
  return data_dict

def main():
  parser = optparse.OptionParser()
  parser.add_option("-c", "--ConfigFile", dest="config_file",
                    help="INI Configuration file." )
  parser.add_option("-y", "--Years", dest="year_list",
                    help="List of years to retrieve the sample data for. Format is [YYYY-MM-DD]" )
  parser.add_option("-f", "--OutfileDirectory", dest="out_dir",
                    help="Directory to write the historical records.")
  (options, args) = parser.parse_args()

  config_file = ConfigParser.RawConfigParser()
  config_file.read(options.config_file)

  logConfFile = config_file.get('logging', 'config_file')
  if logConfFile:
    logging.config.fileConfig(logConfFile)
    logger = logging.getLogger(__name__)
    logger.info("Log file opened.")

  try:
    schema_url = config_file.get("dhec_soap_service", "schema_url")
    base_url = config_file.get("dhec_soap_service", "base_url")

    boundaries_location_file = config_file.get('boundaries_settings', 'boundaries_file')
    sites_location_file = config_file.get('boundaries_settings', 'sample_sites')
    dhec_rest_url = config_file.get('dhec_soap_service', 'dhec_rest_url')
  except ConfigParser.Error as e:
    if logger:
      logger.exception(e)
  else:
    mb_sites = mb_sample_sites()
    mb_sites.load_sites(file_name=sites_location_file, boundary_file=boundaries_location_file)

    dates = options.year_list.split(',')
    sample_data = get_historical_samples(dates, mb_sites, schema_url, base_url)


    complete_file = "%s.csv" % (os.path.join(options.out_dir, 'etcoc_all_stations'))
    if logger:
      logger.info("Creating file: %s to write all results" % (complete_file))
    with open(complete_file, "w") as complete_station_etcoc_file:
      for station in mb_sites:
        try:
          file_name = "%s.csv" % (os.path.join(options.out_dir, station.name))
          if logger:
            logger.info("Creating file: %s to write results" % (file_name))
          with open(file_name, "w") as station_etcoc_file:
            station_data = sample_data[station.name]
            for sample_rec in station_data['results']:
              station_etcoc_file.write('%s,%s,%s\n' % (station.name, sample_rec['date'], sample_rec['value']))
              complete_station_etcoc_file.write('%s,%s,%s\n' % (station.name, sample_rec['date'], sample_rec['value']))
        except (IOError, Exception) as e:
          if logger:
            logger.exception(e)
if __name__ == "__main__":
  main()