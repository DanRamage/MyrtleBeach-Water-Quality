import sys
sys.path.append('../commonfiles/python')
import os

import logging.config
from datetime import datetime, timedelta
from pytz import timezone

import optparse
import ConfigParser
import csv
from collections import OrderedDict
from mb_wq_data import mb_wq_historical_data, mb_sample_sites
from wqHistoricalData import wq_defines

def main():
  parser = optparse.OptionParser()
  parser.add_option("-c", "--ConfigFile", dest="config_file",
                    help="INI Configuration file." )
  parser.add_option("-y", "--ETCOCDirectory", dest="etcoc_directory",
                    help="" )
  parser.add_option('-d', "--OutputDirectory", dest="output_dir", default="",
                    help="")
  (options, args) = parser.parse_args()

  config_file = ConfigParser.RawConfigParser()
  config_file.read(options.config_file)

  logConfFile = config_file.get('logging', 'config_file')
  if logConfFile:
    logging.config.fileConfig(logConfFile)
    logger = logging.getLogger(__name__)
    logger.info("Log file opened.")

  if logger:
    logger.info("Building unique sample dates list.")


  sample_sites = mb_sample_sites()
  sample_sites.load_sites(file_name=config_file.get('boundaries_settings', 'sample_sites'),
                          boundary_file=config_file.get('boundaries_settings', 'boundaries_file'))

  tide_data_file = config_file.get('tide_data', 'file')
  historical_obs_db = config_file.get('historical_database', 'name')
  spatialite_lib = config_file.get('database', 'spatiaLiteLib')
  eastern_tz = timezone('US/Eastern')
  utc_tz = timezone('UTC')

  if len(options.output_dir):
    mb_data_obj = mb_wq_historical_data(tide_data_file=tide_data_file,
                                        xenia_database_name=historical_obs_db,
                                        xenia_nexrad_database_name=historical_obs_db,
                                        spatialite_lib=spatialite_lib)
    for sample_site in sample_sites:
      mb_data_obj.reset(site=sample_site)

      write_site_data_file_header = True

      data_file = os.path.join(options.output_dir, '%s_historical.csv' % (sample_site.name))
      with open(data_file, 'w') as site_data_file:

        fields = ['station', 'date', 'etcoc']

        sample_site_etcoc = os.path.join(options.etcoc_directory, "%s.csv" % (sample_site.name))
        with open(sample_site_etcoc, "r") as etcoc_file:
          etcoc_read = csv.DictReader(etcoc_file, fieldnames=fields)
          row_id = 0
          for row in etcoc_read:
            date_only, time_only = row['date'].split(' ')
            etcoc_value = row['etcoc']

            wq_date = eastern_tz.localize(datetime.strptime(date_only, '%Y-%m-%d'))
            wq_utc_date = wq_date.astimezone(utc_tz)

            if logger:
              logger.debug("Start building historical wq data for: %s Date/Time UTC: %s/EST: %s" % (sample_site.name, wq_utc_date, wq_date))

            site_data = OrderedDict([('id', row_id),
                                    ('station_name',sample_site.name),
                                     ('sample_datetime', wq_date.strftime("%Y-%m-%d %H:%M:%S")),
                                     ('sample_datetime_utc', wq_utc_date.strftime("%Y-%m-%d %H:%M:%S")),
                                     ('enterococcus_value', etcoc_value)])
            try:
              mb_data_obj.query_data(wq_utc_date, wq_utc_date, site_data)
            except Exception,e:
              if logger:
                logger.exception(e)
              sys.exit(-1)
            else:
              header_buf = []
              data = []
              for key in site_data:
                if write_site_data_file_header:
                  if '2ndavenorth' not in key:
                    header_buf.append(key)
                  else:
                    header_buf.append(key.replace('2ndavenorth', 'secondavenorth'))
                if site_data[key] != wq_defines.NO_DATA:
                  data.append(str(site_data[key]))
                else:
                  data.append("")
              if write_site_data_file_header:
                site_data_file.write(",".join(header_buf))
                site_data_file.write('\n')
                header_buf[:]
                write_site_data_file_header = False

              site_data_file.write(",".join(data))
              site_data_file.write('\n')
              site_data_file.flush()
              data[:]
              if logger:
                logger.debug("Finished building historical wq data for: %s Date/Time UTC: %s/EST: %s" % (sample_site.name, wq_utc_date, wq_date))

            row_id += 1

  return

if __name__ == "__main__":
  main()