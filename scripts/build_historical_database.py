import sys
sys.path.append('../commonfiles/python')
import os

import logging.config
from datetime import datetime, timedelta
from pytz import timezone

import optparse
import ConfigParser
import csv
from unitsConversion import uomconversionFunctions
from wqDatabase import wqDB


platform_metadata = {
  'lbhmc.2ndAveNorth.pier':
    {
      'latitude': 33.682793,
      'longitude': -78.884148
    },
  'lbhmc.Apache.pier':
    {
      'latitude':  33.76158,
      'longitude': -78.779
    },
  'lbhmc.CherryGrove.pier':
    {
      'latitude':  33.827676,
      'longitude': -78.632021
    },
  'carocoops.SUN2.buoy':
    {
      'latitude':  33.8373,
      'longitude': -78.4768
    }


}

pier_obs_to_xenia = {
  "Temp": {
    "units": "F",
    "xenia_name": "water_temperature",
    "xenia_units": "celsius"
  },
  "Salinity": {
    "units": "ppt",
    "xenia_name": "salinity",
    "xenia_units": "psu"
  },
  "Depth": {
    "units": "m",
    "xenia_name": "depth",
    "xenia_units": "m"
  },
  "ODO Conc": {
    "units": "mg/L",
    "xenia_name": "oxygen_concentration",
    "xenia_units": "mg_L-1"
  },
  "ODO%": {
    "units": "%",
    "xenia_name": "oxygen_concentration",
    "xenia_units": "%"
  },
  "pH": {
    "units": "",
    "xenia_name": "ph",
    "xenia_units": ""
  },
  "Chlorophyll": {
    "units": "ug/L",
    "xenia_name": "chl_concentration",
    "xenia_units": "ug_L-1"
  },
  "Turbidity": {
    "units": "ntu",
    "xenia_name": "turbidity",
    "xenia_units": "ntu"
  },
  "Turbidity+": {
    "units": "ntu",
    "xenia_name": "turbidity",
    "xenia_units": "ntu"
  },
  "Air Temp": {
    "units": "F",
    "xenia_name": "air_temperature",
    "xenia_units": "celsius"
  },
  "BP": {
    "units": "inHg",
    "xenia_name": "air_pressure",
    "xenia_units": "mb"
  },
  "Barometric Pressure": {
    "units": "inHg",
    "xenia_name": "air_pressure",
    "xenia_units": "mb"
  },
  "RH": {
    "units": "%",
    "xenia_name": "relative_humidity",
    "xenia_units": "%"
  },
  "Relative Humidity": {
    "units": "%",
    "xenia_name": "relative_humidity",
    "xenia_units": "%"
  },
  "Wind Dir": {
    "units": "Degrees",
    "xenia_name": "wind_from_direction",
    "xenia_units": "degrees_true"
  },
  "Wind Speed": {
    "units": "mph",
    "xenia_name": "wind_speed",
    "xenia_units": "m_s-1"
  },
  "Rainfall": {
    "units": "in",
    "xenia_name": "precipitation",
    "xenia_units": "mm"
  },
  "DO Conc": {
    "units": "mg/L",
    "xenia_name": "oxygen_concentration",
    "xenia_units": "mg_L-1"

  },
  "DO Saturation": {
    "units": "%",
    "xenia_name": "oxygen_concentration",
    "xenia_units": "%"
  }
}
pier_met_sensors = ["Air Temp","BP","RH","Wind Dir","Wind Speed","Rainfall","Air Temp","Barometric Pressure","Relative Humidity","Wind Direction" ]

sun2_to_xenia = {
  "fcat_salinity": {
    "units": "psu",
    "xenia_name": "salinity",
    "xenia_units": "psu"

  },
  "fcat_temp": {
    "units": "celsius",
    "xenia_name": "water_temperature",
    "xenia_units": "celsius"
  },
  "ws": {
    "units": "m_s-1",
    "xenia_name": "wind_speed",
    "xenia_units": "m_s-1"

  },
  "wd": {
    "units": "degrees_true",
    "xenia_name": "wind_from_direction",
    "xenia_units": "degrees_true"

  }
}
def process_pier_files(platform_name,
                      file_name,
                       start_data_row,
                       header_row,
                       units_convereter,
                       xenia_db,
                       utc_start_date):
  logger = logging.getLogger(__name__)
  obs_keys = pier_obs_to_xenia.keys()
  header_list = header_row.split(",")
  row_entry_date = datetime.now()
  eastern_tz = timezone('US/Eastern')
  utc_tz = timezone('UTC')
  with open(file_name, "r") as data_file:
    csv_reader = csv.reader(data_file)
    line_num = 0
    checked_platform_exists = False
    date_ndx = header_list.index('Date')
    time_ndx = header_list.index('Time')
    platform_meta = None
    if platform_name in platform_metadata:
      platform_meta = platform_metadata[platform_name]
    for row in csv_reader:
      if line_num >= start_data_row:
        date = row[date_ndx]
        #date format is incosistent, sometimes leadings zeros, other times not.
        month, day, year = date.split('/')
        time = row[time_ndx]
        hour, minute = time.split(':')
        obs_date = eastern_tz.localize(datetime(year=int(year), month=int(month), day=int(day),
                         hour=int(hour), minute=int(minute), second=0))
        utc_obs_date = obs_date.astimezone(utc_tz)

        if utc_start_date is not None and utc_obs_date < utc_start_date:
          continue

        if not checked_platform_exists:
          if xenia_db.platformExists(platform_name) == -1:
            checked_platform_exists = True
            obs_list = []
            for ndx, header_key in enumerate(header_list):
              header_key = header_key.lower().strip()
              logger.debug("Trying to match obs: %s to row" % (header_key))
              matched_obs = None
              s_order = 1
              for obs_key in obs_keys:
                if header_key.find(obs_key.lower().strip()) != -1:
                  if header_key.find('surface') != -1 and header_key.find(obs_key.lower()) != -1:
                    matched_obs = obs_key
                  elif header_key.find('bottom') != -1 and header_key.find(obs_key.lower()) != -1:
                    s_order = 2
                    matched_obs = obs_key
                  else:
                    matched_obs = obs_key

                if matched_obs is not None:
                  obs_info = pier_obs_to_xenia[matched_obs]
                  logger.debug("Matched row obs: %s to obs: %s to row" % (header_key, obs_info['xenia_name']))
                  obs_list.append({'obs_name': obs_info['xenia_name'],
                                   'uom_name': obs_info['xenia_units'],
                                   's_order': s_order})
                  break
            xenia_db.buildMinimalPlatform(platform_name, obs_list)
        found_obs = []
        for ndx, header_key in enumerate(header_list):
          matched_obs = None
          clean_hdr_key = header_key.lower().strip()
          s_order = 1
          for obs_key in obs_keys:
            if clean_hdr_key.find(obs_key.lower().strip()) != -1:
              if clean_hdr_key.find('surface') != -1 and clean_hdr_key.find(obs_key.lower()) != -1:
                matched_obs = obs_key
              elif clean_hdr_key.find('bottom') != -1 and clean_hdr_key.find(obs_key.lower()) != -1:
                s_order = 2
                matched_obs = obs_key
              else:
                matched_obs = obs_key


            if matched_obs is not None:
              #Files that don't use Surface or Bottom indicators, this is how we differentiate
              #as bottom are always first in header list.
              if obs_key not in pier_met_sensors:
                if obs_key not in found_obs:
                  s_order = 2
                found_obs.append(obs_key)

              if len(row[ndx]):
                obs_info = pier_obs_to_xenia[obs_key]
                obs_val = float(row[ndx])
                if obs_info['units'] != obs_info['xenia_units']:
                  obs_val = units_convereter.measurementConvert(obs_val, obs_info['units'], obs_info['xenia_units'])
                  if obs_val is None:
                    obs_val
                logger.debug("Row: %d Adding obs: %s(%s) Date: %s Value: %s S_Order: %d" %\
                             (line_num, obs_info['xenia_name'], obs_info['xenia_units'], utc_obs_date.strftime("%Y-%m-%d %H:%M:%S"), obs_val, s_order))
                try:
                  xenia_db.addMeasurement(obs_info['xenia_name'],
                                          obs_info['xenia_units'],
                                          platform_name,
                                          utc_obs_date,
                                          platform_meta['latitude'],
                                          platform_meta['longitude'],
                                          0,
                                          [obs_val],
                                          sOrder=s_order,
                                          autoCommit=True,
                                          rowEntryDate=row_entry_date )
                except Exception as e:
                  logger.exception(e)
              break
          if matched_obs is None:
            if header_key != 'Date' and header_key != 'Time':
              logger.error("Could not match obs: %s" % (header_key))
              sys.exit(-1)
      line_num += 1
  return

def process_sun2_files(platform_name,
                       file_name,
                       start_data_row,
                       header_row,
                       units_converter,
                       xenia_db,
                       utc_start_date):

  logger = logging.getLogger(__name__)
  obs_keys = sun2_to_xenia.keys()
  header_list = header_row.split(",")
  row_entry_date = datetime.now()
  utc_tz = timezone('UTC')
  with open(file_name, "rU") as data_file:
    csv_reader = csv.reader(data_file)
    line_num = 0
    checked_platform_exists = False
    try:
      date_time_ndx = header_list.index('datetime')
    except ValueError as e:
      date_time_ndx = header_list.index('measurement_date')

    platform_meta = None
    if platform_name in platform_metadata:
      platform_meta = platform_metadata[platform_name]
    for row in csv_reader:
      if line_num >= start_data_row:
        date_val = row[date_time_ndx]

        utc_obs_date = utc_tz.localize(datetime.strptime(row[date_time_ndx], '%Y-%m-%d %H:%M:%S'))

        if utc_start_date is not None and utc_obs_date.date() < utc_start_date.date():
          continue

        if not checked_platform_exists:
          checked_platform_exists = True
          obs_list = []
          for ndx, header_key in enumerate(header_list):
            header_key = header_key.lower().strip()
            logger.debug("Trying to match obs: %s to row" % (header_key))
            matched_obs = None
            s_order = 1
            add_sensors = False
            for obs_key in obs_keys:
              if header_key.find(obs_key.lower().strip()) != -1:
                matched_obs = obs_key

              if matched_obs is not None:
                obs_info = sun2_to_xenia[matched_obs]
                if xenia_db.sensorExists(obs_info['xenia_name'],
                                         obs_info['xenia_units'],
                                         platform_name,
                                         s_order) == -1:
                  add_sensors = True
                  logger.debug("Matched row obs: %s to obs: %s to row" % (header_key, obs_info['xenia_name']))
                  obs_list.append({'obs_name': obs_info['xenia_name'],
                                   'uom_name': obs_info['xenia_units'],
                                   's_order': s_order})
                break
          if xenia_db.platformExists(platform_name) == -1 or add_sensors:
            xenia_db.buildMinimalPlatform(platform_name, obs_list)

        for ndx, header_key in enumerate(header_list):
          matched_obs = None
          clean_hdr_key = header_key.lower().strip()
          s_order = 1
          for obs_key in obs_keys:
            if clean_hdr_key.find(obs_key.lower().strip()) != -1:
              matched_obs = obs_key
              break

          if matched_obs is not None:
            if len(row[ndx]):
              obs_info = sun2_to_xenia[obs_key]
              obs_val = float(row[ndx])
              if obs_info['units'] != obs_info['xenia_units']:
                obs_val = units_converter.measurementConvert(obs_val, obs_info['units'], obs_info['xenia_units'])
                if obs_val is None:
                  obs_val
              logger.debug("Row: %d Adding obs: %s(%s) Date: %s Value: %s S_Order: %d" %\
                           (line_num, obs_info['xenia_name'], obs_info['xenia_units'], utc_obs_date.strftime("%Y-%m-%d %H:%M:%S"), obs_val, s_order))
              try:
                xenia_db.addMeasurement(obs_info['xenia_name'],
                                        obs_info['xenia_units'],
                                        platform_name,
                                        utc_obs_date,
                                        platform_meta['latitude'],
                                        platform_meta['longitude'],
                                        0,
                                        [obs_val],
                                        sOrder=s_order,
                                        autoCommit=True,
                                        rowEntryDate=row_entry_date )
              except Exception as e:
                logger.exception(e)

        if matched_obs is None:
          if header_key != 'Date' and header_key != 'Time':
            logger.error("Could not match obs: %s" % (header_key))
            sys.exit(-1)
      line_num += 1

def main():
  parser = optparse.OptionParser()
  parser.add_option("-c", "--ConfigFile", dest="config_file",
                    help="INI Configuration file." )
  parser.add_option("-f", "--PierDataFile", dest="pier_data_file", default=None,
                    help="" )
  parser.add_option("-u", "--SUN2DataFile", dest="sun2_data_file", default=None,
                    help="" )
  parser.add_option("-a", "--Header", dest="header_row",
                    help="" )
  parser.add_option("-r", "--FirstDataRow", dest="first_data_row",
                    help="" )
  parser.add_option("-p", "--PlatformName", dest="platform_name",
                    help="" )
  parser.add_option("-s", "--StartDate", dest="start_date", default=None,
                    help="" )

  (options, args) = parser.parse_args()

  config_file = ConfigParser.RawConfigParser()
  config_file.read(options.config_file)

  logConfFile = config_file.get('logging', 'config_file')
  if logConfFile:
    logging.config.fileConfig(logConfFile)
    logger = logging.getLogger(__name__)
    logger.info("Log file opened.")

  units_file = config_file.get("units_conversion", "config_file")
  units_conversion = uomconversionFunctions(units_file)

  historical_db = config_file.get("historical_database", "name")
  try:
    xenia_db = wqDB(historical_db, __name__)
  except Exception as e:
    if logger:
      logger.exception(e)

  utc_start_date = None
  if options.start_date is not None:
    utc_start_date = timezone('US/Eastern').localize(datetime.strptime(options.start_date, '%Y-%m-%d %H:%M:%S')).astimezone(timezone('UTC'))
  if options.pier_data_file is not None:
    process_pier_files(options.platform_name,
                       options.pier_data_file,
                       int(options.first_data_row),
                       options.header_row,
                       units_conversion,
                       xenia_db,
                       utc_start_date)
  elif options.sun2_data_file is not None:
    process_sun2_files(options.platform_name,
                       options.sun2_data_file,
                       int(options.first_data_row),
                       options.header_row,
                       units_conversion,
                       xenia_db,
                       utc_start_date)
  if logger:
    logger.info("Log closed.")
if __name__ == "__main__":
  main()

