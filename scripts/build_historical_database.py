import sys
sys.path.append('../commonfiles/python')
import os

import logging.config
from datetime import datetime, timedelta
from dateutil.relativedelta import *
from pytz import timezone
import requests
import optparse
if sys.version_info[0] < 3:
  import ConfigParser
else:
  import configparser as ConfigParser
import csv
import json
from pyoos.collectors.coops.coops_sos import CoopsSos
from xlrd import open_workbook,cellname


from unitsConversion import uomconversionFunctions
from wqDatabase import wqDB
from build_tide_file import create_tide_data_file_mp

from xeniaSQLiteAlchemy import xeniaAlchemy as sl_xeniaAlchemy, multi_obs as sl_multi_obs, platform as sl_platform

from xenia_obs_map import obs_map, json_obs_map


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
  "Bottom_chlorophyll(ug/L)":{
    "units": "ug/L",
    "xenia_name": "chl_concentration",
    "xenia_units": "ug_L-1"

  },
  "Bottom_depth(m)":{
    "units": "m",
    "xenia_name": "depth",
    "xenia_units": "m"
  },
  "Bottom_DO(mg/L)":{
    "units": "mg/L",
    "xenia_name": "oxygen_concentration",
    "xenia_units": "mg_L-1"
  },
  "Bottom_%DO(%)":{
    "units": "%",
    "xenia_name": "oxygen_concentration",
    "xenia_units": "%"
  },
  "Bottom_pH(SU)":{
    "units": "",
    "xenia_name": "ph",
    "xenia_units": ""
  },
  "Bottom_salinity(PSU)":{
    "units": "ppt",
    "xenia_name": "salinity",
    "xenia_units": "psu"
  },
  "Bottom_temperature(degF)":{
    "units": "F",
    "xenia_name": "water_temperature",
    "xenia_units": "celsius"
  },
  "Bottom_turbidity(NTU)":{
    "units": "ntu",
    "xenia_name": "turbidity",
    "xenia_units": "ntu"
  },
  "Surface_chlorophyll(ug/L)":{
    "units": "ug/L",
    "xenia_name": "chl_concentration",
    "xenia_units": "ug_L-1"

  },
  "Surface_depth(m)":{
    "units": "m",
    "xenia_name": "depth",
    "xenia_units": "m"
  },
  "Surface_DO(mg/L)":{
    "units": "mg/L",
    "xenia_name": "oxygen_concentration",
    "xenia_units": "mg_L-1"
  },
  "Surface_%DO(%)":{
    "units": "%",
    "xenia_name": "oxygen_concentration",
    "xenia_units": "%"
  },
  "Surface_pH(SU)":{
    "units": "",
    "xenia_name": "ph",
    "xenia_units": ""
  },
  "Surface_salinity(PSU)":{
    "units": "ppt",
    "xenia_name": "salinity",
    "xenia_units": "psu"
  },
  "Surface_temperature(degF)":{
    "units": "F",
    "xenia_name": "water_temperature",
    "xenia_units": "celsius"
  },
  "Surface_turbidity(NTU)": {
    "units": "ntu",
    "xenia_name": "turbidity",
    "xenia_units": "ntu"
  },
  "W_Air_temp(degF)":{
    "units": "F",
    "xenia_name": "air_temperature",
    "xenia_units": "celsius"
  },
  "W_BP(inHg)": {
    "units": "inHg",
    "xenia_name": "air_pressure",
    "xenia_units": "mb"
  },
  "W_Rainfall(in)": {
    "units": "in",
    "xenia_name": "precipitation",
    "xenia_units": "mm"
  },
  "W_Rel_humidity(%)": {
    "units": "%",
    "xenia_name": "relative_humidity",
    "xenia_units": "%"
  },
  "W_Wind_direction(deg)": {
    "units": "Degrees",
    "xenia_name": "wind_from_direction",
    "xenia_units": "degrees_true"
  },
  "W_Wind_speed(mph)": {
    "units": "mph",
    "xenia_name": "wind_speed",
    "xenia_units": "m_s-1"
  },
  "Bottom Depth":
  {
    "units": "m",
    "xenia_name": "depth",
    "xenia_units": "m"
  },
  "Bottom Temp":
  {
    "units": "F",
    "xenia_name": "water_temperature",
    "xenia_units": "celsius"
  },
  "Bottom Salinity":
  {
    "units": "ppt",
    "xenia_name": "salinity",
    "xenia_units": "psu"
  },
  "Bottom DO Conc":
  {
    "units": "mg/L",
    "xenia_name": "oxygen_concentration",
    "xenia_units": "mg_L-1"
  },
  "Bottom DO Saturation":
  {
    "units": "%",
    "xenia_name": "oxygen_concentration",
    "xenia_units": "%"
  },
  "Bottom pH":
  {
    "units": "",
    "xenia_name": "ph",
    "xenia_units": ""
  },
  "Surface Depth":
  {
    "units": "m",
    "xenia_name": "depth",
    "xenia_units": "m"
  },
  "Surface Temp (F)":
  {
    "units": "F",
    "xenia_name": "water_temperature",
    "xenia_units": "celsius"
  },
  "Surface Salinity":
  {
    "units": "ppt",
    "xenia_name": "salinity",
    "xenia_units": "psu"
  },
  "Surface DO Conc":
  {
    "units": "mg/L",
    "xenia_name": "oxygen_concentration",
    "xenia_units": "mg_L-1"
  },
  "Surface DO Saturation":
  {
    "units": "%",
    "xenia_name": "oxygen_concentration",
    "xenia_units": "%"
  },
  "Surface pH":
  {
    "units": "",
    "xenia_name": "ph",
    "xenia_units": ""
  },
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
pier_met_sensors = ["Air Temp","BP","RH","Wind Dir","Wind Speed","Rainfall","Air Temp","Barometric Pressure","Relative Humidity","Wind Direction","W_Air_temp(degF)","W_BP(inHg)","W_Rainfall(in)","W_Rel_humidity(%)","W_Wind_direction(deg)","W_Wind_speed(mph)"]


def get_pier_data(**kwargs):
  logger = logging.getLogger(__name__)
  base_url = kwargs['base_url']
  try:
    request_params = {
      'action': 'Excel',
      'siteId': kwargs['site_id'],
      'sensorId': kwargs['sensor_list'],
      'startDate': kwargs['start_time'],
      'endDate': kwargs['end_time'],
      'displayType': 'StationSensor',
      'predefFlag': 'false',
      'enddateFlag': 'false',
      'now': kwargs['now_time']
    }
    payload_str = "&".join("%s=%s" % (k, v) for k, v in request_params.items())
    logger.debug("Request: %s params: %s" % (base_url, payload_str))
    req = requests.get(base_url, params=payload_str)
    process_records = False
    if req.status_code == 200:
      logger.debug("Request successful, saving to file: %s" % (kwargs['dest_file']))
      with open(kwargs['dest_file'], 'wb') as f:
        for chunk in req.iter_content(1024):
          f.write(chunk)
        return True
    else:
      logger.error("Request failed.")
  except Exception as e:
    logger.exception(e)
  return False

def process_pier_file(file_name, units_convereter, platform_handle, obs_mapping, db):
  logger = logging.getLogger(__name__)

  logger.debug("Opening file: %s" % (file_name))
  wb = open_workbook(filename=file_name)
  sheet = wb.sheet_by_index(0)
  # Get platform info for lat/long
  # plat_rec = db_obj.session.query(platform) \
  #  .filter(platform.platform_handle == platform_handle) \
  #  .one()
  latitude = platform_metadata[platform_handle]['latitude']
  longitude = platform_metadata[platform_handle]['longitude']
  row_entry_date = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
  for row_index in range(sheet.nrows):
    try:
      if row_index > 0:
        # HEader row, add the column index so we can lookup the obs in the worksheet.
        if row_index == 1:
          for col_index in range(sheet.ncols):
            field_name = sheet.cell(row_index, col_index).value
            obs_rec = obs_mapping.get_rec_from_source_name(field_name)
            if obs_rec is not None:
              obs_rec.source_index = col_index
        else:
          # Build the database records.
          m_date_rec = obs_mapping.get_date_field()
          for obs_rec in obs_mapping:
            # Skip the date, not a true obs.
            if obs_rec.target_obs != 'm_date':
              try:
                m_date = sheet.cell(row_index, m_date_rec.source_index).value
                value = float(sheet.cell(row_index, obs_rec.source_index).value)
                if obs_rec.target_uom != obs_rec.source_uom:
                  value = units_convereter.measurementConvert(value, obs_rec.source_uom, obs_rec.target_uom)
                db_rec = sl_multi_obs(row_entry_date=row_entry_date,
                                      platform_handle=platform_handle,
                                      sensor_id=(obs_rec.sensor_id),
                                      m_type_id=(obs_rec.m_type_id),
                                      m_date=m_date,
                                      m_lon=longitude,
                                      m_lat=latitude,
                                      m_value=value
                                      )

                logger.debug("%s Adding m_date: %s obs(%d): %s(%s): %f" % \
                             (platform_handle,
                              db_rec.m_date,
                              db_rec.sensor_id,
                              obs_rec.target_obs,
                              obs_rec.target_uom,
                              db_rec.m_value))
                db.addRec(db_rec, True)

                # input_queue.put(db_rec)
              except ValueError as e:
                logger.error("%s m_date: %s obs(%d): %s(%s) no value" % \
                             (platform_handle,
                              m_date,
                              obs_rec.sensor_id,
                              obs_rec.target_obs,
                              obs_rec.target_uom
                              ))
              except Exception as e:
                logger.exception(e)
    except Exception as e:
      logger.exception(e)


def process_pier_files(platform_name,
                       start_date,
                       end_date,
                       start_data_row,
                       header_row,
                       units_convereter,
                       unique_dates,
                       ini_file):

  logger = logging.getLogger(__name__)
  obs_keys = pier_obs_to_xenia.keys()
  header_list = header_row.split(",")
  row_entry_date = datetime.now()
  eastern_tz = timezone('US/Eastern')
  utc_tz = timezone('UTC')

  config_file = ConfigParser.RawConfigParser()
  config_file.read(ini_file)
  platfrom_parts = platform_name.split('.')
  json_obs_file = config_file.get(platfrom_parts[1], "obs_mapping_file")
  site_id = config_file.get(platfrom_parts[1], 'site_id')
  sensor_list = config_file.get(platfrom_parts[1], 'sensor_ids')
  platform_handle = config_file.get(platfrom_parts[1], 'platform_handle')
  file_directory = config_file.get(platfrom_parts[1], 'file_directory')
  base_url = config_file.get(platfrom_parts[1], 'base_url')
  database_filename = config_file.get('database', 'name')

  db = sl_xeniaAlchemy()
  if db.connectDB('sqlite', None, None, database_filename, None, False):
    logger.info("Succesfully connect to DB: %s" % (database_filename))
  else:
    logger.error("Unable to connect to DB: %s" % (database_filename))

  obs_mapping = json_obs_map()
  obs_mapping.load_json_mapping(json_obs_file)
  obs_mapping.build_db_mappings(platform_handle=platform_handle, sqlite_database_file=database_filename)

  if len(file_directory) == 0:
      now_time = datetime.now()
      now_str = now_time.strftime('%a %b %d %Y %H:%M:%S GMT-0400 (EDT)')
      get_data = True
      #THe sutron site only allows 32767 rows to be exported into the excel file, so we cut up our dates.
      get_date = True
      current_start_date = start_date
      while get_data:
        if current_start_date < end_date:

          stop_date = current_start_date + relativedelta(months=3)

          filepath, filename = os.path.split(file_name)
          filename, ext = os.path.splitext(filename)
          output_filename = os.path.join(filepath, "%s_%s_%s%s" % (filename,
                                                                   current_start_date.strftime("%Y_%m_%d"),
                                                                   stop_date.strftime("%Y_%m_%d"),
                                                                   ext))

          get_pier_data(base_url=base_url,
                         now_time=now_str,
                         start_time=current_start_date.strftime("%Y-%m-%d %H:%M:%S"),
                         end_time=stop_date.strftime("%Y-%m-%d %H:%M:%S"),
                         dest_file=output_filename,
                         site_id=site_id,
                         sensor_list=sensor_list)
          current_start_date = stop_date
          process_pier_file(output_filename, units_convereter, obs_mapping, db)
        else:
          get_data = False

  else:
    file_list = os.listdir(file_directory)
    file_list.sort()
    for file in file_list:
      full_path = os.path.join(file_directory, file)
      process_pier_file(full_path, units_convereter, platform_handle, obs_mapping, db)

  return

def process_pier_old(**kwargs):
  with open(file_name, "rU") as data_file:
    csv_reader = csv.reader(data_file)
    checked_platform_exists = False
    date_time_ndx = date_ndx = time_ndx = None
    if "Date/Time(EST5EDT)" in header_list:
      date_time_ndx = header_list.index('Date/Time(EST5EDT)')
    else:
      date_ndx = header_list.index('Date')
      time_ndx = header_list.index('Time')
    platform_meta = None
    if platform_name in platform_metadata:
      platform_meta = platform_metadata[platform_name]

    if not checked_platform_exists:
      #if xenia_db.platformExists(platform_name) == -1:
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

    for sample_date in unique_dates:
      utc_end_sample_date = (eastern_tz.localize(datetime.strptime(sample_date, '%Y-%m-%d'))).astimezone(utc_tz)
      utc_start_sample_date = utc_end_sample_date - timedelta(hours=24)

      reset_file_pointer = False
      #New date, rewind to beginning of file.
      data_file.seek(0)
      line_num = 0
      check_first_date = True
      for row in csv_reader:
        if line_num >= start_data_row:
          if date_ndx is not None:
            date = row[date_ndx]
            #date format is incosistent, sometimes leadings zeros, other times not.
            month, day, year = date.split('/')
            time = row[time_ndx]
            hour, minute = time.split(':')
            obs_date = eastern_tz.localize(datetime(year=int(year), month=int(month), day=int(day),
                             hour=int(hour), minute=int(minute), second=0))
          else:
            date = row[date_time_ndx]
            obs_date = eastern_tz.localize(datetime.strptime(date, "%Y-%m-%d %H:%M:%S"))

          utc_obs_date = obs_date.astimezone(utc_tz)

          if check_first_date and utc_start_sample_date < utc_obs_date:
            logger.debug("ETCOC date: %s before data date: %s going to next sample date." % (utc_start_sample_date, utc_obs_date))
            break
          check_first_date = False

          logger.debug("ETCOC date: %s Obs Date: %s" % (utc_start_sample_date, utc_obs_date))

          if utc_obs_date >= utc_start_sample_date and utc_obs_date < utc_end_sample_date:
            logger.debug("Platform: %s Obs Date: %s ETCOC Begin Date: %s ETCOC End Date: %s "\
                         % (platform_name, utc_obs_date, utc_start_sample_date, utc_end_sample_date))
          #if utc_start_date is not None and utc_obs_date < utc_start_date:
          #  continue


            found_obs = []
            for ndx, header_key in enumerate(header_list):
              matched_obs = None
              clean_hdr_key = header_key.lower().strip()
              s_order = 1
              has_surface_bottom_text = False
              for obs_key in obs_keys:
                if clean_hdr_key == 'date' or clean_hdr_key == 'time'\
                  or clean_hdr_key == 'date/time(est5edt)':
                  break
                #if clean_hdr_key.find(obs_key.lower().strip()) != -1:
                if clean_hdr_key == obs_key.lower():
                  if clean_hdr_key.find('surface') != -1 and clean_hdr_key.find(obs_key.lower()) != -1:
                    matched_obs = obs_key
                    has_surface_bottom_text = True
                  elif clean_hdr_key.find('bottom') != -1 and clean_hdr_key.find(obs_key.lower()) != -1:
                    s_order = 2
                    matched_obs = obs_key
                    has_surface_bottom_text = True
                  else:
                    matched_obs = obs_key


                if matched_obs is not None:
                  #Files that don't use Surface or Bottom indicators, this is how we differentiate
                  #as bottom are always first in header list.
                  if not has_surface_bottom_text and obs_key not in pier_met_sensors:
                    obs_info = pier_obs_to_xenia[obs_key]
                    if obs_info['xenia_name'] not in found_obs:
                      s_order = 2
                      found_obs.append(obs_info['xenia_name'])

                  if len(row[ndx]):
                    obs_info = pier_obs_to_xenia[obs_key]
                    obs_val = float(row[ndx])
                    if obs_info['units'] != obs_info['xenia_units']:
                      obs_val = units_convereter.measurementConvert(obs_val, obs_info['units'], obs_info['xenia_units'])
                    logger.debug("Row: %d[%s] Adding obs: %s(%s)[%d] Date: %s Value: %s " %\
                                 (line_num, header_key, obs_info['xenia_name'], obs_info['xenia_units'], s_order, utc_obs_date.strftime("%Y-%m-%d %H:%M:%S"), obs_val))
                    try:
                      if not xenia_db.addMeasurement(obs_info['xenia_name'],
                                              obs_info['xenia_units'],
                                              platform_name,
                                              utc_obs_date.strftime('%Y-%m-%dT%H:%M:%S'),
                                              platform_meta['latitude'],
                                              platform_meta['longitude'],
                                              0,
                                              [obs_val],
                                              sOrder=s_order,
                                              autoCommit=True,
                                              rowEntryDate=row_entry_date ):
                        logger.error(xenia_db.lastErrorMsg)
                    except Exception as e:
                      logger.exception(e)
                    break
                  else:
                    logger.debug("Row: %d Obs: %s no data found." % (line_num, matched_obs))
                    break
              if matched_obs is None:
                logger.error("Row: %d Could not match obs: %s" % (line_num, header_key))

            if not reset_file_pointer:
              reset_file_pointer = True
          else:
            #OUt of the date ranges we are interested in.
            if reset_file_pointer:
              break

        line_num += 1

  return

def process_sun2_files(platform_name,
                       file_name,
                       start_data_row,
                       header_row,
                       units_converter,
                       xenia_db,
                       utc_start_date
                       ):

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
def process_sun2_data(platform_handle,
                       units_converter,
                       xenia_db,
                       unique_dates,
                       ini_file):

  logger = logging.getLogger(__name__)

  config_file = ConfigParser.RawConfigParser()
  config_file.read(ini_file)

  json_obs_file = config_file.get(platform_handle.split('.')[1], 'obs_mapping_file')
  database_filename = config_file.get('database', 'name')

  obs_mapping = json_obs_map()
  obs_mapping.load_json_mapping(json_obs_file)
  obs_mapping.build_db_mappings(platform_handle=platform_handle, sqlite_database_file=database_filename)


  utc_tz = timezone('UTC')
  eastern_tz= timezone('US/Eastern')
  #http://cormp.org/data.php?format=json&platform=sun2&time=2009-01-01T00:00:00/2009-01-02T02:06:02
  base_url = "http://services.cormp.org/data.php?format=json&platform={platform_name}&time={start_time}/{end_time}"

  row_entry_date = datetime.now()
  '''
  sun2_to_xenia = {
    "sea_water_practical_salinity": {
      "units": "psu",
      "xenia_name": "salinity",
      "xenia_units": "psu"

    },
    "sea_water_temperature": {
      "units": "celsius",
      "xenia_name": "water_temperature",
      "xenia_units": "celsius"
    },
    "wind_speed": {
      "units": "m_s-1",
      "xenia_name": "wind_speed",
      "xenia_units": "m_s-1"

    },
    "wind_from_direction": {
      "units": "degrees_true",
      "xenia_name": "wind_from_direction",
      "xenia_units": "degrees_true"

    }
  }
  
  s_order = 1
  obs_list = []
  for obs_key in sun2_to_xenia:
    obs_info = sun2_to_xenia[obs_key]
    obs_list.append({'obs_name': obs_info['xenia_name'],
                     'uom_name': obs_info['xenia_units'],
                     's_order': s_order})
  xenia_db.buildMinimalPlatform(platform_handle, obs_list)
  '''
  platform_parts = platform_handle.split('.')
  for start_date in unique_dates:
    utc_start_date = (eastern_tz.localize(datetime.strptime(start_date, '%Y-%m-%d'))).astimezone(utc_tz)
    start_date = utc_start_date - timedelta(hours=24)

    logger.debug("Platform: %s Begin Date: %s End Date: %s" % (platform_handle, start_date, utc_start_date))

    url = base_url.format(platform_name=platform_parts[1].lower(), start_time=start_date.strftime('%Y-%m-%d %H:%M:%S'),
                          end_time=utc_start_date.strftime('%Y-%m-%d %H:%M:%S'))

    try:
      req = requests.get(url)
      if req.status_code == 200:
        time_series = req.json()
        parameters = time_series['properties']['parameters']
        quality_decode = time_series['properties']['quality_levels']

        for param in parameters:
          xenia_obs = obs_mapping.get_rec_from_source_name(param['id'])
          '''
          if param['id'] == 'sea_water_temperature':
            obs_type = sun2_to_xenia[param['id']]['xenia_name']
            uom_type = 'celsius'
          '''
          if xenia_obs is not None:
            try:
              observations = param['observations']
              for ndx, obs_val in enumerate(observations['values']):
                try:
                  obs_val = float(obs_val)
                except ValueError as e:
                  logger.exception(e)
                else:
                  logger.debug("Adding obs: %s(%s) Date: %s Value: %s S_Order: %d" % \
                               (xenia_obs.target_obs, xenia_obs.target_uom, observations['times'][ndx], obs_val, xenia_obs.s_order))
                  xenia_db.addMeasurement(xenia_obs.target_obs,
                                          xenia_obs.target_uom,
                                          platform_handle,
                                          observations['times'][ndx],
                                          platform_metadata[platform_handle]['latitude'],
                                          platform_metadata[platform_handle]['longitude'],
                                          0,
                                          [obs_val],
                                          sOrder=xenia_obs.s_order,
                                          autoCommit=True,
                                          rowEntryDate=row_entry_date)
            except Exception as e:
              logger.exception(e)
    except Exception as e:
      logger.exception(e)

    '''
    data_time = '%s/%s' % (start_date.strftime('%Y-%m-%dT%H:%M:%S'), utc_start_date.strftime('%Y-%m-%dT%H:%M:%S'))
    params = {
      'format': 'json',
      'platform': platform_name_parts[1].lower(),
      'time': data_time
    }
    try:
      result = requests.get(url, params=params)
      logger.debug("URL Request: %s" % (result.url))
    except Exception as e:
      logger.exception(e)
    else:
      if result.status_code == 200:
        try:
          json_data = json.loads(result.text)
        except Exception as e:
          logger.exception(e)
        else:
          coords = json_data['geometry']['coordinates']
          parameters = json_data['properties']['parameters']
          for param in parameters:
            obs_type = None
            uom_type = None
            s_order = 1
            """
            if param['id'] == 'wind_speed':
              obs_type = param['id']
              uom_type = 'm_s-1'
            elif param['id'] == 'wind_from_direction':
              obs_type = param['id']
              uom_type = 'degrees_true'
            elif param['id'] == 'sea_water_temperature':
              obs_type = param['id']
              uom_type = 'celsius'
            elif param['id'] == 'sea_water_practical_salinity':
              obs_type = 'salinity'
              uom_type = 'psu'
            """
            if param['id'] == 'sea_water_temperature':
              obs_type = sun2_to_xenia[param['id']]['xenia_name']
              uom_type = 'celsius'
            if obs_type is not None:
              try:
                observations = param['observations']
                for ndx, obs_val in enumerate(observations['values']):
                  try:
                    obs_val = float(obs_val)
                  except ValueError as e:
                    logger.exception(e)
                  else:
                    logger.debug("Adding obs: %s(%s) Date: %s Value: %s S_Order: %d" %\
                                 (obs_type, uom_type, observations['times'][ndx], obs_val, s_order))
                    xenia_db.addMeasurement(obs_type,
                                            uom_type,
                                            platform_handle,
                                            observations['times'][ndx],
                                            float(coords[1]),
                                            float(coords[0]),
                                            0,
                                            [obs_val],
                                            sOrder=s_order,
                                            autoCommit=True,
                                            rowEntryDate=row_entry_date )
              except Exception as e:
                logger.exception(e)

      else:
        logger.ERROR("Request failed with code: %d" % (result.status_code))
    '''
def process_nos8661070_data(platform_handle,
                       units_converter,
                       xenia_db,
                       unique_dates):

  logger = logging.getLogger(__name__)
  utc_tz = timezone('UTC')
  eastern_tz= timezone('US/Eastern')
  row_entry_date = datetime.now()

  platform_name_parts = platform_handle.split('.')
  """
  Create a data collection object.
  Contructor parameters are:
    url - THe SWE endpoint we're interested in
    version - Optional default is '1.0.0' The SWE version the endpoint.
    xml - Optional default is None - The XML response from a GetCapabilities query of the server.
  """
  dataCollector = CoopsSos()
  """
  obs_list = ['http://mmisw.org/ont/cf/parameter/water_surface_height_above_reference_datum',
            'http://mmisw.org/ont/cf/parameter/sea_water_temperature',
            'http://mmisw.org/ont/cf/parameter/wind_speed',
            'http://mmisw.org/ont/cf/parameter/wind_from_direction']
  obs_list = [('water_surface_height_above_reference_datum', 'm'),
             ('sea_water_temperature', 'celsius'),
             ('wind_speed', 'm_s-1'),
              ('wind_from_direction', 'degrees_true')]
  """
  nos_to_xenia = {
    "water_surface_height_above_reference_datum": {
      "units": "m",
      "xenia_name": "water_level",
      "xenia_units": "m"

    },
    "sea_water_temperature": {
      "units": "celsius",
      "xenia_name": "water_temperature",
      "xenia_units": "celsius"
    },
    "wind_speed": {
      "units": "m_s-1",
      "xenia_name": "wind_speed",
      "xenia_units": "m_s-1"

    },
    "wind_from_direction": {
      "units": "degrees_true",
      "xenia_name": "wind_from_direction",
      "xenia_units": "degrees_true"

    }
  }
  #nos_obs = nos_to_xenia.keys()
  nos_obs = ['wind_speed', 'wind_from_direction']
  if xenia_db.platformExists(platform_handle) == -1:
    s_order = 1
    obs_list = []
    for obs_key in nos_to_xenia:
      obs_info = nos_to_xenia[obs_key]
      obs_list.append({'obs_name': obs_info['xenia_name'],
                       'uom_name': obs_info['xenia_units'],
                       's_order': s_order})
    xenia_db.buildMinimalPlatform(platform_handle, obs_list)
  for start_date in unique_dates:
    utc_start_date = (eastern_tz.localize(datetime.strptime(start_date, '%Y-%m-%d'))).astimezone(utc_tz)
    start_date = utc_start_date - timedelta(hours=24)
    logger.debug("Platform: %s Begin Date: %s End Date: %s" % (platform_handle, start_date, utc_start_date))
    for single_obs in nos_obs:
      obs_type = nos_to_xenia[single_obs]['xenia_name']
      uom_type = nos_to_xenia[single_obs]['xenia_units']
      s_order = 1
      dataCollector.filter(features=['8661070'],
                           variables=[single_obs],
                           start=start_date,
                           end=utc_start_date)
      try:
        response = dataCollector.raw(responseFormat="text/csv").decode()
      except Exception as e:
        logger.exception(e)
      else:
        csv_reader = csv.reader(response.split('\n'), delimiter=',')
        line_cnt = 0
        for row in csv_reader:
          if line_cnt > 0 and len(row):
            obs_date = datetime.strptime(row[4], '%Y-%m-%dT%H:%M:%SZ')
            obs_val = float(row[5])
            logger.debug("Adding obs: %s(%s) Date: %s Value: %s S_Order: %d" %\
                         (single_obs, uom_type, obs_date, obs_val, s_order))
            try:
              xenia_db.addMeasurement(obs_type,
                                      uom_type,
                                      platform_handle,
                                      obs_date.strftime('%Y-%m-%dT%H:%M:%S'),
                                      float(row[2]),
                                      float(row[3]),
                                      0,
                                      [obs_val],
                                      sOrder=s_order,
                                      autoCommit=True,
                                      rowEntryDate=row_entry_date)
                #logger.error(xenia_db.lastErrorMsg)
            except Exception as e:
              logger.exception(e)
          line_cnt += 1

def build_tide_data_file(tide_output_file, unique_dates, log_conf_file):
  eastern_tz = timezone('US/Eastern')
  tide_dates = []
  for date_rec in unique_dates:
    tide_date = eastern_tz.localize(datetime.strptime(date_rec, '%Y-%m-%d'))
    tide_date = tide_date.replace(hour=0, minute=0, second=0)
    tide_dates.append(tide_date)

  create_tide_data_file_mp('8661070',
                           tide_dates,
                           tide_output_file,
                           1,
                           log_conf_file,
                           True)


def main():
  parser = optparse.OptionParser()
  parser.add_option( "--ConfigFile", dest="config_file",
                    help="INI Configuration file." )
  parser.add_option("--GetPierData", dest="get_pier_data", action="store_true", default=False,
                    help="" )
  parser.add_option("--GetOldPierData", dest="get_old_pier_data", action="store_true", default=False,
                    help="" )
  parser.add_option("--GetSUN2", dest="get_sun2_data", action="store_true", default=False,
                    help="" )
  parser.add_option("--GetNOS", dest="get_nos_data", action="store_true", default=False,
                    help="" )
  parser.add_option("--Header", dest="header_row",
                    help="" )
  parser.add_option("--FirstDataRow", dest="first_data_row",
                    help="" )
  parser.add_option("--PlatformName", dest="platform_name",
                    help="" )
  parser.add_option("--ETCOCFile", dest="etcoc_file", default=None,
                    help="" )
  parser.add_option("--StartDate", dest="start_date", default=None,
                    help="" )
  parser.add_option("--EndDate", dest="end_date", default=None,
                    help="" )
  parser.add_option("--TideOutFile", dest="tide_output_file", default="",
                    help="")


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

  if logger:
    logger.info("Building unique sample dates list.")


  #Get the unique dates.
  unique_dates = []
  fields = ['station', 'date', 'etcoc']
  with open(options.etcoc_file, "r") as etcoc_file:
    etcoc_read = csv.DictReader(etcoc_file, fieldnames=fields)
    for row in etcoc_read:
      date_only, time_only = row['date'].split(' ')
      if date_only not in unique_dates:
        unique_dates.append(date_only)
  unique_dates.sort()

  historical_db = config_file.get("historical_database", "name")
  try:
    xenia_db = wqDB(historical_db, __name__)
  except Exception as e:
    if logger:
      logger.exception(e)

  utc_start_date = None
  if options.start_date is not None:
    utc_tz = timezone('UTC')
    eastern_tz= timezone('US/Eastern')
    utc_end_date = None
    if options.end_date is not None:
      utc_end_date = timezone('US/Eastern').localize(datetime.strptime(options.end_date, '%Y-%m-%d %H:%M:%S')).astimezone(timezone('UTC'))

    start_dates = []

    utc_start_date = timezone('US/Eastern').localize(datetime.strptime(options.start_date, '%Y-%m-%d %H:%M:%S')).astimezone(timezone('UTC'))
    for sample_date in unique_dates:
      utc_sample_date = (eastern_tz.localize(datetime.strptime(sample_date, '%Y-%m-%d'))).astimezone(utc_tz)
      add_date = False
      if utc_end_date is None:
        if utc_sample_date >= utc_start_date:
          add_date = True
      else:
        if utc_sample_date >= utc_start_date and utc_sample_date < utc_end_date:
          add_date = True
      if add_date:
        start_dates.append(sample_date)
      unique_dates = start_dates
  if options.get_old_pier_data:
    i=0
  if options.get_pier_data:
    process_pier_files(options.platform_name,
                       utc_start_date,
                       utc_end_date,
                       int(options.first_data_row),
                       options.header_row,
                       units_conversion,
                       unique_dates,
                       options.config_file)
  elif options.get_sun2_data:
    process_sun2_data(options.platform_name,
                       units_conversion,
                       xenia_db,
                       unique_dates,
                      options.config_file)
  elif options.get_nos_data:
    process_nos8661070_data(options.platform_name,
                       units_conversion,
                       xenia_db,
                       unique_dates)
  elif options.tide_output_file:
    build_tide_data_file(options.tide_output_file, unique_dates, logConfFile)

  if logger:
    logger.info("Log closed.")
if __name__ == "__main__":
  main()
