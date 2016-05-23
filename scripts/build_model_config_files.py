import sys
sys.path.append('../commonfiles/python')
import os
import logging.config
import csv
import glob
import optparse
import ConfigParser
import traceback
from mb_wq_data import mb_sample_sites


def main():
  parser = optparse.OptionParser()
  parser.add_option("-c", "--ConfigFile", dest="config_file",
                    help="INI Configuration file." )
  parser.add_option("-d", "--DestinationDirectory", dest="dest_dir",
                    help="Destination directory to write the config files." )
  parser.add_option("-m", "--ModelCSVFileDirectory", dest="model_csv_dir",
                    help="Directory where the CSV files defining the models are located." )

  (options, args) = parser.parse_args()

  logger = None

  if(options.config_file is None):
    parser.print_help()
    sys.exit(-1)

  try:
    config_file = ConfigParser.RawConfigParser()
    config_file.read(options.config_file)

    logger = None
    logConfFile = config_file.get('logging', 'prediction_engine')
    if logConfFile:
      logging.config.fileConfig(logConfFile)
      logger = logging.getLogger('mb_wq_predicition_logger')
      logger.info("Log file opened.")

  except ConfigParser.Error, e:
    traceback.print_exc(e)
    sys.exit(-1)

  try:
    boundaries_location_file = config_file.get('boundaries_settings', 'boundaries_file')
    sites_location_file = config_file.get('boundaries_settings', 'sample_sites')
  except ConfigParser.Error,e:
    if logger:
      logger.exception(e)
  else:
    #Load the sample site information. Has name, location and the boundaries that contain the site.
    mb_sites = mb_sample_sites(True)
    mb_sites.load_sites(file_name=sites_location_file, boundary_file=boundaries_location_file)
    #Build watershed groups.
    watersheds = {}
    for site in mb_sites:
      if site.contained_by[0] is not None:
        if site.contained_by[0].name.lower() not in watersheds:
          watersheds[site.contained_by[0].name.lower()] = []
        watersheds[site.contained_by[0].name.lower()].append(site.name)

    VB_FUNCTIONS_REPLACE = [
      ("POLY", "VB_POLY"),
      ("QUADROOT", "VB_QUADROOT"),
      ("SQUAREROOT", "VB_SQUAREROOT"),
      ("INVERSE", "VB_INVERSE"),
      ("SQUARE", "VB_SQUARE"),
      ("WindO_comp", "VB_WindO_comp"),
      ("WindA_comp", "VB_WindA_comp"),
      ("LOG10", "VB_LOG10")
    ]
    model_csv_list = glob.glob('%s/*.csv' % (options.model_csv_dir))
    start_line = 1
    header_row = [
      "Location",
      "Site",
      "Equation"
    ]

    for model_csv_file in model_csv_list:
      with open(model_csv_file, "rU") as model_file_obj:
        path, filename_ext = os.path.split(model_csv_file)
        filename, ext = os.path.splitext(filename_ext)
        model_file_reader = csv.DictReader(model_file_obj, delimiter=',', quotechar='"', fieldnames=header_row)
        line_num = 0
        current_watershed = None
        sites = None
        sites_list = []
        for row in model_file_reader:
          #Actually model lines start multiple lines into the file.
          if line_num >= start_line:
            location = row['Location'].lower()
            if current_watershed is None or current_watershed != location:
              sites = watersheds[location]
            site = row['Site']
            if site in sites:
              sites.remove(site)
              sites_list.append(site)
            else:
              sites_list = sites
            for sample_site in sites_list:
              logger.info("Creating model config for: %s" % (sample_site))
              model_config_parser = ConfigParser.ConfigParser()
              model_config_outfile_name = os.path.join(options.dest_dir, '%s.ini' % (sample_site.lower()))
              try:
                #Check to see if the model ini file exists.
                ini_exists = False
                model_num = 1
                if os.path.isfile(model_config_outfile_name):
                  model_config_parser.read(model_config_outfile_name)
                  ini_exists = True
                  model_num = model_config_parser.getint("settings", "model_count") + 1
                with open(model_config_outfile_name, 'w') as model_config_ini_obj:

                  if not ini_exists:
                    model_config_parser.add_section("settings")

                  formula_string = row['Equation']

                  no_log_10 = False
                  if formula_string.find('etcoc =') != -1:
                    no_log_10 = True
                  formula_string = formula_string.replace('[', '(')
                  formula_string = formula_string.replace(']', ')')

                  formula_string = formula_string.replace('radar_rain_summary', '%s_nexrad_summary' % (location))
                  formula_string = formula_string.replace('radar_rainfall_intensity_24', '%s_nexrad_rainfall_intensity' % (location))
                  formula_string = formula_string.replace('radar_rainfall', '%s_nexrad_rainfall' % (location))
                  formula_string = formula_string.replace('radar_preceding_dry_day_cnt', '%s_nexrad_dry_days_count' % (location))
                  formula_string = formula_string.replace('radar_rain_total_one_day_delay', '%s_nexrad_total_1_day_delay' % (location))
                  formula_string = formula_string.replace('radar_rain_total_two_day_delay', '%s_nexrad_total_2_day_delay' % (location))
                  formula_string = formula_string.replace('radar_rain_total_three_day_delay', '%s_nexrad_total_3_day_delay' % (location))
                  formula_string = formula_string.replace('range', 'tide_range_8661070')
                  if no_log_10:
                    formula_string = formula_string.replace('etcoc = ', '')
                  else:
                    formula_string = formula_string.replace('LOG10(etcoc) =', '')
                  formula_string = 'Pow(10, %s)' % (formula_string)
                  for vb_replacement in VB_FUNCTIONS_REPLACE:
                    formula_string = formula_string.replace(vb_replacement[0], vb_replacement[1])

                  model_section = "model_%d" % model_num
                  model_name = "%s-%s" % (sample_site, filename)
                  model_config_parser.add_section(model_section)
                  model_config_parser.set(model_section, 'name', model_name)
                  model_config_parser.set(model_section, 'formula', formula_string)
                  model_config_parser.set("settings", "model_count", model_num)
                  model_config_parser.write(model_config_ini_obj)

              except IOError, e:
                if logger:
                  logger.exception(e)
            del sites_list[:]
          line_num += 1

  logger.info("Log file closed.")

  return


if __name__ == "__main__":
  main()
