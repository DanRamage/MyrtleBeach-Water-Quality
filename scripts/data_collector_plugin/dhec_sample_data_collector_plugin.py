import sys
sys.path.append('../../commonfiles/python')
import logging.config
from data_collector_plugin import data_collector_plugin
import ConfigParser
import traceback
import geojson

from dhecBeachAdvisoryReader import waterQualityAdvisory

class nexrad_collector_plugin(data_collector_plugin):

  def initialize_plugin(self, **kwargs):
    try:
      plugin_details = kwargs['details']
      self.ini_file = plugin_details.get('Settings', 'ini_file')
      return True
    except Exception as e:
      self.logger.exception(e)
    return False

  def run(self):
    try:
      configFile = ConfigParser.RawConfigParser()
      configFile.read(self.ini_file)

      logger = None
      logConfFile = configFile.get('logging', 'scraperConfigFile')
      if(logConfFile):
        logging.config.fileConfig(logConfFile)
        logger = logging.getLogger("dhec_beach_advisory_app")
        logger.info("Log file opened.")
    except ConfigParser.Error, e:
      print("No log configuration file given, logging disabled.")
    except Exception,e:
      import traceback
      traceback.print_exc(e)
      sys.exit(-1)
    try:
      #Base URL to the page that house an individual stations results.
      baseUrl = configFile.get('websettings', 'baseAdvisoryPageUrl')

      #output Filename for the JSON data.
      jsonFilepath = configFile.get('output', 'outputDirectory')

      #Filepath to the geoJSON file that contains the station data for all the stations.
      stationGeoJsonFile = configFile.get('stationData', 'stationGeoJsonFile')

      #The past WQ results.
      stationWQHistoryFile = configFile.get('stationData', 'stationWQHistoryFile')

      dhec_rest_url = configFile.get('websettings', 'dhec_rest_url')
    except ConfigParser.Error, e:
      if(logger):
        logger.exception(e)

    else:
      try:
        advisoryObj = waterQualityAdvisory(baseUrl, True)
        #See if we have a historical WQ file, if so let's use that as well.
        historyWQFile = open(stationWQHistoryFile, "r")
        historyWQ = geojson.load(historyWQFile)

        advisoryObj.processData(stationGeoJsonFile, jsonFilepath, historyWQ, dhec_rest_url)
      except (IOError,Exception) as e:
        if(logger):
          logger.exception(e)

    return
