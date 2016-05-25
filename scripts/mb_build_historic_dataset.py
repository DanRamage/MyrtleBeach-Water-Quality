import sys
sys.path.append('../commonfiles/python')
import os

import logging.config
from datetime import datetime, timedelta
from pytz import timezone
import traceback

import optparse
import ConfigParser
from collections import OrderedDict
from mako.template import Template
from mako import exceptions as makoExceptions
import simplejson as json
import csv
from wq_prediction_tests import wqEquations
from enterococcus_wq_test import EnterococcusPredictionTest,EnterococcusPredictionTestEx

from mb_wq_data import mb_wq_model_data, mb_sample_sites
from wq_results import _resolve, results_exporter


def main():
  parser = optparse.OptionParser()
  parser.add_option("-c", "--ConfigFile", dest="config_file",
                    help="INI Configuration file." )
  parser.add_option("-y", "--ETCOCFile", dest="etcoc_file",
                    help="" )
  (options, args) = parser.parse_args()

  config_file = ConfigParser.RawConfigParser()
  config_file.read(options.config_file)

  #Get the unique dates.
  unique_dates = []
  fields = ['station', 'date', 'etcoc']
  with open(options.etcoc_file, "r") as etcoc_file:
    etcoc_read = csv.DictReader(etcoc_file, fieldnames=fields)
    for row in etcoc_read:
      if row['date'] not in unique_dates:
        unique_dates.append(row['date'])
  return

if __name__ == "__main__":
  main()