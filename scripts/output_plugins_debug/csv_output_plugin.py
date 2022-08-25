import sys
sys.path.append('../../commonfiles/python')
import logging.config
from output_plugin import output_plugin

class csv_output_plugin(output_plugin):
  def __init__(self):
    output_plugin.__init__(self)
    self.logger = logging.getLogger(__name__)

  def initialize_plugin(self, **kwargs):
    try:
      details = kwargs['details']

      self.csv_outfile = details.get("Settings", "csv_outfile")
      return True
    except Exception as e:
      self.logger.exception(e)
    return False

  def emit(self, **kwargs):
    if self.logger:
      self.logger.debug("Starting emit for csv output.")

    ensemble_data = kwargs['ensemble_tests']
    for rec in ensemble_data:
      try:
        site_metadata = rec['metadata']
        file_name = self.csv_outfile % (site_metadata.name)
        with open(file_name, 'a') as csv_output_file:
          test_results = rec['models']
          entero_val = rec['entero_value']
          if entero_val is None:
            entero_val = ''
          for test in test_results.tests:
            try:
              result = ''
              if test.result is not None:
                result = str(test.result)
              csv_output_file.write('%s,%s,%s,%s,%s\n' % (kwargs['prediction_date'],
                                                      site_metadata.name,
                                                      test.model_name,
                                                      result,
                                                      entero_val))
            except Exception as e:
              if self.logger:
                self.logger.exception(e)
      except IOError as e:
        if self.logger:
          self.logger.exception(e)
    if self.logger:
      self.logger.debug("Finished emit for csv output.")
    return