import requests
import json

"""
url = "https://gis.dhec.sc.gov/beachaccess/#"

#driver = webdriver.Firefox()
driver = webdriver.PhantomJS(executable_path='/usr/local/Cellar/phantomjs/2.1.1/bin/phantomjs')

driver.get(url)

stations = driver.execute_script("return beachStationsDB;")
advisories = driver.execute_script("return beachAdvisoriesDB;")
feature_set = driver.execute_script("return ;")
request_cookies = {}
for cookie in driver.get_cookies():
  request_cookies[cookie['name']] = cookie['value']
#url = "https://gis.dhec.sc.gov/arcgis/rest/services/environment/SwimmingAdvisories/MapServer"
#url = "https://gis.dhec.sc.gov/beachservice/beachService.asmx/GetBeachCount"
#req = requests.get(url)
session = requests.Session()

url = "https://gis.dhec.sc.gov/beachservice/beachService.asmx/GetAdvisory"
req = session.get(url, cookies=request_cookies)
"""
url = "https://gis.dhec.sc.gov/arcgis/rest/services/environment/BeachMonitoring/MapServer/1/query?f=json&where=1%3D1&returnGeometry=true&spatialRel=esriSpatialRelIntersects&outFields=*"
req = requests.get(url)
recs = json.loads(req.content)
print req.content