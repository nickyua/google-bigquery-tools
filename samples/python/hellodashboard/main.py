#!/usr/bin/env python
#
# Copyright 2012 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
import bqclient
import httplib2
import logging
import os
from django.utils import simplejson as json
from google.appengine.api import memcache
from google.appengine.ext import webapp
from google.appengine.ext.webapp.util import run_wsgi_app
from google.appengine.ext.webapp.template import render
from oauth2client.appengine import oauth2decorator_from_clientsecrets

# CLIENT_SECRETS, name of a file containing the OAuth 2.0 information for this
# application, including client_id and client_secret, which are found
# on the API Access tab on the Google APIs
# Console <http://code.google.com/apis/console>
CLIENT_SECRETS = os.path.join(os.path.dirname(__file__), 'client_secrets.json')

# BILLING_PROJECT_ID for a project where you and your users
#   are viewing members.  This is where the bill will be sent.
#   During the limited availability preview, there is no bill.
# Replace the BILLING_PROJECT_ID value with the Client ID value
# from your project, the same numeric value you used in client_secrets.json
BILLING_PROJECT_ID = "99999999999"
DATA_PROJECT_ID = "publicdata"
DATASET = "samples"
TABLE = "natality"
QUERY = """
select state, SUM(gestation_weeks) / COUNT(gestation_weeks) as weeks 
from publicdata:samples.natality 
where year > 1990 and year < 2005 and IS_EXPLICITLY_DEFINED(gestation_weeks) 
group by state order by weeks
"""

decorator = oauth2decorator_from_clientsecrets(CLIENT_SECRETS,
    'https://www.googleapis.com/auth/bigquery')

http = httplib2.Http(memcache)

bq = bqclient.BigQueryClient(http, decorator)

class MainHandler(webapp.RequestHandler):
    def _bq2geo(self, bqdata):
        """geodata output for region maps must be in the format region, value.
           Assume the BigQuery query output is in this format and get names from schema.
        """
        logging.info(bqdata)
        columnNameGeo = bqdata['schema']['fields'][0]['name']
        columnNameVal = bqdata['schema']['fields'][1]['name']
        logging.info("Column Names=%s, %s" % (columnNameGeo, columnNameVal))
        geodata = { 'cols': ({'id':columnNameGeo, 'label':columnNameGeo, 'type':'string'},
          {'id':columnNameVal, 'label':columnNameVal, 'type':'number'})}
        geodata['rows'] = [];
        logging.info(geodata)
        for row in bqdata['rows']:
            newrow = ({'c':[]})
            newrow['c'].append({'v': 'US-'+row['f'][0]['v']})
            newrow['c'].append({'v':row['f'][1]['v']})
            geodata['rows'].append(newrow)
        logging.info('FINAL GEODATA---')
        logging.info(geodata)
        return json.dumps(geodata)

    @decorator.oauth_required
    def get(self):
        logging.info('Last mod time: %s' % bq.getLastModTime(
            DATA_PROJECT_ID, DATASET, TABLE))
        data = { 'data': self._bq2geo(bq.Query(QUERY, BILLING_PROJECT_ID)),
                 'query': QUERY }
        template = os.path.join(os.path.dirname(__file__), 'index.html')
        self.response.out.write(render(template, data))

application = webapp.WSGIApplication([
   ('/', MainHandler),
], debug=True)

def main():
   run_wsgi_app(application)

if __name__ == '__main__':
    main()
