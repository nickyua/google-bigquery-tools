#!/usr/bin/env python
#
# Copyright 2007 Google Inc.
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
"""Starting template for Google App Engine applications.

Use this project as a starting point if you are just beginning to build a Google
App Engine project. Remember to download the OAuth 2.0 client secrets which can
be obtained from the Developer Console <https://code.google.com/apis/console/>
and save them as 'client_secrets.json' in the project directory.
"""

__author__ = 'jcgregorio@google.com (Joe Gregorio)'


import cgi
import httplib2
import logging
import os
import pickle
import re

from apiclient.discovery import build
from apiclient.errors import HttpError
from bigqueryv2 import BigQueryClient
from oauth2client.appengine import oauth2decorator_from_clientsecrets
from oauth2client.client import AccessTokenRefreshError
from google.appengine.api import memcache
from google.appengine.ext import webapp
from google.appengine.ext.webapp import template
from google.appengine.ext.webapp.util import run_wsgi_app

# Project ID for your BigQuery Project in the API Console
PROJECT_ID = 'MAKE_THIS_YOURS'

# Change these tables to match your own versions.
# Format should be similar to 'author':'dataset.table'
#     where 'dataset' is the name of the dataset holding
#                     the author, title, and genre tables
#     and   'table'   is the name of the table holding the corresponding data.
tables = {'author':'MAKE_THIS_YOURS',
          'title': 'MAKE_THIS_YOURS',
          'genre': 'MAKE_THIS_YOURS'}

# CLIENT_SECRETS, name of a file containing the OAuth 2.0 information for this
# application, including client_id and client_secret, which are found
# on the API Access tab on the Google APIs
# Console <http://code.google.com/apis/console>
CLIENT_SECRETS = os.path.join(os.path.dirname(__file__), 'client_secrets.json')

# Helpful message to display in the browser if the CLIENT_SECRETS file
# is missing.
MISSING_CLIENT_SECRETS_MESSAGE = """
    <h1>Warning: Please configure OAuth 2.0</h1>
    <p>
    To make this sample run you will need to populate the
    client_secrets.json file found at:
    </p>
    <p>
    <code>%s</code>.
    </p>
    <p>with information found on the <a
    href="https://code.google.com/apis/console">APIs Console</a>.
    </p>
    """ % CLIENT_SECRETS

http = httplib2.Http(memcache)
service = build("plus", "v1", http=http)
decorator = oauth2decorator_from_clientsecrets(
  CLIENT_SECRETS,
  'https://www.googleapis.com/auth/plus.me https://www.googleapis.com/auth/bigquery',
  MISSING_CLIENT_SECRETS_MESSAGE)

# Create the BigQuery client
bq = BigQueryClient(cleanhttp=http, projectID=PROJECT_ID)

class MainHandler(webapp.RequestHandler):

  @decorator.oauth_aware
  def get(self):
    path = os.path.join(os.path.dirname(__file__), 'grant.html')
    variables = {
        'url': decorator.authorize_url(),
        'has_credentials': decorator.has_credentials()
        }
    self.response.out.write(template.render(path, variables))


class AboutHandler(webapp.RequestHandler):

  @decorator.oauth_required
  def get(self):
    try:
      decoratedhttp = decorator.http()
      user = service.people().get(userId='me').execute(decoratedhttp)
      text = 'Hello, %s!<br>' % user['displayName']
      image = ""
      if user['image'] and user['image']['url']:
        image = '<img src="%s">\n' % user['image']['url']
      text += image
      text += "<p>This is a little example of using two APIs (BigQuery and Google+) at the same time</p>"

      path = os.path.join(os.path.dirname(__file__), 'welcome.html')
      self.response.out.write(template.render(path, {'text': text }))

    except AccessTokenRefreshError:
      self.redirect('/')

  @decorator.oauth_required
  def post(self):
    try:
      decoratedhttp = decorator.http()
      user = service.people().get(userId='me').execute(decoratedhttp)
      text = '%s, here are your results:<br>' % user['displayName']

      author = self.request.get('author')
      title = self.request.get('title')
      genre = self.request.get('genre')
      rank = self.request.get('rank')
      ignorecase = self.request.get('ignorecase')

      conditions = 0

      where = {'author':'', 'title':'', 'rank':'', 'genre':'',
               'author_table': tables['author'],
               'genre_table': tables['genre'],
               'title_table': tables['title']}
      # Special case for inner select, add its own WHERE, no AND
      if title:
        where['title'] = 'WHERE title CONTAINS "%s"' % title
      # Special case for first condition in query, no AND
      if author:
        conditions += 1
        where['author'] = ' at.a.author CONTAINS "%s"' % author
      # The following check for conditions > 1 to insert AND as needed
      if rank:
        conditions += 1
        rng = re.compile('[><=!]+')
        if rng.match(rank):
          where['rank'] = ' at.a.id %s' % rank
        else:
          where['rank'] = ' at.a.id = %s' % rank
        if conditions > 1:
          where['rank'] = ' AND%s' % where['rank']
      if genre:
        conditions += 1
        where['genre'] = ' g.genre_list CONTAINS "%s"' % genre
        if conditions > 1:
          where['genre'] = ' AND%s' % where['genre']

      if conditions:
        where['where'] = 'WHERE'
        if ignorecase:
          where['ignorecase'] = ' IGNORE CASE'
        else:
          where['ignorecase'] = ''
      else:
        where['where'] = ''
        where['ignorecase'] = ''

      query = """
SELECT at.a.id, at.t.title, at.a.author, g.genre_list
FROM
    (SELECT *
     FROM
        (SELECT *
         FROM
           %(title_table)s
         %(title)s
        ) AS t
        JOIN
           %(author_table)s AS a
        ON a.id = t.id
    ) AS at
JOIN
    %(genre_table)s AS g
ON at.a.id = g.id
%(where)s%(author)s%(rank)s%(genre)s
ORDER BY at.a.id%(ignorecase)s;""" % where

      # Synchronous call to Query
      queryresults=bq.Query(decoratedhttp, query)

      logging.info('results=%s' % queryresults)

      tablerows = ""
      for row in queryresults['rows']:
        tablerows +='<tr>'
        for datum in row:
          tablerows += '<td>%s</td>' % datum
        tablerows += '<tr>\n'

      text += '<h2>The Query</h2><pre>%s</pre>\n<hr>\n' % queryresults['query']
      text += '<table border=1>\n%s\n</table>\n' % tablerows

      path = os.path.join(os.path.dirname(__file__), 'welcome.html')
      self.response.out.write(template.render(path, {'text': text }))

    except AccessTokenRefreshError:
      self.redirect('/')

    except HttpError, err:
      text += "<h2>Error %d</h2>\n" % err.resp.status
      text += "%s <br>" % cgi.escape(err._get_reason())
      text += "when requesting<br><pre>\t%s</pre>" % err.uri
      path = os.path.join(os.path.dirname(__file__), 'welcome.html')
      self.response.out.write(template.render(path, {'text': text }))


def main():
  application = webapp.WSGIApplication(
      [
       ('/', MainHandler),
       ('/about', AboutHandler),
      ],
      debug=True)
  run_wsgi_app(application)


if __name__ == '__main__':
  main()
