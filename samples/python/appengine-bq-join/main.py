# Copyright 2012 Google Inc. All Rights Reserved.

"""BigQuery App Engine demo.

Demos how to start a BigQuery job running, then poll the job
to get the results when it's complete.
"""

__author__ = 'kbrisbin@google.com (Kathryn Hurley)'

import bigqueryv2
import errors
import httplib2
import os
import simplejson
from oauth2client.appengine import oauth2decorator_from_clientsecrets
from google.appengine.api import memcache
from google.appengine.ext import webapp
from google.appengine.ext.webapp import template

# Project ID for your BigQuery Project in the API Console
PROJECT_ID = '[YOUR PROJECT ID]'

# CLIENT_SECRETS, name of a file containing the OAuth 2.0 information for this
# application, including client_id and client_secret, which are found
# on the API Access tab on the Google APIs
# Console <http://code.google.com/apis/console>
CLIENT_SECRETS = os.path.join(os.path.dirname(__file__), 'client_secrets.json')

decorator = oauth2decorator_from_clientsecrets(
    filename=CLIENT_SECRETS,
    scope='https://www.googleapis.com/auth/bigquery',
    message=template.render(
        os.path.join(os.path.dirname(__file__), 'templates/error.html'),
        {'clientSecrets': CLIENT_SECRETS}))

BQ = bigqueryv2.BigQueryClient(PROJECT_ID)


class MainHandler(webapp.RequestHandler):
  """Display the index page."""

  @decorator.oauth_aware
  def get(self):
    """Main handler.

    Displays index page if logged in.
    Otherwise, starts OAuth 2.0 dance.
    """
    path = os.path.join(os.path.dirname(__file__), 'templates/index.html')
    if decorator.has_credentials():
      self.redirect('/about')

    variables = {'url': decorator.authorize_url()}
    self.response.out.write(template.render(path, variables))


class QueryPage(webapp.RequestHandler):
  """Display the query page."""

  @decorator.oauth_required
  def get(self):
    """Display the query HTML page."""
    path = os.path.join(os.path.dirname(__file__), 'templates/query.html')
    self.response.out.write(template.render(path, {}))


class QueryHandler(webapp.RequestHandler):
  """Handle queries to BigQuery."""

  @decorator.oauth_required
  def get(self):
    """Poll the job to see if it's complete."""
    authorized_http = decorator.http()
    job_id = self.request.get('jobId')

    try:
      response = BQ.poll(authorized_http, job_id)
      json_response = simplejson.dumps(response)
      self.response.headers['Content-Type'] = 'application/json'
      self.response.out.write(json_response)

    except errors.PollError:
      self.response.set_status(500, 'Error during Poll')

  @decorator.oauth_required
  def post(self):
    """Post a new query job to BigQuery."""
    authorized_http = decorator.http()
    query = self.request.get('query')

    try:
      job_id = BQ.query(authorized_http, query)
      json_response = simplejson.dumps({'jobId': job_id})
      self.response.headers['Content-Type'] = 'application/json'
      self.response.out.write(json_response)

    except errors.QueryError:
      self.response.set_status(500, 'Error during Query')


app = webapp.WSGIApplication(
    [
        ('/', MainHandler),
        ('/about', QueryPage),
        ('/query', QueryHandler),
    ],
    debug=True
)
