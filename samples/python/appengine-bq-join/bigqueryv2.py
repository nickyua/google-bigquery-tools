# Copyright 2012 Google Inc. All Rights Reserved.

"""Library to make BigQuery v2 client requests."""

__author__ = 'kbrisbin@google.com (Kathryn Hurley)'

import cgi
import errors
import logging
from apiclient.discovery import build
from apiclient.errors import HttpError

TIMEOUT_MS = 1000
BIGQUERY_API_VERSION = 'v2'


class BigQueryClient(object):
  """BigQuery version 2 client."""

  def __init__(self, project_id, api_version=BIGQUERY_API_VERSION):
    """Creates the BigQuery client connection.

    Args:
      project_id: either the numeric ID or your registered ID.
          This defines the project to receive the bill for query usage.
      api_version: version of BigQuery API to construct.
    """
    self.service = build('bigquery', api_version)
    self.project_id = project_id

  def query(self, authorized_http, query):
    """Issues an synchronous query to bigquery v2.

    Args:
      authorized_http: the authorized Http instance.
      query: string SQL query to run.
    Returns:
      The string job reference.
    Raises:
      QueryError if the query fails.
    """
    logging.info(query)
    job_collection = self.service.jobs()
    job_data = {
        'projectId': self.project_id,
        'configuration': {
            'query': {
                'query': query
            }
          }
        }
    request = job_collection.insert(
        projectId=self.project_id,
        body=job_data)
    try:
      response = request.execute(authorized_http)
    except HttpError:
      raise errors.QueryError
    return response['jobReference']['jobId']

  def poll(self, authorized_http, job_id, timeout_ms=TIMEOUT_MS):
    """Polls the job to get results.

    Args:
      authorized_http: the authorized Http instance.
      job_id: the running job.
      timeout_ms: the number of milliseconds to wait for results.
    Returns:
      The job results.
    Raises:
      PollError when the poll fails.
    """
    job_collection = self.service.jobs()
    request = job_collection.getQueryResults(
        projectId=self.project_id,
        jobId=job_id,
        timeoutMs=timeout_ms)
    try:
      response = request.execute(authorized_http)
    except HttpError, err:
      logging.error(cgi.escape(err._get_reason()))
      raise errors.PollError

    if 'jobComplete' in response:
      complete = response['jobComplete']
      if complete:
        rows = response['rows']
        schema = response['schema']
        converter = self.Converter(schema)
        formatted_rows = []
        for row in rows:
          formatted_rows.append(converter.convert_row(row))
        response['formattedRows'] = formatted_rows
    return response

  class Converter(object):
    """Does schema-based type conversion of result data."""

    def __init__(self, schema_row):
      """Sets up the schema converter.

      Args:
        schema_row: a dict containing BigQuery schema definitions.
      """
      self.schema = []
      for field in schema_row['fields']:
        self.schema.append(field['type'])

    def convert_row(self, row):
      """Converts a row of data into a tuple with type conversion applied.

      Args:
        row: a row of BigQuery data.
      Returns:
        A tuple with the converted data values for the row.
      """
      i = 0
      data = []
      for entry in row['f']:
        data.append(self.convert(entry['v'], self.schema[i]))
        i += 1
      return tuple(data)

    def convert(self, entry, schema_type):
      """Converts an entry based on the schema type given.

      Args:
        entry: the data entry to convert.
        schema_type: appropriate type for the entry.
      Returns:
        The data entry, either as passed in, or converted to the given type.
      """
      if schema_type == u'FLOAT' and entry is not None:
        return float(entry)
      elif schema_type == u'INTEGER' and entry is not None:
        return int(entry)
      else:
        return entry
