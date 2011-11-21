#!/usr/bin/env python
#
# Copyright 2011 Google Inc. All Rights Reserved.
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

"""Library to make BigQuery v2 client requests."""

__author__ = 'Donn Denman'
__version__ = '0.0a1.dev1'

import logging
import os

from apiclient import discovery
from google.appengine.api import memcache
import httplib2
from oauth2client.client import Credentials

# Defaults
_TIMEOUT_MS = 100000
_BIGQUERY_API_VERSION = 'v2'

class BigQueryClientException(Exception):
  pass

class BigQueryClient(object):
    """BigQuery version 2 client.

    Args:
        cleanhttp: an undecorated httplib2 object.
        projectID: either the numeric ID or your registered ID.
            This defines the project to receive the bill for query usage.
        api_version: version of BigQuery API to construct.
    """

    def __init__(self, cleanhttp, projectID=False,
               api_version=_BIGQUERY_API_VERSION):
        """Creates the BigQuery client connection"""
        self.service = discovery.build('bigquery', api_version, http=cleanhttp)
        self.defaultProject = projectID

    def Query(self, decoratedhttp, query, project_id=False, timeout_ms=_TIMEOUT_MS):
        """Issues a synchronous query to bigquery v2.

        Args:
            decoratedhttp: [required] httplib2 object wrapped by
                oauth2client.appengine decorator.
            query: [required] String of SQL query to run.
            project_id: project to bill for query.
        """
        query_config = {
            'query': query,
            'timeoutMs': timeout_ms
        }
        if not project_id:
            project_id = self.defaultProject

        logging.info('Query: %s', query)
        result_json = (self.service.jobs()
                       .query(projectId=project_id, body=query_config)
                       .execute(decoratedhttp))
        total_rows = result_json['totalRows']
        logging.info('Query result total_rows: %s', total_rows)

        schema = self.Schema(result_json['schema'])
        result_rows = []
        if 'rows' in result_json:
            for row in result_json['rows']:
                result_rows.append(schema.ConvertRow(row))
                logging.info('Returning %d rows.', len(result_rows))
        return {'rows':result_rows, 'schema':schema, 'query':query}

    class Schema(object):
        """Does schema-based type conversion of result data."""

        def __init__(self, schema_row):
            """Sets up the schema converter.

            Args:
              schema_row: a dict containing BigQuery schema definitions, ala
            {'fields': [{'type': 'FLOAT', 'name': 'field', 'mode': 'REQUIRED'},
                        {'type': 'INTEGER', 'name': 'climate_bin'}]}
            """
            self.schema = []
            for field in schema_row['fields']:
                self.schema.append(field['type'])

        def ConvertRow(self, row):
            """Converts a row of data into a tuple with type conversion applied.

            Args:
              row: a row of BigQuery data, ala
                  {'f': [{'v': 665.60329999999999}, {'v': '1'}]}
            Returns:
              a tuple with the converted data values for the row.
            """
            i = 0
            data = []
            for entry in row['f']:
                data.append(self.Convert(entry['v'], self.schema[i]))
                i += 1
            return tuple(data)

        def Convert(self, entry, schema_type):
          """Converts an entry based on the schema type given.

          Args:
            entry: the data entry to convert.
            schema_type: appropriate type for the entry.
          Returns:
            the data entry, either as passed in, or converted to the given type.
          """
          if schema_type == u'FLOAT' and entry is not None:
            return float(entry)
          elif schema_type == u'INTEGER' and entry is not None:
            return int(entry)
          else:
            return entry
