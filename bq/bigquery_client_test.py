#!/usr/bin/env python
# Copyright 2011 Google Inc. All Rights Reserved.

"""Tests for bigquery_client.py."""



import itertools
import json
import tempfile

from google.apputils import googletest

import bigquery_client


class BigqueryClientTest(googletest.TestCase):

  def setUp(self):
    self.client = bigquery_client.BigqueryClient(api='', api_version='')
    self.reference_tests = {
        'prj:': ('prj', '', ''),
        'example.com:prj': ('example.com:prj', '', ''),
        'example.com:prj-2': ('example.com:prj-2', '', ''),
        'www.example.com:prj': ('www.example.com:prj', '', ''),
        'prj:ds': ('prj', 'ds', ''),
        'example.com:prj:ds': ('example.com:prj', 'ds', ''),
        'prj:ds.tbl': ('prj', 'ds', 'tbl'),
        'example.com:prj:ds.tbl': ('example.com:prj', 'ds', 'tbl'),
        }
    self.parse_tests = self.reference_tests.copy()
    self.parse_tests.update({
        'ds.': ('', 'ds', ''),
        'ds.tbl': ('', 'ds', 'tbl'),
        'tbl': ('', '', 'tbl'),
        })
    self.field_names = ('projectId', 'datasetId', 'tableId')

  @staticmethod
  def _LengthToType(parts):
    if len(parts) == 1:
      return bigquery_client.ApiClientHelper.ProjectReference
    if len(parts) == 2:
      return bigquery_client.ApiClientHelper.DatasetReference
    if len(parts) == 3:
      return bigquery_client.ApiClientHelper.TableReference
    return None

  def _GetReference(self, parts):
    parts = filter(bool, parts)
    reference_type = BigqueryClientTest._LengthToType(parts)
    args = dict(itertools.izip(self.field_names, parts))
    return reference_type(**args)

  def testToCamel(self):
    self.assertEqual('lowerCamel', bigquery_client._ToLowerCamel('lower_camel'))

  def testReadSchemaFromFile(self):
    # Test the filename case.
    with tempfile.NamedTemporaryFile() as f:
      # Write out the results.
      print >>f, '['
      print >>f, ' { "name": "Number", "type": "integer", "mode": "REQUIRED" },'
      print >>f, ' { "name": "Name", "type": "string", "mode": "REQUIRED" },'
      print >>f, ' { "name": "Other", "type": "string", "mode": "OPTIONAL" }'
      print >>f, ']'
      f.flush()
      # Read them as JSON.
      f.seek(0)
      result = json.load(f)
      # Compare the results.
      self.assertEqual(result, self.client.ReadSchema(f.name))

  def testReadSchemaFromString(self):
    # Check some cases that should pass.
    self.assertEqual(
        [{'name': 'foo', 'type': 'INTEGER'}],
        bigquery_client.BigqueryClient.ReadSchema('foo:integer'))
    self.assertEqual(
        [{'name': 'foo', 'type': 'INTEGER'},
         {'name': 'bar', 'type': 'STRING'}],
        bigquery_client.BigqueryClient.ReadSchema('foo:integer, bar:string'))
    self.assertEqual(
        [{'name': 'foo', 'type': 'STRING'}],
        bigquery_client.BigqueryClient.ReadSchema('foo'))
    self.assertEqual(
        [{'name': 'foo', 'type': 'STRING'},
         {'name': 'bar', 'type': 'STRING'}],
        bigquery_client.BigqueryClient.ReadSchema('foo,bar'))
    self.assertEqual(
        [{'name': 'foo', 'type': 'INTEGER'},
         {'name': 'bar', 'type': 'STRING'}],
        bigquery_client.BigqueryClient.ReadSchema('foo:integer, bar'))
    # Check some cases that should fail.
    self.assertRaises(bigquery_client.BigquerySchemaError,
                      bigquery_client.BigqueryClient.ReadSchema,
                      '')
    self.assertRaises(bigquery_client.BigquerySchemaError,
                      bigquery_client.BigqueryClient.ReadSchema,
                      'foo,bar:int:baz')
    self.assertRaises(bigquery_client.BigquerySchemaError,
                      bigquery_client.BigqueryClient.ReadSchema,
                      'foo:int,,bar:string')

  def testParseIdentifier(self):
    for identifier, parse in self.parse_tests.iteritems():
      self.assertEquals(parse, bigquery_client.BigqueryClient._ParseIdentifier(
          identifier))

  def testGetReference(self):
    for identifier, parse in self.reference_tests.iteritems():
      reference = self._GetReference(parse)
      self.assertEquals(reference, self.client.GetReference(identifier))

  def testParseDatasetReference(self):
    dataset_parses = dict((k, v) for k, v in self.reference_tests.iteritems()
                          if len(filter(bool, v)) == 2)

    for identifier, parse in dataset_parses.iteritems():
      reference = self._GetReference(parse)
      self.assertEquals(reference, self.client.GetDatasetReference(identifier))

    for invalid in ['ds.tbl', 'prj:ds.tbl']:
      self.assertRaises(bigquery_client.BigqueryError,
                        self.client.GetDatasetReference, invalid)

  def testParseProjectReference(self):
    project_parses = dict((k, v) for k, v in self.reference_tests.iteritems()
                          if len(filter(bool, v)) == 1)

    for identifier, parse in project_parses.iteritems():
      reference = self._GetReference(parse)
      self.assertEquals(reference, self.client.GetProjectReference(identifier))

    invalid_projects = [
        'prj:ds', 'example.com:prj:ds', 'ds.', 'ds.tbl', 'prj:ds.tbl']

    for invalid in invalid_projects:
      self.assertRaises(bigquery_client.BigqueryError,
                        self.client.GetProjectReference, invalid)


if __name__ == '__main__':
  googletest.main()
