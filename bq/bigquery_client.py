#!/usr/bin/env python
# Copyright 2011 Google Inc. All Rights Reserved.

"""Bigquery Client library for Python."""



import datetime
import itertools
import logging
import os
import re
import shutil
import sys
import tempfile
import time


import apiclient
from apiclient import anyjson
from apiclient import discovery
from apiclient import http as http_request
from apiclient import model

import httplib2

json = anyjson.simplejson


def _Typecheck(obj, types, message=None, method=None):
  if not isinstance(obj, types):
    if not message:
      if method:
        message = 'Invalid reference for %s: %r' % (method, obj)
      else:
        message = 'Type of %r is not one of %s' % (obj, types)
    raise TypeError(message)


class BigqueryError(Exception):

  @staticmethod
  def Create(error, server_error):
    """Returns a BigqueryError for json error embedded in server_error."""
    # TODO(user): All errors should have reason and message,
    # but we currently have a couple error cases that don't
    # honor this. Remove this work-around of inspecting multiple
    # fields when it is no longer needed.
    reason = error.get('reason') or error.get('code')
    message = (error.get('message') or error.get('errorMessage') or
               '%s: %s' % (reason, ' '.join(error.get('arguments', []))))
    if not reason or not message:
      return BigqueryInterfaceError(
          'Error reported by server with missing error fields. '
          'Server returned: %s' % (str(server_error),))
    if reason == 'notFound':
      return BigqueryNotFoundError(message)
    if reason == 'duplicate':
      return BigqueryDuplicateError(message)
    if reason == 'accessDenied':
      return BigqueryAccessDeniedError(message)
    if reason == 'invalidQuery':
      return BigqueryInvalidQueryError(message)
    # We map the less interesting errors to BigqueryServiceError.
    return BigqueryServiceError(message)


class BigquerySchemaError(BigqueryError):
  """Error in locating or parsing the schema."""
  pass


class BigqueryCommunicationError(BigqueryError):
  """Error communicating with the server."""
  pass


class BigqueryInterfaceError(BigqueryError):
  """Response from server missing required fields."""
  pass


class BigqueryServiceError(BigqueryError):
  """Base class of Bigquery-specific error responses.

  The BigQuery server received request and returned an error.
  """
  pass


class BigqueryNotFoundError(BigqueryServiceError):
  """The requested resource or identifier was not found."""
  pass


class BigqueryDuplicateError(BigqueryServiceError):
  """The requested resource or identifier already exists."""
  pass


class BigqueryAccessDeniedError(BigqueryServiceError):
  """The user does not have access to the requested resource."""
  pass


class BigqueryInvalidQueryError(BigqueryServiceError):
  """The SQL statement is invalid."""
  pass


class BigqueryTableError(BigqueryError):
  pass


class BigqueryModel(model.JsonModel):
  """Adds optional global parameters to all requests."""

  def __init__(self, trace=None, **kwds):
    super(BigqueryModel, self).__init__(**kwds)
    self.trace = trace

  # pylint:disable-msg=C6409
  def request(self, headers, path_params, query_params, body_value):
    """Updates outgoing request."""
    if 'trace' not in query_params and self.trace:
      query_params['trace'] = self.trace
    return super(BigqueryModel, self).request(
        headers, path_params, query_params, body_value)


class BigqueryHttp(http_request.HttpRequest):
  """Converts errors into Bigquery errors."""

  def __init__(self, bigquery_model, *args, **kwds):
    super(BigqueryHttp, self).__init__(*args, **kwds)
    self._model = bigquery_model

  @staticmethod
  def Factory(bigquery_model):
    def _Construct(*args, **kwds):
      captured_model = bigquery_model
      return BigqueryHttp(captured_model, *args, **kwds)
    return _Construct

  # pylint:disable-msg=C6409
  def execute(self, **kwds):
    try:
      return super(BigqueryHttp, self).execute(**kwds)
    except apiclient.errors.HttpError, e:
      if e.resp.get('content-type', '').startswith('application/json'):
        # TODO(user): Remove this when apiclient supports logging
        # of error responses.
        self._model._log_response(e.resp, e.content)  # pylint:disable-msg=W0212
        BigqueryClient.RaiseError(json.loads(e.content))
      else:
        raise BigqueryCommunicationError(
            'Error communicating with BigQuery server, server returned ' +
            e.resp.get('status', '(unexpected)'))


class BigqueryClient(object):
  """Class encapsulating interaction with the BigQuery service."""

  def __init__(self, **kwds):
    super(BigqueryClient, self).__init__()
    for key, value in kwds.iteritems():
      setattr(self, key, value)
    self._apiclient = None
    for required_flag in ('api', 'api_version'):
      if required_flag not in kwds:
        raise ValueError('Missing required flag: %s' % (required_flag,))
    default_flag_values = {
        'project_id': '',
        'dataset_id': '',
        'discovery_document': None,
        'job_property': '',
        'trace': None,
        }
    for flagname, default in default_flag_values.iteritems():
      if not hasattr(self, flagname):
        setattr(self, flagname, default)
    if self.dataset_id and not self.project_id:
      raise ValueError('Cannot set dataset_id without project_id')

  @property
  def apiclient(self):
    """Return the apiclient attached to self."""
    if self._apiclient is None:
      http = self.credentials.authorize(httplib2.Http())
      bigquery_model = BigqueryModel(self.trace)
      bigquery_http = BigqueryHttp.Factory(bigquery_model)
      if self.discovery_document is None:
        discovery_url = '%s/discovery/v1/apis/{api}/{apiVersion}/rest' % (
            self.api,)
        self._apiclient = discovery.build(
            'bigquery', self.api_version, http=http,
            discoveryServiceUrl=discovery_url,
            model=bigquery_model,
            requestBuilder=bigquery_http)
      else:
        self._apiclient = discovery.build_from_document(
            self.discovery_document, self.api, http=http,
            model=bigquery_model,
            requestBuilder=bigquery_http)
    return self._apiclient

  @staticmethod
  def FormatTime(secs):
    return time.strftime('%d %b %H:%M:%S', time.localtime(secs))

  @staticmethod
  def FormatAcl(acl):
    """Format a server-returned ACL for printing."""
    acl_entries = {
        'OWNER': [],
        'WRITER': [],
        'READER': [],
        }
    for entry in acl:
      entry = entry.copy()
      role = entry.pop('role', '')
      if not role or len(entry.values()) != 1:
        raise BigqueryServiceError(
            'Invalid ACL returned by server: %s' % (acl,))
      for key, value in entry.iteritems():
        # TODO(user): Remove this if once we've switched
        # to v2.
        if key == 'allAuthenticatedUsers':
          acl_entries[role].append(key)
        else:
          acl_entries[role].append(value)
    result_lines = []
    if acl_entries['OWNER']:
      result_lines.extend([
          'Owners:', ',\n'.join('  %s' % (o,) for o in acl_entries['OWNER'])])
    if acl_entries['WRITER']:
      result_lines.extend([
          'Writers:', ',\n'.join('  %s' % (o,) for o in acl_entries['WRITER'])])
    if acl_entries['READER']:
      result_lines.extend([
          'Readers:', ',\n'.join('  %s' % (o,) for o in acl_entries['READER'])])
    return '\n'.join(result_lines)

  @staticmethod
  def FormatSchema(schema):
    """Format a schema for printing."""

    def PrintFields(fields, indent=0):
      """Print all fields in a schema, recurring as necessary."""
      lines = []
      for field in fields:
        prefix = '|  ' * indent
        junction = '|' if field.get('type', 'STRING') != 'RECORD' else '+'
        entry = '%s- %s: %s' % (
            junction, field['name'], field.get('type', 'STRING').lower())
        if field.get('mode', 'NULLABLE') != 'NULLABLE':
          entry += ' (%s)' % (field['mode'].lower(),)
        lines.append(prefix + entry)
        if 'fields' in field:
          lines.extend(PrintFields(field['fields'], indent + 1))
      return lines

    return '\n'.join(PrintFields(schema.get('fields', [])))

  @staticmethod
  def NormalizeWait(wait):
    try:
      return int(wait)
    except ValueError:
      raise ValueError('Invalid value for wait: %s' % (wait,))

  @staticmethod
  def ValidatePrintFormat(print_format):
    if print_format not in ['show', 'list']:
      raise ValueError('Unknown format: %s' % (print_format,))

  @staticmethod
  def _ParseIdentifier(identifier):
    """Parses identifier into a tuple of (possibly empty) identifiers.

    This will parse the identifier into a tuple of the form
    (project_id, dataset_id, table_id) without doing any validation on
    the resulting names; missing names are returned as ''.

    Args:
      identifier: string, identifier to parse

    Returns:
      project_id, dataset_id, table_id: (string, string, string)
    """
    # We need to handle the case of a lone project identifier of the
    # form domain.com:proj separately.
    if re.search('^[\w.]+\.[\w.]+:\w*:?$', identifier):
      return identifier, '', ''
    project_id, _, dataset_and_table_id = identifier.rpartition(':')
    # Note that the two below only differ in the case that '.' is not
    # present.
    if project_id:
      dataset_id, _, table_id = dataset_and_table_id.partition('.')
    else:
      dataset_id, _, table_id = dataset_and_table_id.rpartition('.')
    return project_id, dataset_id, table_id

  def GetProjectReference(self, identifier=''):
    """Determine a project reference from an identifier and self."""
    project_id, dataset_id, table_id = BigqueryClient._ParseIdentifier(
        identifier)
    try:
      # ParseIdentifier('foo') is just a table_id, but we want to read
      # it as a project_id.
      project_id = project_id or table_id or self.project_id
      if not dataset_id and project_id:
        return ApiClientHelper.ProjectReference.Create(projectId=project_id)
    except ValueError:
      pass
    raise BigqueryError('Cannot determine project described by %s' % (
        identifier,))

  def GetDatasetReference(self, identifier=''):
    """Determine a DatasetReference from an identifier and self."""
    project_id, dataset_id, table_id = BigqueryClient._ParseIdentifier(
        identifier)
    if table_id and not project_id and not dataset_id:
      # identifier is 'foo'
      project_id = self.project_id
      dataset_id = table_id
    elif project_id and dataset_id and not table_id:
      # identifier is 'foo:bar'
      pass
    elif not identifier:
      # identifier is ''
      project_id = self.project_id
      dataset_id = self.dataset_id
    else:
      raise BigqueryError('Cannot determine dataset described by %s' % (
          identifier,))

    try:
      return ApiClientHelper.DatasetReference.Create(
          projectId=project_id, datasetId=dataset_id)
    except ValueError:
      raise BigqueryError('Cannot determine dataset described by %s' % (
          identifier,))

  def GetTableReference(self, identifier=''):
    """Determine a TableReference from an identifier and self."""
    project_id, dataset_id, table_id = BigqueryClient._ParseIdentifier(
        identifier)
    try:
      return ApiClientHelper.TableReference.Create(
          projectId=project_id or self.project_id,
          datasetId=dataset_id or self.dataset_id,
          tableId=table_id,
          )
    except ValueError:
      raise BigqueryError('Cannot determine table described by %s' % (
          identifier,))

  def GetReference(self, identifier=''):
    """Try to deduce a project/dataset/table reference from a string.

    If the identifier is not compound, treat it as the most specific
    identifier we don't have as a flag, or as the table_id. If it is
    compound, fill in any unspecified part.

    Args:
      identifier: string, Identifier to create a reference for.

    Returns:
      A valid ProjectReference, DatasetReference, or TableReference.

    Raises:
      BigqueryError: if no valid reference can be determined.
    """
    try:
      return self.GetTableReference(identifier)
    except BigqueryError:
      pass
    try:
      return self.GetDatasetReference(identifier)
    except BigqueryError:
      pass
    try:
      return self.GetProjectReference(identifier)
    except BigqueryError:
      pass
    raise BigqueryError('Cannot determine reference for "%s"' % (identifier,))

  @staticmethod
  def _SymlinkOrCopy(upload_file, temp_dir):
    temp_upload_file = os.path.join(temp_dir, 'data.bin')
    if os.name == 'posix':
      # On Unix, just symlink the file.
      os.symlink(upload_file, temp_upload_file)
    else:
      # Since Python doesn't support Windows symlinks, we copy the file, but
      # warn the user if the file is large.
      size = os.stat(upload_file).st_size / (1024 * 1024)
      if size > 10:
        print 'To complete this operation, the BigQuery CLI needs to create a'
        print 'temporary file that is %d MB in size.' % (size,)
        print
        response = None
        while response not in ['y', 'n', '']:
          response = raw_input('Proceed? (Y/n) ').lower()
        if response == 'n':
          raise BigqueryError('Operation cancelled by user.')
      shutil.copy(upload_file, temp_upload_file)
    return temp_upload_file

  def StartJob(self, configuration, project_id=None, upload_file=None):
    """Start a job with the given configuration."""
    project_id = project_id or self.project_id
    if not project_id:
      raise ValueError(
          'Cannot start a job without a project id. Try '
          'running "bq init".')
    configuration = configuration.copy()
    if self.job_property:
      configuration['properties'] = dict(
          prop.partition('=')[0::2] for prop in self.job_property)
    job_request = {'configuration': configuration}
    try:
      temp_dir = None
      temp_upload_file = None

      if upload_file:
        # The API client determines the MIME type from the file suffix.
        # Since uploads must have MIME type application/octet-stream
        # (for now), we symlink (or copy, on Windows) the input file to
        # a temporary file with the appropriate suffix.
        # TODO(user): Remove all tempfile nonsense once the API
        # client allows us to specify the MIME type.
        temp_dir = tempfile.mkdtemp()
        temp_upload_file = self._SymlinkOrCopy(upload_file, temp_dir)

      # TODO(user): Figure out a way to show upload progress.
      result = self.apiclient.jobs().insert(
          body=job_request, media_body=temp_upload_file,
          projectId=project_id).execute()
    finally:
      if temp_upload_file:
        os.remove(temp_upload_file)
      if temp_dir:
        os.rmdir(temp_dir)
    return result

  def RunJobSynchronously(self, configuration, project_id=None,
                          upload_file=None):
    result = self.StartJob(configuration, project_id=project_id,
                           upload_file=upload_file)
    job_reference = BigqueryClient.ConstructObjectReference(result)
    result = self.WaitJob(job_reference)
    return self.RaiseIfJobError(result)

  def WaitJob(self, job_reference, status='DONE',
              wait=sys.maxint, print_status=True):
    """Poll for a job to run until it reaches the requested status.

    Arguments:
      job_reference: JobReference to poll.
      status: (optional, default 'DONE') Desired job status.
      wait: (optional, default maxint) Max wait time.
      print_status: (optional, default True) If True, print status
        and wait time as the job executes.

    Returns:
      The job object returned by the final status call.

    Raises:
      StopIteration: If polling does not reach the desired state before
        timing out.
      ValueError: If given an invalid wait value.
    """

    def PrintStatus(job_id, current_wait, status):
      printed = False
      if print_status:
        print '\rWaiting on %s ... (%ds) Current status: %-7s' % (
            job_id, current_wait, status),
        printed = True
        sys.stdout.flush()
      return printed

    _Typecheck(job_reference, ApiClientHelper.JobReference, method='WaitJob')
    # TODO(user): Change the defaults for this function.
    current = 'UNKNOWN'
    start_time = time.time()
    job = None
    print_status = print_status and not self.quiet

    # This is a first pass at wait logic: we ping at 1s intervals a few
    # times, then increase to max(3, max_wait), and then keep waiting
    # that long until we've run out of time.
    waits = itertools.chain(
        itertools.repeat(1, 8),
        xrange(2, 30, 3),
        itertools.repeat(30))
    current_wait = 0
    while current_wait <= wait:
      try:
        done, job = self.PollJob(job_reference, status=status, wait=wait)
        if done:
          PrintStatus(job_reference.jobId, current_wait, job['status']['state'])
          break
      except BigqueryCommunicationError, e:
        # Communication errors while waiting on a job are okay.
        logging.warning('Transient error during job status check: %s', e)
      for _ in xrange(waits.next()):
        current_wait = time.time() - start_time
        PrintStatus(job_reference.jobId, current_wait, job['status']['state'])
        time.sleep(1)
    else:
      raise StopIteration(
          'Wait timed out. Operation not finished, in state %s' % (current,))
    if print_status and wait:
      print
    return job

  def PollJob(self, job_reference, status='DONE', wait=0):
    """Poll a job once for a specific status.

    Arguments:
      job_reference: JobReference to poll.
      status: (optional, default 'DONE') Desired job status.
      wait: (optional, default 0) Max server-side wait time for one poll call.

    Returns:
      Tuple (in_state, job) where in_state is True if job is
      in the desired state.

    Raises:
      ValueError: If given an invalid wait value.
    """
    _Typecheck(job_reference, ApiClientHelper.JobReference, method='PollJob')
    wait = BigqueryClient.NormalizeWait(wait)
    # TODO(user): Wire in wait once the server supports wait
    # timeouts on get requests.
    job = self.apiclient.jobs().get(**dict(job_reference)).execute()
    current = job['status']['state']
    return (current == status, job)

  def GetObjectInfo(self, reference):
    """Get all data returned by the server about a specific object."""
    # Projects are handled separately, because we only have
    # bigquery.projects.list.
    if isinstance(reference, ApiClientHelper.ProjectReference):
      projects = self.ListProjects()
      for project in projects:
        if BigqueryClient.ConstructObjectReference(project) == reference:
          project['kind'] = 'bigquery#project'
          return project
      raise BigqueryNotFoundError('Unknown %r' % (reference,))

    if isinstance(reference, ApiClientHelper.JobReference):
      return self.apiclient.jobs().get(**dict(reference)).execute()
    elif isinstance(reference, ApiClientHelper.DatasetReference):
      return self.apiclient.datasets().get(**dict(reference)).execute()
    elif isinstance(reference, ApiClientHelper.TableReference):
      return self.apiclient.tables().get(**dict(reference)).execute()
    else:
      raise TypeError('Type of reference must be one of: ProjectReference, '
                      'JobReference, DatasetReference, or TableReference')

  def GetTableSchema(self, table_dict):
    table_info = self.apiclient.tables().get(**table_dict).execute()
    return table_info.get('schema', {})

  def ReadTableRows(self, table_dict, max_rows=sys.maxint):
    """Read at most max_rows rows from a table."""
    rows = []
    while len(rows) < max_rows:
      data = self.apiclient.tabledata().list(
          maxResults=min(10000, max_rows - len(rows)),
          startIndex=len(rows), **table_dict).execute()
      max_rows = min(max_rows, int(data['totalRows']))
      more_rows = data.get('rows', [])
      for row in more_rows:
        rows.append([entry.get('v', '') for entry in row.get('f', [])])
      if not more_rows and len(rows) != max_rows:
        raise BigqueryInterfaceError(
            'Not enough rows returned by server for %r' % (
                ApiClientHelper.TableReference.Create(**table_dict),))
    return rows

  def ReadSchemaAndRows(self, table_dict, max_rows=sys.maxint):
    """Convenience method to get the schema and rows from a table.

    Arguments:
      table_dict: table reference dictionary.
      max_rows: number of rows to read.

    Returns:
      A tuple where the first item is the list of fields and the
      second item a list of rows.
    """
    return (self.GetTableSchema(table_dict).get('fields', []),
            self.ReadTableRows(table_dict, max_rows))

  @staticmethod
  def ConfigureFormatter(formatter, reference_type, print_format='list'):
    """Configure a formatter for a given reference type.

    If print_format is 'show', configures the formatter with several
    additional fields (useful for printing a single record).

    Arguments:
      formatter: TableFormatter object to configure.
      reference_type: Type of object this formatter will be used with.
      print_format: Either 'show' or 'list' to control what fields are
        included.

    Raises:
      ValueError: If reference_type or format is unknown.
    """
    BigqueryClient.ValidatePrintFormat(print_format)
    if reference_type == ApiClientHelper.JobReference:
      formatter.AddColumns(
          ('jobId', 'Job Type', 'State', 'Start Time', 'Duration'))
    elif reference_type == ApiClientHelper.ProjectReference:
      formatter.AddColumns(
          ('projectId', 'friendlyName'))
    elif reference_type == ApiClientHelper.DatasetReference:
      formatter.AddColumns(('datasetId',))
      if print_format == 'show':
        formatter.AddColumns(('Last modified', 'ACLs'))
    elif reference_type == ApiClientHelper.TableReference:
      formatter.AddColumns(('tableId',))
      if print_format == 'show':
        formatter.AddColumns(('Last modified', 'Schema'))
    else:
      raise ValueError('Unknown reference type: %s' % (
          reference_type.__name__,))

  @staticmethod
  def RaiseError(result):
    """Raises an appropriate BigQuery error given the json error result."""
    error = result.get('error', {}).get('errors', [None])[0]
    raise BigqueryError.Create(error, result)

  @staticmethod
  def RaiseIfJobError(job):
    """Raises a BigQueryError if the job is in an error state.

    Args:
      job: a Job resource.

    Returns:
      job, if it is not in an error state.

    Raises:
      BigqueryError: A BigqueryError instance based on the job's error
      description.
    """
    if 'errorResult' in job.get('status', {}):
      error = job['status']['errorResult']
      raise BigqueryError.Create(error, error)
    return job

  @staticmethod
  def GetJobTypeName(job_info):
    """Helper for job printing code."""
    job_names = set(('extract', 'load', 'query', 'link'))
    try:
      return set(job_info.get('configuration', {}).keys()).intersection(
          job_names).pop()
    except KeyError:
      return None

  @staticmethod
  def ReadSchema(schema):
    """Create a schema from a string or a filename.

    If schema does not contain ':' and is the name of an existing
    file, read it as a JSON schema. If not, it must be a
    comma-separated list of fields in the form name:type.

    Args:
      schema: A filename or schema.

    Returns:
      The new schema (as a dict).

    Raises:
      BigquerySchemaError:
        If the schema is invalid or the filename does not exist.
    """

    def NewField(entry):
      name, _, field_type = entry.partition(':')
      if entry.count(':') > 1 or not name.strip():
        raise BigquerySchemaError('Invalid schema entry: %s' % (entry,))
      return {
          'name': name.strip(),
          'type': field_type.strip().upper() or 'STRING',
          }

    if not schema:
      raise BigquerySchemaError('Schema cannot be empty')
    elif ':' not in schema and os.path.exists(schema):
      with open(schema) as f:
        try:
          return json.load(f)
        except ValueError, e:
          raise BigquerySchemaError(
              ('Error decoding JSON schema from file %s: %s\n'
               'To specify a one-column schema, use "name:string".') % (
                   schema, e))
    else:
      return [NewField(entry) for entry in schema.split(',')]

  @staticmethod
  def _KindToName(kind):
    """Convert a kind to just a type name."""
    return kind.partition('#')[2]

  @staticmethod
  def FormatInfoByKind(object_info):
    """Format a single object_info (based on its 'kind' attribute)."""
    kind = BigqueryClient._KindToName(object_info.get('kind'))
    if kind == 'job':
      return BigqueryClient.FormatJobInfo(object_info)
    elif kind == 'project':
      return BigqueryClient.FormatProjectInfo(object_info)
    elif kind == 'dataset':
      return BigqueryClient.FormatDatasetInfo(object_info)
    elif kind == 'table':
      return BigqueryClient.FormatTableInfo(object_info)
    else:
      raise ValueError('Unknown object type: %s' % (kind,))

  @staticmethod
  def FormatJobInfo(job_info):
    """Prepare a job_info for printing.

    Arguments:
      job_info: Job dict to format.

    Returns:
      The new job_info.
    """
    result = job_info.copy()
    reference = BigqueryClient.ConstructObjectReference(result)
    result.update(dict(reference))
    if 'startTime' in result.get('statistics', {}):
      start = int(result['statistics']['startTime']) / 1000
      duration_seconds = int(result['statistics']['endTime']) / 1000 - start
      result['Duration'] = str(datetime.timedelta(seconds=duration_seconds))
      result['Start Time'] = BigqueryClient.FormatTime(start)
    result['Job Type'] = BigqueryClient.GetJobTypeName(result)
    result['State'] = result['status']['state']
    if result['State'] == 'DONE':
      try:
        BigqueryClient.RaiseIfJobError(result)
        result['State'] = 'SUCCESS'
      except BigqueryError:
        result['State'] = 'FAILURE'
    return result

  @staticmethod
  def FormatProjectInfo(project_info):
    """Prepare a project_info for printing.

    Arguments:
      project_info: Project dict to format.

    Returns:
      The new project_info.
    """
    result = project_info.copy()
    reference = BigqueryClient.ConstructObjectReference(result)
    result.update(dict(reference))
    return result

  @staticmethod
  def FormatDatasetInfo(dataset_info):
    """Prepare a dataset_info for printing.

    Arguments:
      dataset_info: Dataset dict to format.

    Returns:
      The new dataset_info.
    """
    result = dataset_info.copy()
    reference = BigqueryClient.ConstructObjectReference(result)
    result.update(dict(reference))
    if 'lastModifiedTime' in result:
      result['Last modified'] = BigqueryClient.FormatTime(
          int(result['lastModifiedTime']) / 1000)
    if 'access' in result:
      result['ACLs'] = BigqueryClient.FormatAcl(result['access'])
    return result

  @staticmethod
  def FormatTableInfo(table_info):
    """Prepare a table_info for printing.

    Arguments:
      table_info: Table dict to format.

    Returns:
      The new table_info.
    """
    result = table_info.copy()
    reference = BigqueryClient.ConstructObjectReference(result)
    result.update(dict(reference))
    if 'lastModifiedTime' in result:
      result['Last modified'] = BigqueryClient.FormatTime(
          int(result['lastModifiedTime']) / 1000)
    if 'schema' in result:
      result['Schema'] = BigqueryClient.FormatSchema(result['schema'])
    return result

  @staticmethod
  def ConstructObjectReference(object_info):
    """Construct a Reference from a server response."""
    if 'kind' in object_info:
      typename = BigqueryClient._KindToName(object_info['kind'])
      lower_camel = typename + 'Reference'
      if lower_camel not in object_info:
        raise ValueError('Cannot find %s in object of type %s: %s' % (
            lower_camel, typename, object_info))
    else:
      keys = [k for k in object_info if k.endswith('Reference')]
      if len(keys) != 1:
        raise ValueError('Expected one Reference, found %s: %s' % (
            len(keys), keys))
      lower_camel = keys[0]
    upper_camel = lower_camel[0].upper() + lower_camel[1:]
    reference_type = getattr(ApiClientHelper, upper_camel, None)
    if reference_type is None:
      raise ValueError('Unknown reference type: %s' % (typename,))
    return reference_type.Create(**object_info[lower_camel])

  @staticmethod
  def ConstructObjectInfo(reference):
    """Construct an Object from an ObjectReference."""
    typename = reference.__class__.__name__
    lower_camel = typename[0].lower() + typename[1:]
    return {lower_camel: dict(reference)}

  def ListJobs(self, reference):
    """Return a list of jobs."""
    _Typecheck(reference, ApiClientHelper.ProjectReference, method='ListJobs')
    request = dict(reference)
    request['projection'] = 'full'
    jobs = self.apiclient.jobs().list(**dict(request)).execute()
    return jobs.get('jobs', [])

  def ListProjects(self):
    """List the projects associated with this account."""
    result = self.apiclient.projects().list().execute()
    return result.get('projects', [])

  def ListDatasets(self, reference):
    """List the datasets associated with this reference."""
    _Typecheck(reference, ApiClientHelper.ProjectReference,
               method='ListDatasets')
    result = self.apiclient.datasets().list(**dict(reference)).execute()
    return result.get('datasets', [])

  def ListTables(self, reference):
    """List the tables associated with this reference."""
    _Typecheck(reference, ApiClientHelper.DatasetReference, method='ListTables')
    result = self.apiclient.tables().list(**dict(reference)).execute()
    return result.get('tables', [])

  def CopyTable(self, source_reference, dest_reference):
    """Copies a table."""
    _Typecheck(source_reference, ApiClientHelper.TableReference,
               method='CopyTable')
    _Typecheck(dest_reference, ApiClientHelper.TableReference,
               method='CopyTable')
    keywords = {
        'sourceProjectId': source_reference.projectId,
        'sourceDatasetId': source_reference.datasetId,
        'sourceTableId': source_reference.tableId
        }
    request = {
        'destinationTable': dict(dest_reference)
        }
    self.apiclient.tables().copy(body=request,
                                 **keywords).execute()

  def DatasetExists(self, reference):
    _Typecheck(reference, ApiClientHelper.DatasetReference,
               method='DatasetExists')
    try:
      self.apiclient.datasets().get(**dict(reference)).execute()
      return True
    except BigqueryNotFoundError:
      return False

  def TableExists(self, reference):
    _Typecheck(reference, ApiClientHelper.TableReference, method='TableExists')
    try:
      self.apiclient.tables().get(**dict(reference)).execute()
      return True
    except BigqueryNotFoundError:
      return False

  def CreateDataset(self, reference, ignore_existing=False):
    """Create a dataset corresponding to DatasetReference.

    Args:
      reference: the DatasetReference to create.
      ignore_existing: (boolean, default False) If False, raise
        an exception if the dataset already exists.

    Raises:
      TypeError: if reference is not a DatasetReference.
      BigqueryDuplicateError: if reference exists and ignore_existing
        is False.
    """
    _Typecheck(reference, ApiClientHelper.DatasetReference,
               method='CreateDataset')

    try:
      self.apiclient.datasets().insert(
          body=BigqueryClient.ConstructObjectInfo(reference),
          **dict(reference.GetProjectReference())).execute()
    except BigqueryDuplicateError:
      if not ignore_existing:
        raise

  def CreateTable(self, reference, ignore_existing=False, schema=None):
    """Create a dataset corresponding to DatasetReference.

    Args:
      reference: the DatasetReference to create.
      ignore_existing: (boolean, default True) If False, raise
        an exception if the dataset already exists.
      schema: an optional schema.

    Raises:
      TypeError: if reference is not a DatasetReference.
      BigqueryDuplicateError: if reference exists and ignore_existing
        is False.
    """
    _Typecheck(reference, ApiClientHelper.TableReference, method='CreateTable')

    try:
      body = BigqueryClient.ConstructObjectInfo(reference)
      if schema:
        body['schema'] = {'fields': schema}
      self.apiclient.tables().insert(
          body=body,
          **dict(reference.GetDatasetReference())).execute()
    except BigqueryDuplicateError:
      if not ignore_existing:
        raise

  def DeleteDataset(self, reference, ignore_not_found=False,
                    delete_contents=None):
    """Deletes DatasetReference reference.

    Args:
      reference: the DatasetReference to delete.
      ignore_not_found: Whether to ignore "not found" errors.
      delete_contents: [Boolean] Whether to delete the contents of
        non-empty datasets. If not specified and the dataset has
        tables in it, the delete will fail. If not specified, the
        server default applies.

    Raises:
      TypeError: if reference is not a DatasetReference.
      BigqueryNotFoundError: if reference does not exist and
        ignore_not_found is False.
    """
    _Typecheck(reference, ApiClientHelper.DatasetReference,
               method='DeleteDataset')

    args = dict(reference)
    if delete_contents is not None:
      args['deleteContents'] = delete_contents
    try:
      self.apiclient.datasets().delete(**args).execute()
    except BigqueryNotFoundError:
      if not ignore_not_found:
        raise

  def DeleteTable(self, reference, ignore_not_found=False):
    """Deletes TableReference reference.

    Args:
      reference: the TableReference to delete.
      ignore_not_found: Whether to ignore "not found" errors.

    Raises:
      TypeError: if reference is not a TableReference.
      BigqueryNotFoundError: if reference does not exist and
        ignore_not_found is False.
    """
    _Typecheck(reference, ApiClientHelper.TableReference, method='DeleteTable')
    try:
      self.apiclient.tables().delete(**dict(reference)).execute()
    except BigqueryNotFoundError:
      if not ignore_not_found:
        raise


class ApiClientHelper(object):
  """Static helper methods and classes not provided by the discovery client."""

  def __init__(self, *unused_args, **unused_kwds):
    raise NotImplementedError('Cannot instantiate static class ApiClientHelper')

  class Reference(object):
    """Base class for Reference objects returned by apiclient."""
    _required_fields = set()
    _format_str = ''

    def __init__(self, **kwds):
      if type(self) == ApiClientHelper.Reference:
        raise NotImplementedError(
            'Cannot instantiate abstract class ApiClientHelper.Reference')
      for name in self._required_fields:
        if not kwds.get(name, ''):
          raise ValueError('Missing required argument %s to %s' % (
              name, self.__class__.__name__))
        setattr(self, name, kwds[name])

    @classmethod
    def Create(cls, **kwds):
      """Factory method for this class."""
      args = dict((k, v) for k, v in kwds.iteritems()
                  if k in cls._required_fields)
      return cls(**args)

    def __iter__(self):
      return ((name, getattr(self, name)) for name in self._required_fields)

    def __str__(self):
      return self._format_str % dict(self)

    def __repr__(self):
      return "%s '%s'" % (self.typename, self)

    def __eq__(self, other):
      d = dict(other)
      return all(getattr(self, name) == d.get(name, '')
                 for name in self._required_fields)

  class JobReference(Reference):
    _required_fields = set(('projectId', 'jobId'))
    _format_str = '%(jobId)s'
    typename = 'job'

  class ProjectReference(Reference):
    _required_fields = set(('projectId',))
    _format_str = '%(projectId)s'
    typename = 'project'

  class DatasetReference(Reference):
    _required_fields = set(('projectId', 'datasetId'))
    _format_str = '%(projectId)s:%(datasetId)s'
    typename = 'dataset'

    def GetProjectReference(self):
      return ApiClientHelper.ProjectReference.Create(
          projectId=self.projectId)

  class TableReference(Reference):
    _required_fields = set(('projectId', 'datasetId', 'tableId'))
    _format_str = '%(projectId)s:%(datasetId)s.%(tableId)s'
    typename = 'table'

    def GetDatasetReference(self):
      return ApiClientHelper.DatasetReference.Create(
          projectId=self.projectId, datasetId=self.datasetId)

    def GetProjectReference(self):
      return ApiClientHelper.ProjectReference.Create(
          projectId=self.projectId)
