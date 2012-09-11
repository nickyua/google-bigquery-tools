#!/usr/bin/env python
# Copyright 2011 Google Inc. All Rights Reserved.

"""Python script for interacting with BigQuery."""




import cmd
import codecs
import httplib
import json
import os
import pdb
import pipes
import platform
import shlex
import stat
import sys
import time
import traceback
import types


import apiclient
import httplib2
import oauth2client
import oauth2client.client
import oauth2client.file
import oauth2client.tools

from google.apputils import app
from google.apputils import appcommands
import gflags as flags

import table_formatter
import bigquery_client


flags.DEFINE_string(
    'apilog', None,
    'Turn on logging of all server requests and responses. If no string is '
    'provided, log to stdout; if a string is provided, instead log to that '
    'file.')
flags.DEFINE_string(
    'api',
    'https://www.googleapis.com',
    'API endpoint to talk to.'
    )
flags.DEFINE_string(
    'api_version', 'v2',
    'API version to use.')
flags.DEFINE_boolean(
    'debug_mode', False,
    'Show tracebacks on Python exceptions.')
flags.DEFINE_string(
    'trace', None,
    'A tracing token of the form "trace:<traceid>" '
    'to include in api requests.')

flags.DEFINE_string(
    'bigqueryrc', os.path.join(os.path.expanduser('~'), '.bigqueryrc'),
    'Path to configuration file. The configuration file specifies '
    'new defaults for any flags, and can be overrridden by specifying the '
    'flag on the command line. If the --bigqueryrc flag is not specified, the '
    'BIGQUERYRC environment variable is used. If that is not specified, the '
    'path "~/.bigqueryrc" is used.')
flags.DEFINE_string(
    'credential_file', os.path.join(os.path.expanduser('~'),
    '.bigquery.v2.token'),
    'Filename used for storing the BigQuery OAuth token.')
flags.DEFINE_string(
    'discovery_file', '',
    'Filename for JSON document to read for discovery.')
flags.DEFINE_boolean(
    'synchronous_mode', True,
    'If True, wait for command completion before returning, and use the '
    'job completion status for error codes. If False, simply create the '
    'job, and use the success of job creation as the error code.',
    short_name='sync')
flags.DEFINE_string(
    'project_id', '',
    'Default project to use for requests.')
flags.DEFINE_string(
    'dataset_id', '',
    'Default dataset to use for requests. (Ignored when not applicable.)')
# This flag is "hidden" at the global scope to avoid polluting help
# text on individual commands for rarely used functionality.
flags.DEFINE_string(
    'job_id', None,
    'A unique job_id to use for the request. If None, the server will create '
    'a unique job_id. Applies only to commands that launch jobs, such as cp, '
    'extract, link, load, and query. ')
flags.DEFINE_boolean(
    'quiet', False,
    'If True, ignore status updates while jobs are running.',
    short_name='q')
flags.DEFINE_boolean(
    'headless',
    False,
    'Whether this bq session is running without user interaction. This '
    'affects behavior that expects user interaction, like whether '
    'debug_mode will break into the debugger and lowers the frequency '
    'of informational printing.')
flags.DEFINE_enum(
    'format', None,
    ['none', 'json', 'prettyjson', 'csv', 'sparse', 'pretty'],
    'Format for command output. Options include:'
    '\n pretty: formatted table output'
    '\n sparse: simpler table output'
    '\n prettyjson: easy-to-read JSON format'
    '\n json: maximally compact JSON'
    '\n csv: csv format with header'
    '\nThe first three are intended to be human-readable, and the latter '
    'three are for passing to another program. If no format is selected, '
    'one will be chosen based on the command run.')
flags.DEFINE_multistring(
    'job_property', None,
    'Additional key-value pairs to include in the properties field of '
    'the job configuration')  # No period: Multistring adds flagspec suffix.
flags.DEFINE_string(
    'service_account', '',
    'Use this service account email address for authorization. '
    'For example, 1234567890@developer.gserviceaccount.com.'
    )
flags.DEFINE_string(
    'service_account_private_key_file', '',
    'Filename that contains the service account private key. '
    'Required if --service_account is specified.')
flags.DEFINE_string(
    'service_account_private_key_password', 'notasecret',
    'Password for private key. This password must match the password '
    'you set on the key when you created it in the Google APIs Console. '
    'Defaults to the default Google APIs Console private key password.')
flags.DEFINE_string(
    'service_account_credential_store', None,
    'File to be used as a credential store for service accounts. '
    'Must be set if using a service account.')

FLAGS = flags.FLAGS
# These are long names.
# pylint:disable-msg=C6409
JobReference = bigquery_client.ApiClientHelper.JobReference
ProjectReference = bigquery_client.ApiClientHelper.ProjectReference
DatasetReference = bigquery_client.ApiClientHelper.DatasetReference
TableReference = bigquery_client.ApiClientHelper.TableReference
BigqueryClient = bigquery_client.BigqueryClient
# pylint:enable-msg=C6409

_CLIENT_USER_AGENT = 'bq/2.0'
_CLIENT_SCOPE = 'https://www.googleapis.com/auth/bigquery'
_CLIENT_INFO = {
    'client_id': '977385342095.apps.googleusercontent.com',
    'client_secret': 'wbER7576mc_1YOII0dGk7jEE',
    'scope': _CLIENT_SCOPE,
    'user_agent': _CLIENT_USER_AGENT,
    }
_BIGQUERY_TOS_MESSAGE = (
    'In order to get started, please visit the Google APIs Console to '
    'create a project and agree to our Terms of Service:\n'
    '\thttp://code.google.com/apis/console\n\n'
    'For detailed sign-up instructions, please see our Getting Started '
    'Guide:\n'
    '\thttps://developers.google.com/bigquery/docs/getting-started\n\n'
    'Once you have completed the sign-up process, please try your command '
    'again.')
_DELIMITER_MAP = {
    'tab': '\t',
    '\\t': '\t',
    }

# These aren't relevant for user-facing docstrings:
# pylint:disable-msg=C6112
# pylint:disable-msg=C6113
# TODO(user): Write some explanation of the structure of this file.

####################
# flags processing
####################



def _GetBigqueryRcFilename():
  """Return the name of the bigqueryrc file to use.

  In order, we look for a flag the user specified, an environment
  variable, and finally the default value for the flag.

  Returns:
    bigqueryrc filename as a string.
  """
  return ((FLAGS['bigqueryrc'].present and FLAGS.bigqueryrc) or
          os.environ.get('BIGQUERYRC') or
          FLAGS.bigqueryrc)


def _ProcessBigqueryrc():
  """Updates FLAGS with values found in the bigqueryrc file."""
  bigqueryrc = _GetBigqueryRcFilename()
  if not os.path.exists(bigqueryrc):
    return
  with open(bigqueryrc) as rcfile:
    for line in rcfile:
      if line.lstrip().startswith('#') or not line.strip():
        continue
      elif line.lstrip().startswith('['):
        # TODO(user): Support command-specific flag sections.
        continue
      flag, equalsign, value = line.partition('=')
      # if no value given, assume stringified boolean true
      if not equalsign:
        value = 'true'
      flag = flag.strip()
      value = value.strip()
      while flag.startswith('-'):
        flag = flag[1:]
      # We want flags specified at the command line to override
      # those in the flagfile.
      if flag not in FLAGS:
        raise app.UsageError(
            'Unknown flag %s found in bigqueryrc file' % (flag,))
      if not FLAGS[flag].present:
        FLAGS[flag].Parse(value)
      elif FLAGS[flag].Type().startswith('multi'):
        old_value = getattr(FLAGS, flag)
        FLAGS[flag].Parse(value)
        setattr(FLAGS, flag, old_value + getattr(FLAGS, flag))


def _ResolveApiInfoFromFlags():
  """Determine an api and api_version."""
  api_version = FLAGS.api_version
  api = FLAGS.api
  return {'api': api, 'api_version': api_version}


def _GetServiceAccountCredentialsFromFlags(storage):
  if not oauth2client.client.HAS_OPENSSL:
    raise app.UsageError(
        'BigQuery requires OpenSSL to be installed in order to use '
        'service account credentials. Please install OpenSSL '
        'and the Python OpenSSL package.')

  if FLAGS.service_account_private_key_file:
    try:
      with file(FLAGS.service_account_private_key_file, 'rb') as f:
        key = f.read()
    except IOError as e:
      raise app.UsageError(
          'Service account specified, but private key in file "%s" '
          'cannot be read:\n%s' % (FLAGS.service_account_private_key_file, e))
  else:
    raise app.UsageError(
        'Service account authorization requires the '
        'service_account_private_key_file flag to be set.')

  credentials = oauth2client.client.SignedJwtAssertionCredentials(
      FLAGS.service_account, key, _CLIENT_SCOPE,
      private_key_password=FLAGS.service_account_private_key_password,
      user_agent=_CLIENT_USER_AGENT)
  credentials.set_store(storage)
  return credentials


def _GetCredentialsFromOAuthFlow(storage):
  print
  print '******************************************************************'
  print '** No OAuth2 credentials found, beginning authorization process **'
  print '******************************************************************'
  print
  if FLAGS.headless:
    print 'Running in headless mode, exiting.'
    sys.exit(1)
  while True:
    # If authorization fails, we want to retry, rather than let this
    # cascade up and get caught elsewhere. If users want out of the
    # retry loop, they can ^C.
    try:
      flow = oauth2client.client.OAuth2WebServerFlow(**_CLIENT_INFO)
      credentials = oauth2client.tools.run(flow, storage)
      break
    except (oauth2client.client.FlowExchangeError, SystemExit), e:
      # Here SystemExit is "no credential at all", and the
      # FlowExchangeError is "invalid" -- usually because you reused
      # a token.
      print 'Invalid authorization: %s' % (e,)
      print
    except httplib2.HttpLib2Error as e:
      print 'Error communicating with server. Please check your internet '
      print 'connection and try again.'
      print
      print 'Error is: %s' % (e,)
      sys.exit(1)
  print
  print '************************************************'
  print '** Continuing execution of BigQuery operation **'
  print '************************************************'
  print
  return credentials


def _GetCredentialsFromFlags():

  intended_perms = stat.S_IWUSR | stat.S_IRUSR
  if not os.path.exists(FLAGS.credential_file):
    try:
      old_umask = os.umask(intended_perms)
      open(FLAGS.credential_file, 'w').close()
      os.umask(old_umask)
    except OSError, e:  # pylint:disable-msg=W0703
      raise bigquery_client.BigqueryError(
          'Cannot create credential file %s: %s' % (FLAGS.credential_file, e))
  try:
    credential_perms = os.stat(FLAGS.credential_file).st_mode
    if credential_perms != intended_perms:
      os.chmod(FLAGS.credential_file, intended_perms)
  except OSError, e:
    raise bigquery_client.BigqueryError(
        'Error setting permissions on %s: %s' % (FLAGS.credential_file, e))
  storage = oauth2client.file.Storage(FLAGS.credential_file)
  try:
    credentials = storage.get()
  except BaseException, e:
    BigqueryCmd.ProcessError(
        e, name='GetCredentialsFromFlags',
        message_prefix=(
            'Credentials appear corrupt. Please delete the credential file '
            'and try your command again. You can delete your credential '
            'file using "bq init --delete_credentials".\n\nIf that does '
            'not work, you may have encountered a bug in the BigQuery CLI.'))
    sys.exit(1)

  if credentials is None or credentials.invalid:
    if FLAGS.service_account:
      credentials = _GetServiceAccountCredentialsFromFlags(storage)
    else:
      credentials = _GetCredentialsFromOAuthFlow(storage)
  return credentials


def _GetFormatterFromFlags(secondary_format='sparse'):
  if FLAGS['format'].present:
    return table_formatter.GetFormatter(FLAGS.format)
  else:
    return table_formatter.GetFormatter(secondary_format)


def _PrintTable(client, table_dict, **extra_args):
  fields, rows = client.ReadSchemaAndRows(table_dict, **extra_args)
  formatter = _GetFormatterFromFlags(secondary_format='pretty')
  formatter.AddFields(fields)
  formatter.AddRows(rows)
  formatter.Print()


def _GetWaitPrinterFactoryFromFlags():
  """Returns the default wait_printer_factory to use while waiting for jobs."""
  if FLAGS.quiet:
    return BigqueryClient.QuietWaitPrinter
  if FLAGS.headless:
    return BigqueryClient.TransitionWaitPrinter
  return BigqueryClient.VerboseWaitPrinter


def _PromptWithDefault(message):
  """Prompts user with message, return key pressed or '' on enter."""
  if FLAGS.headless:
    print 'Running --headless, accepting default for prompt: %s' % (message,)
    return ''
  return raw_input(message).lower()


def _PromptYN(message):
  """Prompts user with message, returning the key 'y', 'n', or '' on enter."""
  response = None
  while response not in ['y', 'n', '']:
    response = _PromptWithDefault(message)
  return response


def _NormalizeFieldDelimiter(field_delimiter):
  """Validates and returns the correct field_delimiter."""
  # The only non-string delimiter we allow is None, which represents
  # no field delimiter specified by the user.
  if field_delimiter is None:
    return field_delimiter
  try:
    # We check the field delimiter flag specifically, since a
    # mis-entered Thorn character generates a difficult to
    # understand error during request serialization time.
    _ = field_delimiter.decode(sys.stdin.encoding or 'utf8')
  except UnicodeDecodeError:
    raise app.UsageError(
        'The field delimiter flag is not valid. Flags must be '
        'specified in your default locale. For example, '
        'the Latin 1 representation of Thorn is byte code FE, '
        'which in the UTF-8 locale would be expressed as C3 BE.')

  # Allow TAB and \\t substitution.
  key = field_delimiter.lower()
  return _DELIMITER_MAP.get(key, field_delimiter)



class Client(object):
  """Class wrapping a singleton bigquery_client.BigqueryClient."""
  client = None

  @staticmethod
  def Create(**kwds):
    """Build a new BigqueryClient configured from kwds and FLAGS."""

    def KwdsOrFlags(name):
      return kwds[name] if name in kwds else getattr(FLAGS, name)

    # Note that we need to handle possible initialization tasks
    # for the case of being loaded as a library.
    _ProcessBigqueryrc()
    bigquery_client.ConfigurePythonLogger(FLAGS.apilog)
    credentials = _GetCredentialsFromFlags()
    assert credentials is not None
    client_args = {}
    global_args = ('credential_file', 'job_property',
                   'project_id', 'dataset_id', 'trace', 'sync',
                   'api', 'api_version')
    for name in global_args:
      client_args[name] = KwdsOrFlags(name)
    client_args['wait_printer_factory'] = _GetWaitPrinterFactoryFromFlags()
    if FLAGS.discovery_file:
      with open(FLAGS.discovery_file) as f:
        client_args['discovery_document'] = f.read()
    return BigqueryClient(credentials=credentials, **client_args)

  @classmethod
  def Get(cls):
    """Return a BigqueryClient initialized from flags."""
    if cls.client is None:
      try:
        cls.client = Client.Create()
      except ValueError, e:
        # Convert constructor parameter errors into flag usage errors.
        raise app.UsageError(e)
    return cls.client

  @classmethod
  def Delete(cls):
    """Delete the existing client.

    This is needed when flags have changed, and we need to force
    client recreation to reflect new flag values.
    """
    cls.client = None


def _Typecheck(obj, types, message=None):  # pylint:disable-msg=W0621
  """Raises a user error if obj is not an instance of types."""
  if not isinstance(obj, types):
    message = message or 'Type of %s is not one of %s' % (obj, types)
    raise app.UsageError(message)


# TODO(user): This code uses more than the average amount of
# Python magic. Explain what the heck is going on throughout.
class NewCmd(appcommands.Cmd):
  """Featureful extension of appcommands.Cmd."""

  def __init__(self, name, flag_values):
    super(NewCmd, self).__init__(name, flag_values)
    run_with_args = getattr(self, 'RunWithArgs', None)
    self._new_style = isinstance(run_with_args, types.MethodType)
    if self._new_style:
      func = run_with_args.im_func
      code = func.func_code
      self._full_arg_list = list(code.co_varnames[:code.co_argcount])
      # TODO(user): There might be some corner case where this
      # is *not* the right way to determine bound vs. unbound method.
      if isinstance(run_with_args.im_self, run_with_args.im_class):
        self._full_arg_list.pop(0)
      self._max_args = len(self._full_arg_list)
      self._min_args = self._max_args - len(func.func_defaults or [])
      self._star_args = bool(code.co_flags & 0x04)
      self._star_kwds = bool(code.co_flags & 0x08)
      if self._star_args:
        self._max_args = sys.maxint
      self._debug_mode = FLAGS.debug_mode
      self.surface_in_shell = True
      self.__doc__ = self.RunWithArgs.__doc__
    elif self.Run.im_func is NewCmd.Run.im_func:
      raise appcommands.AppCommandsError(
          'Subclasses of NewCmd must override Run or RunWithArgs')

  def __getattr__(self, name):
    if name in self._command_flags:
      return self._command_flags[name].value
    return super(NewCmd, self).__getattribute__(name)

  def _GetFlag(self, flagname):
    if flagname in self._command_flags:
      return self._command_flags[flagname]
    else:
      return None

  def Run(self, argv):
    """Run this command.

    If self is a new-style command, we set up arguments and call
    self.RunWithArgs, gracefully handling exceptions. If not, we
    simply call self.Run(argv).

    Args:
      argv: List of arguments as strings.

    Returns:
      0 on success, nonzero on failure.
    """
    if not self._new_style:
      return super(NewCmd, self).Run(argv)

    original_values = self._command_flags.FlagValuesDict()
    try:
      args = self._command_flags(argv)[1:]
      for flag, value in self._command_flags.FlagValuesDict().iteritems():
        setattr(self, flag, value)
        if value == original_values[flag]:
          original_values.pop(flag)
      new_args = []
      for argname in self._full_arg_list[:self._min_args]:
        flag = self._GetFlag(argname)
        if flag is not None and flag.present:
          new_args.append(flag.value)
        elif args:
          new_args.append(args.pop(0))
        else:
          print 'Not enough positional args, still looking for %s' % (argname,)
          if self.usage:
            print 'Usage: %s' % (self.usage,)
          return 1

      new_kwds = {}
      for argname in self._full_arg_list[self._min_args:]:
        flag = self._GetFlag(argname)
        if flag is not None and flag.present:
          new_kwds[argname] = flag.value
        elif args:
          new_kwds[argname] = args.pop(0)

      if args and not self._star_args:
        print 'Too many positional args, still have %s' % (args,)
        return 1
      new_args.extend(args)

      if self._debug_mode:
        return self.RunDebug(new_args, new_kwds)
      else:
        return self.RunSafely(new_args, new_kwds)
    finally:
      for flag, value in original_values.iteritems():
        setattr(self, flag, value)
        self._command_flags[flag].Parse(value)

  def RunCmdLoop(self, argv):
    """Hook for use in cmd.Cmd-based command shells."""
    try:
      args = shlex.split(argv)
    except ValueError, e:
      raise SyntaxError(BigqueryCmd.EncodeForPrinting(e))
    return self.Run([self._command_name] + args)

  def _HandleError(self, e):
    message = str(e)
    if isinstance(e, bigquery_client.BigqueryClientConfigurationError):
      message += ' Try running "bq init".'
    print 'Exception raised in %s operation: %s' % (self._command_name, message)
    return 1

  def RunDebug(self, args, kwds):
    """Run this command in debug mode."""
    try:
      return_value = self.RunWithArgs(*args, **kwds)
    except BaseException, e:
      # Don't break into the debugger for expected exceptions.
      if isinstance(e, app.UsageError) or (
          isinstance(e, bigquery_client.BigqueryError) and
          not isinstance(e, bigquery_client.BigqueryInterfaceError)):
        return self._HandleError(e)
      print
      print '****************************************************'
      print '**  Unexpected Exception raised in bq execution!  **'
      if FLAGS.headless:
        print '**  --headless mode enabled, exiting.             **'
        print '**  See STDERR for traceback.                     **'
      else:
        print '**  --debug_mode enabled, starting pdb.           **'
      print '****************************************************'
      print
      traceback.print_exc()
      print
      if not FLAGS.headless:
        pdb.post_mortem()
      return 1
    return return_value

  def RunSafely(self, args, kwds):
    """Run this command, turning exceptions into print statements."""
    try:
      return_value = self.RunWithArgs(*args, **kwds)
    except BaseException, e:
      return self._HandleError(e)
    return return_value


class BigqueryCmd(NewCmd):
  """Bigquery-specific NewCmd wrapper."""

  def RunSafely(self, args, kwds):
    """Run this command, printing information about any exceptions raised."""
    try:
      return_value = self.RunWithArgs(*args, **kwds)
    except BaseException, e:
      return BigqueryCmd.ProcessError(e, name=self._command_name)
    return return_value

  @staticmethod
  def EncodeForPrinting(s):
    """Safely encode a string as the encoding for sys.stdout."""
    encoding = sys.stdout.encoding or 'ascii'
    return unicode(s).encode(encoding, 'backslashreplace')

  @staticmethod
  def ProcessError(
      e, name='unknown',
      message_prefix='You have encountered a bug in the BigQuery CLI.'):
    """Translate an error message into some printing and a return code."""
    response = []
    retcode = 1

    contact_us_msg = (
        'Please send an email to bigquery-team@google.com to '
        'report this, with the following information: \n\n')
    error_details = (
        '========================================\n'
        '== Platform ==\n'
        '  %s\n'
        '== bq version ==\n'
        '  %s\n'
        '== Command line ==\n'
        '  %s\n'
        '== UTC timestamp ==\n'
        '  %s\n'
        '== Error trace ==\n'
        '%s'
        '========================================\n') % (
            ':'.join([
                platform.python_implementation(),
                platform.python_version(),
                platform.platform()]),
            _Version.VersionNumber(),
            sys.argv,
            time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime()),
            ''.join(traceback.format_tb(sys.exc_info()[2]))
            )

    codecs.register_error('strict', codecs.replace_errors)
    message = BigqueryCmd.EncodeForPrinting(e)
    if isinstance(e, (bigquery_client.BigqueryNotFoundError,
                      bigquery_client.BigqueryDuplicateError)):
      response.append('BigQuery error in %s operation: %s' % (name, message))
      retcode = 2
    elif isinstance(e, bigquery_client.BigqueryTermsOfServiceError):
      response.append(str(e) + '\n')
      response.append(_BIGQUERY_TOS_MESSAGE)
    elif isinstance(e, bigquery_client.BigqueryInvalidQueryError):
      response.append('Error in query string: %s' % (message,))
    elif (isinstance(e, bigquery_client.BigqueryError)
          and not isinstance(e, bigquery_client.BigqueryInterfaceError)):
      response.append('BigQuery error in %s operation: %s' % (name, message))
    elif isinstance(e, (app.UsageError, TypeError)):
      response.append(message)
    elif (isinstance(e, SyntaxError) or
          isinstance(e, bigquery_client.BigquerySchemaError)):
      response.append('Invalid input: %s' % (message,))
    elif isinstance(e, flags.FlagsError):
      response.append('Error parsing command: %s' % (message,))
    elif isinstance(e, KeyboardInterrupt):
      response.append('')
    else:  # pylint:disable-msg=W0703
      # Errors with traceback information are printed here.
      # The traceback module has nicely formatted the error trace
      # for us, so we don't want to undo that via TextWrap.
      if isinstance(e, bigquery_client.BigqueryInterfaceError):
        message_prefix = (
            'Bigquery service returned an invalid reply in %s operation: %s.'
            '\n\n'
            'Please make sure you are using the latest version '
            'of the bq tool and try again. '
            'If this problem persists, you may have encountered a bug in the '
            'bigquery client.' % (name, message))
      elif isinstance(e, oauth2client.client.Error):
        message_prefix = (
            'Authorization error. This may be a network connection problem, '
            'so please try again. If this problem persists, the credentials '
            'may be corrupt. Try deleting and re-creating your credentials. '
            'You can delete your credentials using '
            '"bq init --delete_credentials".'
            '\n\n'
            'If this problem still occurs, you may have encountered a bug '
            'in the bigquery client.')
      elif (isinstance(e, httplib.HTTPException)
            or isinstance(e, apiclient.errors.Error)
            or isinstance(e, httplib2.HttpLib2Error)):
        message_prefix = (
            'Network connection problem encountered, please try again.'
            '\n\n'
            'If this problem persists, you may have encountered a bug in the '
            'bigquery client.')

      print flags.TextWrap(message_prefix + ' ' + contact_us_msg)
      print error_details
      response.append('Unexpected exception in %s operation: %s' % (
          name, message))

    print flags.TextWrap('\n'.join(response))
    return retcode

  def PrintJobStartInfo(self, job):
    """Print a simple status line."""
    reference = BigqueryClient.ConstructObjectReference(job)
    print 'Successfully started %s %s' % (self._command_name, reference)


class _Load(BigqueryCmd):
  usage = """load <destination_table> <source> <schema>"""

  def __init__(self, name, fv):
    super(_Load, self).__init__(name, fv)
    flags.DEFINE_string(
        'field_delimiter', None,
        'The character that indicates the boundary between columns in the '
        'input file. "\\t" and "tab" are accepted names for tab.',
        short_name='F', flag_values=fv)
    flags.DEFINE_enum(
        'encoding', None,
        ['UTF-8', 'ISO-8859-1'],
        'The character encoding used by the input file.  Options include:'
        '\n ISO-8859-1 (also known as Latin-1)'
        '\n UTF-8',
        short_name='E', flag_values=fv)
    flags.DEFINE_integer(
        'skip_leading_rows', None,
        'The number of rows at the beginning of the source file to skip.',
        flag_values=fv)
    flags.DEFINE_string(
        'schema', None,
        'Either a filename or a comma-separated list of fields in the form '
        'name[:type].',
        flag_values=fv)
    flags.DEFINE_boolean(
        'replace', False,
        'If true erase existing contents before loading new data.',
        flag_values=fv)
    flags.DEFINE_integer(
        'max_bad_records', 0,
        'Maximum number of bad records allowed before the entire job fails.',
        flag_values=fv)
    flags.DEFINE_boolean(
        'allow_quoted_newlines', None,
        'Whether to allow quoted newlines in CSV import data.',
        flag_values=fv)

  def RunWithArgs(self, destination_table, source, schema=None):
    """Perform a load operation of source into destination_table.

    Usage:
      load <destination_table> <source> [<schema>]

    The <destination_table> is the fully-qualified table name of table to
    create, or append to if the table already exists.

    The <source> argument can be a path to a single local file, or a
    comma-separated list of URIs.

    The <schema> argument should be either the name of a JSON file or a text
    schema. This schema should be omitted if the table already has one.

    In the case that the schema is provided in text form, it should be a
    comma-separated list of entries of the form name[:type], where type will
    default to string if not specified.

    In the case that <schema> is a filename, it should contain a
    single array object, each entry of which should be an object with
    properties 'name', 'type', and (optionally) 'mode'. See the online
    documentation for more detail:
      https://code.google.com/apis/bigquery/docs/uploading.html#createtable

    Note: the case of a single-entry schema with no type specified is
    ambiguous; one can use name:string to force interpretation as a
    text schema.

    Examples:
      bq load ds.new_tbl ./info.csv ./info_schema.json
      bq load ds.new_tbl gs://mybucket/info.csv ./info_schema.json
      bq load ds.small gs://mybucket/small.csv name:integer,value:string
      bq load ds.small gs://mybucket/small.csv field1,field2,field3

    Arguments:
      destination_table: Destination table name.
      source: Name of local file to import, or a comma-separated list of
        URI paths to data to import.
      schema: Either a text schema or JSON file, as above.
    """
    client = Client.Get()
    table_reference = client.GetTableReference(destination_table)
    opts = {
        'encoding': self.encoding,
        'skip_leading_rows': self.skip_leading_rows,
        'max_bad_records': self.max_bad_records,
        'allow_quoted_newlines': self.allow_quoted_newlines,
        'job_id': FLAGS.job_id,
        }
    if self.replace:
      opts['write_disposition'] = 'WRITE_TRUNCATE'
    if self.field_delimiter:
      opts['field_delimiter'] = _NormalizeFieldDelimiter(self.field_delimiter)

    job = client.Load(table_reference, source, schema=schema, **opts)
    if not FLAGS.sync:
      self.PrintJobStartInfo(job)


class _Query(BigqueryCmd):
  usage = """query <sql>"""

  def __init__(self, name, fv):
    super(_Query, self).__init__(name, fv)
    flags.DEFINE_string(
        'destination_table', '',
        'Name of destination table for query results.',
        flag_values=fv)
    flags.DEFINE_integer(
        'max_rows', 100,
        'How many rows to return in the result.',
        flag_values=fv)
    flags.DEFINE_boolean(
        'batch', False,
        'Whether to run the query in batch mode.',
        flag_values=fv)

  def RunWithArgs(self, *args):
    """Execute a query.

    Examples:
      bq query 'select count(*) from publicdata:samples.shakespeare'

    Usage:
      query <sql_query>
    """
    kwds = {
        'destination_table': self.destination_table,
        'job_id': FLAGS.job_id,
        }
    if self.batch:
      kwds['priority'] = 'BATCH'
    client = Client.Get()
    job = client.Query(' '.join(args), **kwds)
    if not FLAGS.sync:
      self.PrintJobStartInfo(job)
    else:
      _PrintTable(client, job['configuration']['query']['destinationTable'],
                  max_rows=self.max_rows)


class _Extract(BigqueryCmd):
  usage = """extract <source_table> <destination_uri>"""

  def __init__(self, name, fv):
    super(_Extract, self).__init__(name, fv)
    flags.DEFINE_string(
        'field_delimiter', None,
        'The character that indicates the boundary between columns in the '
        'output file. "\\t" and "tab" are accepted names for tab.',
        short_name='F', flag_values=fv)

  def RunWithArgs(self, source_table, destination_uri):
    """Perform an extract operation of source_table into destination_uri.

    Usage:
      extract <source_table> <destination_uri>

    Examples:
      bq extract ds.summary gs://mybucket/summary.csv

    Arguments:
      source_table: Source table to extract.
      destination_uri: Google Storage uri.
    """
    client = Client.Get()
    kwds = {
        'job_id': FLAGS.job_id,
        }
    table_reference = client.GetTableReference(source_table)
    job = client.Extract(
        table_reference, destination_uri,
        field_delimiter=_NormalizeFieldDelimiter(self.field_delimiter),
    **kwds)
    if not FLAGS.sync:
      self.PrintJobStartInfo(job)


class _List(BigqueryCmd):
  usage = """ls [(-j|-p|-d)] [-n <number>] [<identifier>]"""

  def __init__(self, name, fv):
    super(_List, self).__init__(name, fv)
    flags.DEFINE_boolean(
        'jobs', False,
        'Show jobs described by this identifier.',
        short_name='j', flag_values=fv)
    flags.DEFINE_integer(
        'max_results', None,
        'Maximum number to list.',
        short_name='n', flag_values=fv)
    flags.DEFINE_boolean(
        'projects', False,
        'Show all projects.',
        short_name='p', flag_values=fv)
    flags.DEFINE_boolean(
        'datasets', False,
        'Show datasets described by this identifier.',
        short_name='d', flag_values=fv)

  def RunWithArgs(self, identifier=''):
    """List the objects contained in the named collection.

    List the objects in the named project or dataset. A trailing : or
    . can be used to signify a project or dataset.
     * With -j, show the jobs in the named project.
     * With -p, show all projects.

    Examples:
      bq ls
      bq ls -j proj
      bq ls -p -n 1000
      bq ls mydataset
    """
    # pylint:disable-msg=C6115
    if self.j and self.p:
      raise app.UsageError(
          'Cannot specify more than one of -j and -p.')
    if self.p and identifier:
      raise app.UsageError('Cannot specify an identifier with -p')

    client = Client.Get()
    formatter = _GetFormatterFromFlags()
    if identifier:
      reference = client.GetReference(identifier)
    else:
      try:
        reference = client.GetReference(identifier)
      except bigquery_client.BigqueryError:
        # We want to let through the case of no identifier, which
        # will fall through to the second case below.
        reference = None
    # If we got a TableReference, we might be able to make sense
    # of it as a DatasetReference, as in 'ls foo' with dataset_id
    # set.
    if isinstance(reference, TableReference):
      try:
        reference = client.GetDatasetReference(identifier)
      except bigquery_client.BigqueryError:
        pass
    _Typecheck(reference, (types.NoneType, ProjectReference, DatasetReference),
               ('Invalid identifier "%s" for ls, cannot call list on object '
                'of type %s') % (identifier, type(reference).__name__))

    if self.d and isinstance(reference, DatasetReference):
      reference = reference.GetProjectReference()

    if self.j:
      reference = client.GetProjectReference(identifier)
      _Typecheck(reference, ProjectReference,
                 'Cannot determine job(s) associated with "%s"' % (identifier,))
      project_reference = client.GetProjectReference(identifier)
      BigqueryClient.ConfigureFormatter(formatter, JobReference)
      results = map(  # pylint:disable-msg=C6402
          client.FormatJobInfo,
          client.ListJobs(reference=project_reference,
                          max_results=self.max_results))
    elif self.p or reference is None:
      BigqueryClient.ConfigureFormatter(formatter, ProjectReference)
      results = map(  # pylint:disable-msg=C6402
          client.FormatProjectInfo,
          client.ListProjects(max_results=self.max_results))
    elif isinstance(reference, ProjectReference):
      BigqueryClient.ConfigureFormatter(formatter, DatasetReference)
      results = map(  # pylint:disable-msg=C6402
          client.FormatDatasetInfo,
          client.ListDatasets(reference, max_results=self.max_results))
    else:  # isinstance(reference, DatasetReference):
      BigqueryClient.ConfigureFormatter(formatter, TableReference)
      results = map(  # pylint:disable-msg=C6402
          client.FormatTableInfo,
          client.ListTables(reference, max_results=self.max_results))

    for result in results:
      formatter.AddDict(result)
    formatter.Print()


class _Delete(BigqueryCmd):
  usage = """rm [-f] [-r] [(-d|-t)] <identifier>"""

  def __init__(self, name, fv):
    super(_Delete, self).__init__(name, fv)
    flags.DEFINE_boolean(
        'dataset', False,
        'Remove dataset described by this identifier.',
        short_name='d', flag_values=fv)
    flags.DEFINE_boolean(
        'table', False,
        'Remove table described by this identifier.',
        short_name='t', flag_values=fv)
    flags.DEFINE_boolean(
        'force', False,
        "Ignore existing tables and datasets, don't prompt.",
        short_name='f', flag_values=fv)
    flags.DEFINE_boolean(
        'recursive', False,
        'Remove dataset and any tables it may contain.',
        short_name='r', flag_values=fv)

  def RunWithArgs(self, identifier):
    """Delete the dataset or table described by identifier.

    Always requires an identifier, unlike the show and ls commands.
    By default, also requires confirmation before deleting. Supports
    the -d and -t flags to signify that the identifier is a dataset
    or table.
     * With -f, don't ask for confirmation before deleting.
     * With -r, remove all tables in the named dataset.

    Examples:
      bq rm ds.table
      bq rm -r -f old_dataset
    """
    client = Client.Get()

    # pylint:disable-msg=C6115
    if self.d and self.t:
      raise app.UsageError('Cannot specify more than one of -d and -t.')
    if not identifier:
      raise app.UsageError('Must provide an identifier for rm.')

    if self.t:
      reference = client.GetTableReference(identifier)
    elif self.d:
      reference = client.GetDatasetReference(identifier)
    else:
      reference = client.GetReference(identifier)
      _Typecheck(reference, (DatasetReference, TableReference),
                 'Invalid identifier "%s" for rm.' % (identifier,))

    if isinstance(reference, TableReference) and self.r:
      raise app.UsageError(
          'Cannot specify -r with %r' % (reference,))

    if not self.force:
      if ((isinstance(reference, DatasetReference) and
           client.DatasetExists(reference)) or
          (isinstance(reference, TableReference)
           and client.TableExists(reference))):
        if 'y' != _PromptYN('rm: remove %r? (y/N) ' % (reference,)):
          print 'NOT deleting %r, exiting.' % (reference,)
          return 0

    if isinstance(reference, DatasetReference):
      client.DeleteDataset(reference,
                           ignore_not_found=self.force,
                           delete_contents=self.recursive)
    elif isinstance(reference, TableReference):
      client.DeleteTable(reference,
                         ignore_not_found=self.force)


class _Copy(BigqueryCmd):
  usage = """cp [-n] <source_table> <dest_table>"""

  def __init__(self, name, fv):
    super(_Copy, self).__init__(name, fv)
    flags.DEFINE_boolean(
        'no_clobber', False,
        'Do not overwrite an existing table.',
        short_name='n', flag_values=fv)
    flags.DEFINE_boolean(
        'force', False,
        "Ignore existing destination tables, don't prompt.",
        short_name='f', flag_values=fv)

  def RunWithArgs(self, source_table, dest_table):
    """Copies one table to another.

    Examples:
      bq cp dataset.old_table dataset2.new_table
    """
    client = Client.Get()
    source_reference = client.GetTableReference(source_table)
    dest_reference = client.GetTableReference(dest_table)

    if self.no_clobber:
      write_disposition = 'WRITE_EMPTY'
      ignore_already_exists = True
    else:
      write_disposition = 'WRITE_TRUNCATE'
      ignore_already_exists = False
      if not self.force:
        if client.TableExists(dest_reference):
          if 'y' != _PromptYN('cp: replace %r? (y/N) ' % (dest_reference,)):
            print 'NOT copying %r, exiting.' % (source_reference,)
            return 0
    kwds = {
        'write_disposition': write_disposition,
        'ignore_already_exists': ignore_already_exists,
        'job_id': FLAGS.job_id,
        }
    job = client.CopyTable(source_reference, dest_reference, **kwds)
    if job is None:
      print "Table '%s' already exists, skipping" % (dest_reference,)
    elif not FLAGS.sync:
      self.PrintJobStartInfo(job)
    else:
      print "Table '%s' successfully copied to '%s'" % (
          source_reference, dest_reference)


class _Make(BigqueryCmd):
  usage = """mk [-d] <identifier>  OR  mk [-t] <identifier> [<schema>]"""

  def __init__(self, name, fv):
    super(_Make, self).__init__(name, fv)
    flags.DEFINE_boolean(
        'force', False,
        'Ignore errors reporting that the object already exists.',
        short_name='f', flag_values=fv)
    flags.DEFINE_boolean(
        'dataset', False,
        'Create dataset with this name.',
        short_name='d', flag_values=fv)
    flags.DEFINE_boolean(
        'table', False,
        'Create table with this name.',
        short_name='t', flag_values=fv)
    flags.DEFINE_string(
        'schema', '',
        'Either a filename or a comma-separated list of fields in the form '
        'name[:type].',
        flag_values=fv)
    flags.DEFINE_string(
        'description', None,
        'Description of the dataset or table.',
        flag_values=fv)
    flags.DEFINE_integer(
        'expiration', None,
        'Expiration time, in seconds from now, of a table.',
        flag_values=fv)

  def RunWithArgs(self, identifier='', schema=''):
    # pylint:disable-msg=C6115
    """Create a dataset or table with this name.

    See 'bq help load' for more information on specifying the schema.

    Examples:
      bq mk new_dataset
      bq mk new_dataset.new_table
      bq --dataset_id=new_dataset mk table
      bq mk -t new_dataset.newtable name:integer,value:string
    """
    client = Client.Get()

    if self.d and self.t:
      raise app.UsageError('Cannot specify both -d and -t.')

    if self.t:
      reference = client.GetTableReference(identifier)
    elif self.d or not identifier:
      reference = client.GetDatasetReference(identifier)
    else:
      reference = client.GetReference(identifier)
      _Typecheck(reference, (DatasetReference, TableReference),
                 "Invalid identifier '%s' for mk." % (identifier,))
    if isinstance(reference, DatasetReference):
      if self.schema:
        raise app.UsageError('Cannot specify schema with a dataset.')
      if self.expiration:
        raise app.UsageError('Cannot specify an expiration for a dataset.')
      if client.DatasetExists(reference):
        message = "Dataset '%s' already exists." % (reference,)
        if not self.f:
          raise bigquery_client.BigqueryError(message)
        else:
          print message
          return
      client.CreateDataset(reference, ignore_existing=True,
                           description=self.description)
      print "Dataset '%s' successfully created." % (reference,)
    elif isinstance(reference, TableReference):
      if client.TableExists(reference):
        message = "Table '%s' already exists." % (reference,)
        if not self.f:
          raise bigquery_client.BigqueryError(message)
        else:
          print message
          return
      if schema:
        schema = bigquery_client.BigqueryClient.ReadSchema(schema)
      else:
        schema = None
      expiration = None
      if self.expiration:
        expiration = int(self.expiration + time.time()) * 1000
      client.CreateTable(reference, ignore_existing=True, schema=schema,
                         description=self.description,
                         expiration=expiration)
      print "Table '%s' successfully created." % (reference,)


class _Update(BigqueryCmd):
  usage = """update [-d] [-t] <identifier> [<schema>]"""

  def __init__(self, name, fv):
    super(_Update, self).__init__(name, fv)
    flags.DEFINE_boolean(
        'dataset', False,
        'Updates a dataset with this name.',
        short_name='d', flag_values=fv)
    flags.DEFINE_boolean(
        'table', False,
        'Updates a table with this name.',
        short_name='t', flag_values=fv)
    flags.DEFINE_string(
        'schema', '',
        'Either a filename or a comma-separated list of fields in the form '
        'name[:type].',
        flag_values=fv)
    flags.DEFINE_string(
        'description', None,
        'Description of the dataset or table.',
        flag_values=fv)
    flags.DEFINE_integer(
        'expiration', None,
        'Expiration time, in seconds from now, of a table.',
        flag_values=fv)

  def RunWithArgs(self, identifier='', schema=''):
    # pylint:disable-msg=C6115
    """Updates a dataset or table with this name.

    See 'bq help load' for more information on specifying the schema.

    Examples:
      bq update --description "Dataset description" existing_dataset
      bq update --description "My table" dataset.table
      bq update -t new_dataset.newtable name:integer,value:string
    """
    client = Client.Get()

    if self.d and self.t:
      raise app.UsageError('Cannot specify both -d and -t.')

    if self.t:
      reference = client.GetTableReference(identifier)
    elif self.d or not identifier:
      reference = client.GetDatasetReference(identifier)
    else:
      reference = client.GetReference(identifier)
      _Typecheck(reference, (DatasetReference, TableReference),
                 "Invalid identifier '%s' for mk." % (identifier,))
    if isinstance(reference, DatasetReference):
      if self.schema:
        raise app.UsageError('Cannot specify schema with a dataset.')
      if self.expiration:
        raise app.UsageError('Cannot specify an expiration for a dataset.')
      client.UpdateDataset(reference, description=self.description)
      print "Dataset '%s' successfully updated." % (reference,)
    elif isinstance(reference, TableReference):
      if schema:
        schema = bigquery_client.BigqueryClient.ReadSchema(schema)
      else:
        schema = None
      expiration = None
      if self.expiration:
        expiration = int(self.expiration + time.time()) * 1000
      client.UpdateTable(reference, schema=schema,
                         description=self.description,
                         expiration=expiration)
      print "Table '%s' successfully updated." % (reference,)


class _Show(BigqueryCmd):
  usage = """show [<identifier>]"""

  def __init__(self, name, fv):
    super(_Show, self).__init__(name, fv)
    flags.DEFINE_boolean(
        'job', False,
        'If true, interpret this identifier as a job id.',
        short_name='j', flag_values=fv)
    flags.DEFINE_boolean(
        'dataset', False,
        'Show dataset with this name.',
        short_name='d', flag_values=fv)

  def RunWithArgs(self, identifier=''):
    """Show all information about an object.

    Examples:
      bq show -j <job_id>
      bq show dataset
      bq show dataset.table
    """
    # pylint:disable-msg=C6115
    client = Client.Get()
    if self.j:
      reference = client.GetJobReference(identifier)
    elif self.d:
      reference = client.GetDatasetReference(identifier)
    else:
      reference = client.GetReference(identifier)
    if reference is None:
      raise app.UsageError('Must provide an identifier for show.')

    object_info = client.GetObjectInfo(reference)

    # The JSON formats are handled separately so that they don't print
    # the record as a list of one record.
    if FLAGS.format == 'prettyjson':
      print json.dumps(object_info, sort_keys=True, indent=2)
    elif FLAGS.format == 'json':
      print json.dumps(object_info, separators=(',', ':'))
    elif FLAGS.format in [None, 'sparse', 'pretty']:
      formatter = _GetFormatterFromFlags()
      BigqueryClient.ConfigureFormatter(
          formatter, type(reference), print_format='show')
      object_info = BigqueryClient.FormatInfoByKind(object_info)
      formatter.AddDict(object_info)
      print '%s %s\n' % (reference.typename.capitalize(), reference)
      formatter.Print()
      print
      if (isinstance(reference, JobReference) and
          object_info['State'] == 'FAILURE'):
        error_result = object_info['status']['errorResult']
        error_ls = object_info['status'].get('errors', [])
        error = bigquery_client.BigqueryError.Create(
            error_result, error_result, error_ls)
        print 'Errors encountered during job execution. %s\n' % (error,)
    else:
      formatter = _GetFormatterFromFlags()
      formatter.AddColumns(object_info.keys())
      formatter.AddDict(object_info)
      formatter.Print()


class _Head(BigqueryCmd):
  usage = """head [-n <max rows>] [<table identifier>]"""

  def __init__(self, name, fv):
    super(_Head, self).__init__(name, fv)
    flags.DEFINE_integer(
        'max_rows', 100,
        'The number of rows to print when showing table data.',
        short_name='n', flag_values=fv)

  def RunWithArgs(self, identifier=''):
    """Displays rows in a table.

    Examples:
      bq head dataset.table
      bq head -n 10 dataset.table
    """
    client = Client.Get()
    reference = client.GetReference(identifier)
    _Typecheck(reference, (types.NoneType, TableReference),
               'Must provide a table identifier for head.')
    _PrintTable(client, dict(reference), max_rows=self.max_rows)


class _Wait(BigqueryCmd):
  usage = """wait [<job_id>] [<secs>]"""

  def RunWithArgs(self, job_id='', secs=sys.maxint):
    # pylint:disable-msg=C6115
    """Wait some number of seconds for a job to finish.

    Poll job_id until either (1) the job is DONE or (2) the
    specified number of seconds have elapsed. Waits forever
    if unspecified. If no job_id is specified, and there is
    only one running job, we poll that job.

    Examples:
      bq wait # Waits forever for the currently running job.
      bq wait job_id  # Waits forever
      bq wait job_id 100  # Waits 100 seconds
      bq wait job_id 0  # See if a job is done.

    Arguments:
      job_id: Job ID to wait on.
      secs: Number of seconds to wait (must be >= 0).
    """
    try:
      secs = BigqueryClient.NormalizeWait(secs)
    except ValueError:
      raise app.UsageError('Invalid wait time: %s' % (secs,))

    client = Client.Get()
    if not job_id:
      running_jobs = client.ListJobRefs(state_filter=['PENDING', 'RUNNING'])
      if len(running_jobs) != 1:
        raise bigquery_client.BigqueryError(
            'No job_id provided, found %d running jobs' % (len(running_jobs),))
      job_reference = running_jobs.pop()
    else:
      job_reference = client.GetJobReference(job_id)

    client.WaitJob(job_reference=job_reference, wait=secs)


# pylint:disable-msg=C6409
class CommandLoop(cmd.Cmd):
  """Instance of cmd.Cmd built to work with NewCmd."""

  class TerminateSignal(Exception):
    """Exception type used for signaling loop completion."""
    pass

  def __init__(self, commands, prompt=None):
    cmd.Cmd.__init__(self)
    self._commands = {'help': commands['help']}
    self._special_command_names = ['help', 'repl', 'EOF']
    for name, command in commands.iteritems():
      if (name not in self._special_command_names and
          isinstance(command, NewCmd) and
          command.surface_in_shell):
        self._commands[name] = command
        setattr(self, 'do_%s' % (name,), command.RunCmdLoop)
    self._default_prompt = prompt or 'BigQuery> '
    self._set_prompt()
    self._last_return_code = 0

  @property
  def last_return_code(self):
    return self._last_return_code

  def _set_prompt(self):
    client = Client().Get()
    if client.project_id:
      path = str(client.GetReference())
      self.prompt = '%s> ' % (path,)
    else:
      self.prompt = self._default_prompt

  def do_EOF(self, *unused_args):
    """Terminate the running command loop.

    This function raises an exception to avoid the need to do
    potentially-error-prone string parsing inside onecmd.

    Returns:
      Never returns.

    Raises:
      CommandLoop.TerminateSignal: always.
    """
    raise CommandLoop.TerminateSignal()

  def postloop(self):
    print 'Goodbye.'

  def completedefault(self, unused_text, line, unused_begidx, unused_endidx):
    if not line:
      return []
    else:
      command_name = line.partition(' ')[0].lower()
      usage = ''
      if command_name in self._commands:
        usage = self._commands[command_name].usage
      elif command_name == 'set':
        usage = 'set (project_id|dataset_id) <name>'
      elif command_name == 'unset':
        usage = 'unset (project_id|dataset_id)'
      if usage:
        print
        print usage
        print '%s%s' % (self.prompt, line),
      return []

  def emptyline(self):
    print 'Available commands:',
    print ' '.join(list(self._commands))

  def precmd(self, line):
    """Preprocess the shell input."""
    if line == 'EOF':
      return line
    if line.startswith('exit') or line.startswith('quit'):
      return 'EOF'
    words = line.strip().split()
    if len(words) > 1 and words[0].lower() == 'select':
      return 'query %s' % (pipes.quote(line),)
    if len(words) == 1 and words[0] not in ['help', 'ls', 'version']:
      return 'help %s' % (line.strip(),)
    return line

  def onecmd(self, line):
    """Process a single command.

    Runs a single command, and stores the return code in
    self._last_return_code. Always returns False unless the command
    was EOF.

    Args:
      line: (str) Command line to process.

    Returns:
      A bool signaling whether or not the command loop should terminate.
    """
    try:
      self._last_return_code = cmd.Cmd.onecmd(self, line)
    except CommandLoop.TerminateSignal:
      return True
    except BaseException, e:
      name = line.split(' ')[0]
      BigqueryCmd.ProcessError(e, name=name)
      self._last_return_code = 1
    return False

  def get_names(self):
    names = dir(self)
    commands = (name for name in self._commands
                if name not in self._special_command_names)
    names.extend('do_%s' % (name,) for name in commands)
    names.append('do_select')
    names.remove('do_EOF')
    return names

  def do_set(self, line):
    """Set the value of the project_id or dataset_id flag."""
    client = Client().Get()
    name, value = (line.split(' ') + ['', ''])[:2]
    if (name not in ('project_id', 'dataset_id') or
        not 1 <= len(line.split(' ')) <= 2):
      print 'set (project_id|dataset_id) <name>'
    elif name == 'dataset_id' and not client.project_id:
      print 'Cannot set dataset_id with project_id unset'
    else:
      setattr(client, name, value)
      self._set_prompt()
    return 0

  def do_unset(self, line):
    """Unset the value of the project_id or dataset_id flag."""
    name = line.strip()
    client = Client.Get()
    if name not in ('project_id', 'dataset_id'):
      print 'unset (project_id|dataset_id)'
    else:
      setattr(client, name, '')
      if name == 'project_id':
        client.dataset_id = ''
      self._set_prompt()
    return 0

  def do_help(self, command_name):
    """Print the help for command_name (if present) or general help."""

    # TODO(user): Add command-specific flags.
    def FormatOneCmd(name, command, command_names):
      indent_size = appcommands.GetMaxCommandLength() + 3
      if len(command_names) > 1:
        indent = ' ' * indent_size
        command_help = flags.TextWrap(
            command.CommandGetHelp('', cmd_names=command_names),
            indent=indent,
            firstline_indent='')
        first_help_line, _, rest = command_help.partition('\n')
        first_line = '%-*s%s' % (indent_size, name + ':', first_help_line)
        return '\n'.join((first_line, rest))
      else:
        default_indent = '  '
        return '\n' + flags.TextWrap(
            command.CommandGetHelp('', cmd_names=command_names),
            indent=default_indent,
            firstline_indent=default_indent) + '\n'

    if not command_name:
      print '\nHelp for Bigquery commands:\n'
      command_names = list(self._commands)
      print '\n\n'.join(
          FormatOneCmd(name, command, command_names)
          for name, command in self._commands.iteritems()
          if name not in self._special_command_names)
      print
    elif command_name in self._commands:
      print FormatOneCmd(command_name, self._commands[command_name],
                         command_names=[command_name])
    return 0

  def postcmd(self, stop, line):
    return bool(stop) or line == 'EOF'
# pylint:enable-msg=C6409


class _Repl(BigqueryCmd):
  """Start an interactive bq session."""

  def __init__(self, name, fv):
    super(_Repl, self).__init__(name, fv)
    self.surface_in_shell = False
    flags.DEFINE_string(
        'prompt', '',
        'Prompt to use for BigQuery shell.',
        flag_values=fv)

  def RunWithArgs(self):
    """Start an interactive bq session."""
    repl = CommandLoop(appcommands.GetCommandList(), prompt=self.prompt)
    print 'Welcome to BigQuery! (Type help for more information.)'
    while True:
      try:
        repl.cmdloop()
        break
      except KeyboardInterrupt:
        print
    return repl.last_return_code


class _Init(BigqueryCmd):
  """Create a .bigqueryrc file and set up OAuth credentials."""

  def __init__(self, name, fv):
    super(_Init, self).__init__(name, fv)
    self.surface_in_shell = False
    flags.DEFINE_boolean(
        'delete_credentials', None,
        'If specified, the credentials file associated with this .bigqueryrc '
        'file is deleted.',
        flag_values=fv)

  def DeleteCredentials(self):
    """Deletes this user's credential file."""
    _ProcessBigqueryrc()
    filename = FLAGS.credential_file
    if not os.path.exists(filename):
      print 'Credential file %s does not exist.' % (filename,)
      return 0
    try:
      if 'y' != _PromptYN('Delete credential file %s? (y/N) ' % (filename,)):
        print 'NOT deleting %s, exiting.' % (filename,)
        return 0
      os.remove(filename)
    except OSError, e:
      print 'Error removing %s: %s' % (filename, e)
      return 1

  def RunWithArgs(self):
    """Authenticate and create a default .bigqueryrc file."""
    _ProcessBigqueryrc()
    bigquery_client.ConfigurePythonLogger(FLAGS.apilog)
    if self.delete_credentials:
      return self.DeleteCredentials()
    bigqueryrc = _GetBigqueryRcFilename()
    # Delete the old one, if it exists.
    print
    print 'Welcome to BigQuery! This script will walk you through the '
    print 'process of initializing your .bigqueryrc configuration file.'
    print
    if os.path.exists(bigqueryrc):
      print ' **** NOTE! ****'
      print 'An existing .bigqueryrc file was found at %s.' % (bigqueryrc,)
      print 'Are you sure you want to continue and overwrite your existing '
      print 'configuration?'
      print

      if 'y' != _PromptYN('Overwrite %s? (y/N) ' % (bigqueryrc,)):
        print 'NOT overwriting %s, exiting.' % (bigqueryrc,)
        return 0
      print
      try:
        os.remove(bigqueryrc)
      except OSError, e:
        print 'Error removing %s: %s' % (bigqueryrc, e)
        return 1

    print 'First, we need to set up your credentials if they do not '
    print 'already exist.'
    print

    client = Client.Get()
    entries = {'credential_file': FLAGS.credential_file}
    projects = client.ListProjects()
    print 'Credential creation complete. Now we will select a default project.'
    print
    if not projects:
      print 'No projects found for this user. Please go to '
      print '  https://code.google.com/apis/console'
      print 'and create a project.'
      print
    else:
      print 'List of projects:'
      formatter = _GetFormatterFromFlags()
      formatter.AddColumn('#')
      BigqueryClient.ConfigureFormatter(formatter, ProjectReference)
      for index, project in enumerate(projects):
        result = BigqueryClient.FormatProjectInfo(project)
        result.update({'#': index + 1})
        formatter.AddDict(result)
      formatter.Print()

      if len(projects) == 1:
        project_reference = BigqueryClient.ConstructObjectReference(
            projects[0])
        print 'Found only one project, setting %s as the default.' % (
            project_reference,)
        print
        entries['project_id'] = project_reference.projectId
      else:
        print 'Found multiple projects. Please enter a selection for '
        print 'which should be the default, or leave blank to not '
        print 'set a default.'
        print

        response = None
        while not isinstance(response, int):
          response = _PromptWithDefault(
              'Enter a selection (1 - %s): ' % (len(projects),))
          try:
            if not response or 1 <= int(response) <= len(projects):
              response = int(response or 0)
          except ValueError:
            pass
        print
        if response:
          project_reference = BigqueryClient.ConstructObjectReference(
              projects[response - 1])
          entries['project_id'] = project_reference.projectId

    try:
      with open(bigqueryrc, 'w') as rcfile:
        for flag, value in entries.iteritems():
          print >>rcfile, '%s = %s' % (flag, value)
    except IOError, e:
      print 'Error writing %s: %s' % (bigqueryrc, e)
      return 1

    print 'BigQuery configuration complete! Type "bq" to get started.'
    print
    _ProcessBigqueryrc()
    # Destroy the client we created, so that any new client will
    # pick up new flag values.
    Client.Delete()
    return 0


class _Version(BigqueryCmd):
  usage = """version"""

  @staticmethod
  def VersionNumber():
    """Return the version of bq."""
    try:
      import pkg_resources  # pylint:disable-msg=C6204
      version = pkg_resources.get_distribution('bigquery').version
      return 'v%s' % (version,)
    except ImportError:
      return '<unknown>'


  def RunWithArgs(self):
    """Return the version of bq."""
    version = type(self).VersionNumber()
    print 'This is BigQuery CLI %s' % (version,)


def main(argv):
  try:
    FLAGS.auth_local_webserver = False

    appcommands.AddCmd('load', _Load)
    appcommands.AddCmd('query', _Query)
    appcommands.AddCmd('extract', _Extract)
    appcommands.AddCmd('ls', _List)
    appcommands.AddCmd('rm', _Delete)
    appcommands.AddCmd('mk', _Make)
    appcommands.AddCmd('show', _Show)
    appcommands.AddCmd('head', _Head)
    appcommands.AddCmd('wait', _Wait)
    appcommands.AddCmd('cp', _Copy)
    appcommands.AddCmd('update', _Update)

    appcommands.AddCmd('version', _Version)
    appcommands.AddCmd('shell', _Repl)
    appcommands.AddCmd('init', _Init)

    if (not argv or
        (len(argv) > 1 and
         argv[1] not in ['init', 'help', 'version'] and
         argv[1] in appcommands.GetCommandList())):
      # Service Accounts don't use cached oauth credentials and
      # all bigqueryrc defaults are technically optional.
      if not FLAGS.service_account:
        if not (os.path.exists(_GetBigqueryRcFilename()) or
                os.path.exists(FLAGS.credential_file)):
          appcommands.GetCommandByName('init').Run([])
  except KeyboardInterrupt, e:
    print 'Control-C pressed, exiting.'
    sys.exit(1)
  except BaseException, e:  # pylint:disable-msg=W0703
    print 'Error initializing bq client: %s' % (e,)
    if FLAGS.debug_mode or FLAGS.headless:
      traceback.print_exc()
      if not FLAGS.headless:
        pdb.post_mortem()
    sys.exit(1)


# pylint: disable-msg=C6409
def run_main():
  """Function to be used as setuptools script entry point.

  Appcommands assumes that it always runs as __main__, but launching
  via a setuptools-generated entry_point breaks this rule. We do some
  trickery here to make sure that appcommands and flags find their
  state where they expect to by faking ourselves as __main__.
  """

  # Put the flags for this module somewhere the flags module will look
  # for them.
  # pylint: disable-msg=W0212
  new_name = flags._GetMainModule()
  sys.modules[new_name] = sys.modules['__main__']
  for flag in FLAGS.FlagsByModuleDict().get(__name__, []):
    FLAGS._RegisterFlagByModule(new_name, flag)
    for key_flag in FLAGS.KeyFlagsByModuleDict().get(__name__, []):
      FLAGS._RegisterKeyFlagForModule(new_name, key_flag)
  # pylint: enable-msg=W0212

  # Now set __main__ appropriately so that appcommands will be
  # happy.
  sys.modules['__main__'] = sys.modules[__name__]
  appcommands.Run()
  sys.modules['__main__'] = sys.modules.pop(new_name)


if __name__ == '__main__':
  appcommands.Run()
