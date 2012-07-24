# Copyright 2012 Google Inc. All Rights Reserved.

"""Any errors occurring during execution.

- Error during query.
- Error during poll.
"""


class Error(Exception):
  """Base exception."""
  pass


class QueryError(Error):
  """Exception raised for errors during query."""
  pass


class PollError(Error):
  """Exception raised for errors during job poll."""
  pass
