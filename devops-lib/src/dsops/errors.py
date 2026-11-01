"""One root for every error dsops raises on purpose.

Conventions used across the library:

* Bad arguments (unknown mode, negative batch size, invalid name) raise the
  built-in ``ValueError`` or ``TypeError``, like the standard library does.
* Failures while doing the work raise a subclass of :class:`DsopsError`, so a
  caller can ``except DsopsError`` around a pipeline step and know it caught
  only dsops' own failures, never a bug of its own.
"""


class DsopsError(Exception):
    """Base class for operational errors raised by dsops."""
