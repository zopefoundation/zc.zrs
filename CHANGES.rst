Recent Changes
==============

For earlier changes, see the `HISTORY.rst <HISTORY.rst>`_.

4.0.0 (unreleased)
------------------

- Add support for Python 3.8 and 3.9.

- Reach compatibility with ZODB 5.6

- Drop support for Python 2.7

- Drop support for Python 3.4 and Python 3.5.


3.1.0 (2017-04-07)
------------------

- Python 3 (3.4 and 3.5) support.

  3.6 support will be added when Twisted supports Python 3.6.
  (There are worrying test failures under Python 3.6.)

- Minor convenience change to API: When instantiating primary or
  secondary servers, you can pass a file-name instead of a storage
  instance and a file storage will be created automatically.


3.0.0 (2017-04-04)
------------------

- Add support for ZODB 5

- Drop ZooKeeper support.

