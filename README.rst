========
Omnidiff
========

A Python library for managing collections of files.

This library is work in progress, and may not be usable prior to release 1.0.

.. image:: https://github.com/sjjessop/omnidiff/workflows/tests/badge.svg
   :alt: Test status
   :target: https://github.com/sjjessop/omnidiff/actions?query=workflow%3Atests

.. image:: https://codecov.io/gh/sjjessop/omnidiff/branch/develop/graph/badge.svg
   :alt: codecov
   :target: https://codecov.io/gh/sjjessop/omnidiff

.. image:: https://img.shields.io/badge/python-3.7%20%7C%203.8%20%7C%203.9-blue.svg
   :alt: Python versions 3.7 3.8 3.9
   :target: https://www.python.org/downloads/

.. image:: https://img.shields.io/badge/badges-4-green.svg
   :alt: 4 badges
   :target: https://shields.io/

Purpose
=======

Note that this is not a file diff program. It does not compare two files and
tell you which lines have changed.

The main use case is checking and reconciling different copies of a large-ish
collection of files. Say for example you have your music collection on two
different devices, and you think you might have messed one of them up somehow,
then you can check how the two different versions compare. Or, if you have
multiple snaphots of the same collection of files (separate backups, say) then
you can see how the collection evolved from version to version.

Versioning
==========

Version numbers follow `Semantic Versioning <https://semver.org/>`_. However,
the version number in the code might only be updated at the point of creating a
`release tag <https://github.com/sjjessop/omnidiff/tags>`_. So, if you're
working in the repo then the version number does not indicate compatibility
with past releases, except that the tip of the ``release`` branch is always the
current (most recent) release.

Only documented behaviour is considered when evaluating whether a change is
backward-compatible or not. Therefore undocumented behaviour may change with
only a patch-level version number increment.

Behaviour explicitly documented as provisional may change with only a minor
version number increment, so you can depend on provisional behaviour by pinning
to version ``major.minor.*``.

Support for a version of Python that has passed its
`end of support <https://www.python.org/downloads/>`_ may be removed with only
a minor version number increment.

There is only one "release stream", and changes will not be backported to past
major or minor releases.

Compatibility
=============

Omnidice does not work with Python versions 3.6 or lower, because it uses
the ``from __future__ import annotations`` feature new in Python 3.7.

Changelog
=========

Changes will not be logger prior to release 1.0
