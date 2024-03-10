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

.. image:: https://img.shields.io/badge/python-3.8%20%7C%203.9%20%7C%203.10%20%7C%203.11%20%7C%203.12-blue.svg
   :alt: Python versions 3.8 3.9 3.10 3.11 3.12
   :target: https://www.python.org/downloads/

.. image:: https://img.shields.io/badge/badges-4-green.svg
   :alt: 4 badges
   :target: https://shields.io/

Purpose
=======

This is not a file diff program. It does not compare two files and tell you
which lines have changed.

The main use case is checking and reconciling different copies of a large-ish
collection of files. Say for example you have your music collection on two
different devices, and you think you might have messed one of them up somehow,
then you can check how the two different versions compare. Or, if you have
multiple snaphots of the same collection of files (separate backups, say) then
you can see how the collection evolved from version to version.

Changelog
=========

Changes will not be logged prior to release 1.0
