#!/usr/bin/env python

from distutils.core import setup
 
setup (name ='boincdag',
       version = '0.1',
       description = 'DAG Batch Job Creator for BOINC',
       author = 'David Coss, PhD',
       author_email = 'David.Coss@stjude.org',
       license = 'GPL v3',
       py_modules = ['dag','dag_utils','gsub','update_dag']
       )
