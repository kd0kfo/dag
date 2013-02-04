#!/usr/bin/env python

from distutils.core import setup,Command

class Tester(Command):
    user_options = []

    def initialize_options(self):
        import os
        self._dir = os.getcwd()

    def finalize_options(self):
        pass

    def run(self):
        import test
        print("Finally, there is a test.")
        if test.run():
            print("Success")
        else:
            print("Failure")
 
setup (name ='boincdag',
       version = '1.0',
       description = 'DAG Batch Job Creator for BOINC',
       author = 'David Coss, PhD',
       author_email = 'David.Coss@stjude.org',
       license = 'GPL v3',
       packages = ['dag'],
       scripts = ["scripts/gsub", "scripts/update_dag"],
       cmdclass ={'test':Tester}
       )
