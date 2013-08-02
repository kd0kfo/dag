#!/usr/bin/env python

from setuptools import setup, Command


class Tester(Command):
    user_options = []

    def initialize_options(self):
        import os
        self._dir = os.getcwd()

    def finalize_options(self):
        pass

    def run(self):
        import test
        print("Testing dag")
        if test.test_dag_objects():
            print("Success")
        else:
            print("Failure")
            exit(1)

        print("Testing gsub")
        if test.test_gsub():
            print("Success")
        else:
            print("Failure")
            exit(1)

        print("Testing shell processes")
        if test.test_shell_processes():
            print("Success")
        else:
            print("Failure")
            exit(1)

        print("Testing progress bar")
        #test.test_progress_bar()
        print("Did you see a progress bar?")


setup(name='dag',
      version='1.4.0',
      description='DAG Batch Job Creator for BOINC',
      author='David Coss, PhD',
      author_email='David.Coss@stjude.org',
      license='GPL v3',
      packages=['dag'],
      scripts=["scripts/gsub", "scripts/update_dag"],
      cmdclass={'test': Tester}
      )
