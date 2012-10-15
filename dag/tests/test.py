#!/usr/bin/env python
import unittest

class TestDag(unittest.TestCase):
    def setUp(self):
        import os,sys
        from sys import version_info as vi

        req_version = (2,7)
        if vi < req_version:
            raise Exception("Minimum Version Required: %d.%d" % req_version)
        cwd = os.getcwd()
        os.chdir('..')
        sys.path.append(os.getcwd())
        os.chdir(cwd)

        
        
    def test_dag_pickle(self):
        import os.path as OP
        import os
        from dag import DAG,Process,load
        # PICKLE
        d = DAG()
        d.add_process(Process("ple",['a','b'],['c','d'],"-brief"))
        d.add_process(Process("ple",['c','e'],['f']))
        filename = d.save()
        self.assertTrue(OP.isfile(filename))
        
        # UNPICKLE
        try:
            with open(filename,"rb") as file:
                d2 = load(file)
        finally:
            os.unlink(filename)

    def test_boinc_apps(self):
        import dag_utils
        import os
        from os import path as OP

        sep = os.sep
        required_files = ["bin" + sep + "create_work", "bin" + sep + "dir_hier_path"]
        required_dirs = ["bin","templates"]
        
        self.assertIsNotNone(dag_utils.project_path)
        self.assertNotEqual(len(dag_utils.project_path),0)

        last_wd = os.getcwd()
        os.chdir(dag_utils.project_path)
        for dir in required_dirs:
            self.assertTrue(OP.isdir(dir))
        for file in required_files:
            self.assertTrue(OP.isfile(file))

        os.chdir(last_wd)

    def test_create_dag(self):
        from gsub import create_dag as create_dag
        from os import path as OP

        parsers = {}
        
        if OP.isfile("test.sub"):
            thedag = create_dag("test.sub",parsers)
            self.assertIsNotNone(thedag)
        else:
            print("test.sub not provided. Skipping.")
            return


if __name__ == '__main__':
    unittest.main()
