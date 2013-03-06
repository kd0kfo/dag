#!/usr/bin/env python
"""
dag.lsf
=========


@author: David Coss, PhD
@date: November 7, 2012
@license: GPL version 3 (see COPYING or http://www.gnu.org/licenses/gpl.html for details)

Interface module to LSF. This allows processes in a DAG to interpret and use LSF data.
"""


def create_work(the_dag,dagfile, show_progress = False):
    """
    Creates a workunit by processing the dag and running stage_files and schedule_work.

    Sets the workunit information in the dag.Process objects

    Arguments: dag and dagfile path to dag file
    Returns: no value
    Throws Exception if an input file does not exist and is not part of a parent process.
    """

    def get_command_string(proc):
        retval = proc.executable_name
        if proc.args:
            retval += " " + proc.args
        return retval
    
    def make_bsub(command,proc):
        import random
        import os.path as OP
        if not proc.workunit_name:
            proc.workunit_name = proc.cmd + "-" + ("%09d" % int(random.random()*1000000000))
        filename = "%s.bsub" % proc.workunit_name
        if not OP.isfile(filename):
            with open(filename,"w") as script_file:
                script_file.write("#BSUB -J %s\n" % proc.workunit_name)
                script_file.write("#BSUB -P %s\n" % proc.project_name)
                script_file.write("#BSUB -app %s\n" % proc.application_profile)
                script_file.write("#BSUB -eo {0}.err -oo {0}.out\n".format(proc.workunit_name))
                script_file.write("\n%s\n" % command)

    import dag
    import subprocess
    
    proc = the_dag.processes[0]
    cmd = get_command_string(proc)
    make_bsub(cmd,proc)
    
    subprocess.call("bsub < %s.bsub" % proc.workunit_name,shell=True)
    

def clean_workunit(root_dag, proc):
    raise Exception("ADD CLEAN FUNCTION!!!!")
