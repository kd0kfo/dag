#!/usr/bin/env python
"""
dag.lsf
=========


@author: David Coss, PhD
@date: November 7, 2012
@license: GPL version 3 (see COPYING or http://www.gnu.org/licenses/gpl.html for details)

Interface module to LSF. This allows processes in a DAG to interpret and use LSF data.
"""
from dag import DagException

class JobSubmitFailed(DagException):
    pass

class BjobsFailed(DagException):
    pass

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
                 if hasattr(proc,"rsc_memory_bound"):
                     if proc.rsc_memory_bound:
                         inmeg = int(proc.rsc_memory_bound//(1024.**2))
                         script_file.write('#BSUB -R "rusage[mem={0}]" -M {0}\n'.format(inmeg))
                 script_file.write("\n%s\n" % command)

     import dag
     import subprocess

     proc = the_dag.processes[0]
     cmd = get_command_string(proc)
     make_bsub(cmd,proc)

     retval = subprocess.call("bsub < %s.bsub" % proc.workunit_name,shell=True)

     if retval:
         from os.path import join
         from os import getcwd
         filename = join(getcwd,"{0}.bsub".format(proc.workunit_name))
         raise JobSubmitFailed("Could not submit job. Bsub script name: {0}.bsub".format(filename))

     retval = subprocess.call('bsub -w "ended({0})" -J {0}_notifier -app python-2.7.2 update_dag update {0}'.format(proc.workunit_name),shell=True)

     proc.state = dag.States.RUNNING
     the_dag.save()
    

def clean_workunit(root_dag, proc):
    raise Exception("ADD CLEAN FUNCTION!!!!")

def get_state(proc):
    import subprocess as SP
    from dag import States
    bjobs = SP.Popen("bjobs -a -J {0}".format(proc.workunit_name).split(),stdout=SP.PIPE,stderr=SP.PIPE)
    retval = bjobs.wait()
    (stdout,stderr) = bjobs.communicate()
    if retval:
        raise BjobsFailed("Could not get status of job {0}\nRetval: {1}\nMessage: {2}".format(proc.workunit_name,retval,stderr))
    
    if not stdout:
        raise BjobsFailed("Could not get status of job {0}\nRetval: {1}\nMessage: {2}".format(proc.workunit_name,retval,stderr))
    
    if "RUN" in stdout:
        return States.RUNNING
    elif "PEND" in stdout:
        return States.STAGED
    elif "EXIT" in stdout:
        return States.FAIL
    elif "DONE" in stdout:
        return States.SUCCESS
    
    raise BjobsFailed("Could not get status of job {0}\nRetval: {1}\nMessage: {2}".format(proc.workunit_name,retval,stderr))
        
