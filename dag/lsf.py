#!/usr/bin/env python
"""
dag.lsf
=======


@author: David Coss, PhD
@date: May 9, 2013
@license: GPL version 3 (see COPYING or
 http://www.gnu.org/licenses/gpl.html for details)

Interface module to LSF. This allows processes in a DAG to interpret
 and use LSF data.
"""
from dag import DagException, GridProcess


class JobSubmitFailed(DagException):
    pass


class BjobsFailed(DagException):
    pass


class LSFProcess(GridProcess):
    def __init__(self, *args, **kmap):
        super(LSFProcess, self).__init__(*args)
        
        self.executable_name = self.cmd
        self.app_profile = None
        self.project_name = None
        self.host = None
        if 'memory_limit' in kmap:
            self.rsc_memory_limit = int(kmap['memory_limit'][0])
        if 'app' in kmap:
            self.app_profile = kmap['app'][0]
        if 'project_name' in kmap:
            self.project_name = kmap['project_name'][0]
        if 'host' in kmap:
            self.host = kmap['host'][0]


def stage_files(proc, source_dir=None, set_grp_perms=True, overwrite=True):
    """
    Creates a bsub file for the Process

    @param proc: Process to be staged.
    @type proc: dag.Process
    @param source_dir: Optional source directory of input file.
     Default: current working directory
    @type source_dir: str
    @param set_grp_perms: Indicate whether group write permission
     should be explicitly set. False means it is not set by the program,
     but does not mean it is *unset*. Default: True
    @type set_grp_perms: bool
    @param overwrite: Indicate whether or not files should be overwritten.
    @type overwrite: bool
    """
    def get_command_string(proc):
        return "{} {}".format(proc.executable_name, proc.args)

    def make_bsub(command, proc):
        import random
        import os.path as OP
        if not proc.workunit_name:
            proc.workunit_name = proc.cmd + "-" + ("%09d" % int(random.random()
                                                                * 1000000000))
        filename = "%s.bsub" % proc.workunit_name
        if not OP.isfile(filename) or overwrite:
            with open(filename, "w") as script_file:
                script_file.write("#BSUB -J %s\n" % proc.workunit_name)
                if hasattr(proc, "project_name"):
                    script_file.write("#BSUB -P %s\n" % proc.project_name)
                if hasattr(proc, "application_profile"):
                    script_file.write("#BSUB -app %s\n"
                                      % proc.application_profile)
                script_file.write("#BSUB -eo {0}.err -oo {0}.out\n"
                                  .format(proc.workunit_name))
                if hasattr(proc, "rsc_memory_bound"):
                    if proc.rsc_memory_bound:
                        inmeg = int(proc.rsc_memory_bound // (1024. ** 2))
                        script_file.write('#BSUB -R "rusage[mem={0}]" -M {0}\n'
                                          .format(inmeg))
                if hasattr(proc, "nproc"):
                    if proc.nproc > 1:
                        nproc = int(proc.nproc)
                        script_file.write('#BSUB -n {0}'.format(nproc))
                if hasattr(proc, "host"):
                    if proc.host:
                        script_file.write("#BSUB -m {0}\n".format(proc.host))
                script_file.write("\n%s\n" % command)

    cmd = get_command_string(proc)
    make_bsub(cmd, proc)


def create_work(the_dag, dagfile):
    """
    Creates a workunit by processing the dag and running stage_files
     and schedule_work.

    Sets the workunit information in the dag.GridProcess objects

    @param the_dag: DAG
    @type the_dag: dag.DAG
    @param dagfile: Path to dag file
    @type dagfile: str
    Returns: no value
    @raise JobSubmitFailed: If an input file does not exist and is not part
     of a parent process.
    """
    import dag
    import subprocess

    for proc in the_dag.processes:
        if proc.state not in [dag.States.CREATED, dag.States.STAGED]:
            continue
        if the_dag.incomplete_prereqs(proc):
            continue
        if not proc.workunit_name:
            stage_files(proc)
        retval = subprocess.call("bsub < %s.bsub"
                                 % proc.workunit_name, shell=True)

        if retval:
            from os.path import join
            from os import getcwd
            filename = join(getcwd(),
                            "{0}.bsub".format(proc.workunit_name))
            raise JobSubmitFailed("Could not submit job."
                                  " Bsub script name: {0}"
                                  .format(filename))
        notifier_project_name = "dag_notifier"
        if hasattr(proc, "project_name"):
            notifier_project_name = proc.project_name
        retval = subprocess.call('bsub -P {1} -w "ended({0})" -J {0}_notifier'
                                 ' -app python-2.7.2 update_dag update {0}'
                                 .format(proc.workunit_name,
                                         notifier_project_name),
                                 shell=True)

        proc.state = dag.States.RUNNING
        the_dag.save()


def clean_workunit(root_dag, proc):
    """
    Removes temporary files from a process.

    @param root_dag: DAG
    @type root_dag: dag.DAG
    @param proc: Process to be cleaned
    @type proc: dag.Process
    """
    proc.clean_temp_files()


def get_state(proc):
    """
    Gets the state of the process using bjobs and returns the corresponding
     dag.States value. If bjobs does not find the job based on the job name,
     an exception is raised.

    @param proc: Process to be found
    @type proc: dag.Process
    @return: Process State
    @rtype: dag.States
    @raise BjobsFailed: If the job cannot be found by bjobs.
    """
    import subprocess as SP
    from dag import States
    bjobs = SP.Popen("bjobs -a -J {0}".format(proc.workunit_name)
                     .split(), stdout=SP.PIPE, stderr=SP.PIPE)
    retval = bjobs.wait()
    (stdout, stderr) = bjobs.communicate()
    if retval:
        raise BjobsFailed("Could not get status of job {0}\nRetval: {1}\n"
                          "Message: {2}"
                          .format(proc.workunit_name, retval, stderr))

    if not stdout:
        raise BjobsFailed("Could not get status of job {0}\nRetval: {1}\n"
                          "Message: {2}"
                          .format(proc.workunit_name, retval, stderr))

    if "RUN" in stdout:
        return States.RUNNING
    elif "PEND" in stdout:
        return States.STAGED
    elif "EXIT" in stdout:
        return States.FAIL
    elif "DONE" in stdout:
        return States.SUCCESS

    raise BjobsFailed("Could not get status of job {0}\nRetval: {1}\n"
                      "Message: {2}"
                      .format(proc.workunit_name, retval, stderr))
