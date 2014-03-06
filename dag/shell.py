"""
dag.shell
=======


@author: David Coss, PhD
@date: May 9, 2013
@license: GPL version 3 (see COPYING or
 http://www.gnu.org/licenses/gpl.html for details)

Interface module to POSIX shells. This allows processes in a DAG to interpret
 and use LSF data.
"""

from dag import Process

class ShellProcess(Process):
    def __init__(self, cmd, args):
        super(ShellProcess, self).__init__()
        self.cmd = cmd
        self.args = args

    def __str__(self):
        from dag import enum2string, States
        strval = "Command: {0} {1}\n".format(self.cmd, " ".join(self.args))
        strval += "Status: {0}".format(enum2string(States, self.state))

        return strval

    def start(self):
        import subprocess

        print("Starting {0}".format(self.cmd))
        retval = subprocess.call([self.cmd] + self.args)
        print("{0} Finished".format(self.cmd))
        return retval


def parse_shell(cmd, args, header_map):
    return [ShellProcess(cmd, args)]


def runprocess(proc):
    proc.start()
    proc.state = States.SUCCESS
    return procs

def blah(foo):
    print(foo)
    return foo

def create_work(root_dag, dag_path):
    from dag import States
    from multiprocessing import Pool

    print("SHELL: Starting {0} processes".format(len(root_dag.processes)))
    pool = Pool(root_dag.num_cores)
    
    def callback(procs):
        for proc in procs:
            print("CALLBACK. Finished %s" % proc.workunit_name)

    torun = root_dag.get_processes_by_state((States.CREATED, States.STAGED))
    for process in torun:
        pool.apply_async(runprocess, (process,), callback=callback)

    pool.close()
    pool.join()