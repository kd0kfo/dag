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

from dag import Process, States, strstate

DEFAULT_NUMBER_OF_CORES = 1


class ShellProcess(Process):
    def __init__(self, cmd, args):
        super(ShellProcess, self).__init__()
        self.cmd = cmd
        self.args = args
        self.nice = 0

    def __str__(self):
        from dag import enum2string, States
        strval = "Command: {0} {1}\n".format(self.cmd, " ".join(self.args))
        strval += "Status: {0}".format(enum2string(States, self.state))

        return strval

    def start(self):
        import subprocess
        
        def set_niceness():
            if self.nice:
                from os import nice
                print("Changing niceness by %d" % self.nice)
                nice(self.nice)

        print("Starting {0}".format(self.cmd))
        retval = subprocess.call([self.cmd] + self.args, preexec_fn=set_niceness)
        print("{0} Finished".format(self.cmd))


def parse_shell(cmd, args, header_map):
    newproc = ShellProcess(cmd, args)
    if "nice" in header_map:
        newproc.nice = int(header_map["nice"])
    return [newproc]


def runprocess(proc):
    print("RUNPROC")
    proc.start()
    proc.state = States.SUCCESS
    return proc


def callback(proc):
    print("Finished:%s State: %s" % (proc.workunit_name, strstate(proc.state)))
 

def create_work(root_dag, dag_path):
    from multiprocessing import Pool

    if root_dag.num_cores:
        num_cores = root_dag.num_cores
    else:
        num_cores = DEFAULT_NUMBER_OF_CORES
    pool = Pool(num_cores)

    torun = root_dag.get_processes_by_state((States.CREATED, States.STAGED))
    if not torun:
        pool.close()
        return

    print("Starting %d processes using %d cores" % (len(torun), num_cores))
    # doing loop so that in the future finished processes
    # may start other processes
    import time
    from os import nice
    while torun: 
        for process in torun:
            process.state = States.RUNNING
            pool.apply_async(runprocess, (process,), callback=callback)
        time.sleep(5)
        torun = root_dag.get_processes_by_state((States.CREATED, States.STAGED))

    pool.close()
    pool.join()


def clean_workunit(root_dag, proc):
    from os import unlink
    for outputfile in proc.output_files:
        outputfile.unlink()