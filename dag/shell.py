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
        strval = "Workunit Name: {0}\n".format(self.workunit_name)
        strval += "Command: {0} {1}\n".format(self.cmd, " ".join(self.args))
        if self.children:
            strval += "Children: {0}\n".format(" ".join([child.workunit_name
                                               for child in self.children]))
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
        stdout_file = open("%s.stdout" % self.workunit_name, "w")
        stderr_file = open("%s.stderr" % self.workunit_name, "w")
        retval = subprocess.call([self.cmd] + self.args, preexec_fn=set_niceness, stdout=stdout_file, stderr=stderr_file)
        print("{0} Finished".format(self.cmd))


class Waiter(ShellProcess):
    def __init__(self, cmd, args):
        super(Waiter, self).__init__(cmd, args)
        self.workunit_name = "waiting-%s" % args[0]
        self.POLL_PERIOD = 60  # Seconds

    def start(self):
        import time
        def check_pid(thepid):
            from os import kill
            import errno
            try:
                kill(thepid, 0)
            except OSError as ose:
                if errno.ESRCH == ose.errno:
                    return False
            return True

        pid = int(self.args[0])
        print("Waiting on pid %d" % pid)
        while check_pid(pid):
            time.sleep(self.POLL_PERIOD)
        print("No longer waiting on pid %d" % pid)


def parse_shell(cmd, args, header_map, parsers, init_code=None):
    if not cmd in parsers:
        proc_list = [ShellProcess(cmd, args)]
    else:
        if init_code:
            exec(init_code)
        funct = "%s(args,header_map)" % parsers[cmd]
        proc_list = eval(funct)   # uses parser_args
    if "nice" in header_map:
        for newproc in proc_list:
            newproc.nice = int(header_map["nice"])
    return proc_list


def runprocess(proc, queue):
    print("Starting %s" % proc.workunit_name)
    try:
        proc.state = States.RUNNING
        queue.put((proc.workunit_name, proc.state))
        proc.start()
        proc.state = States.SUCCESS
    except Exception as e:
        print("ERROR calling start")
        print("TYPE: %s" % type(proc))
        print(e)
        proc.state = States.FAIL
    queue.put((proc.workunit_name, proc.state))
    return proc


def callback(proc):
    print("Finished:%s State: %s" % (proc.workunit_name, strstate(proc.state)))


def create_work(root_dag, dag_path):
    from multiprocessing import Manager, Pool, Queue

    if root_dag.num_cores:
        num_cores = root_dag.num_cores
    else:
        num_cores = DEFAULT_NUMBER_OF_CORES
    pool = Pool(num_cores)
    manager = Manager()
    thread_queue = manager.Queue()

    torun = root_dag.generate_runnable_list()    
    if not torun:
        pool.close()
        return

    # doing loop so that in the future finished processes
    # may start other processes
    import time
    from os import nice
    waiting_states = (States.CREATED, States.STAGED)
    num_processes_left = len(root_dag.get_processes_by_state(waiting_states))
    print("Doing work locally with %d cores" % num_cores)
    try:
        while torun or num_processes_left:
            should_save_dag = not thread_queue.empty()
            for process in torun:
                should_save_dag = True
                pool.apply_async(runprocess, (process, thread_queue,), callback=callback)
            time.sleep(5)
            while not thread_queue.empty():
                (procname, state) = thread_queue.get()
                finished_process = root_dag.get_process(procname)
                if not finished_process:
                    continue
                finished_process.state = state
                should_save_dag = True
            torun = root_dag.generate_runnable_list()
            num_processes_left = len(root_dag.get_processes_by_state(waiting_states))
            if should_save_dag:
                root_dag.save()
    except KeyboardInterrupt as ki:
        pool.terminate()
    pool.close()
    pool.join()


def clean_workunit(root_dag, proc):
    from os import unlink
    for outputfile in proc.output_files:
        outputfile.unlink()