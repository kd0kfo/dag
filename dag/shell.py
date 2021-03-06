"""
dag.shell
=========


@author: David Coss, PhD
@date: April 21, 2014
@license: GPL version 3 (see COPYING or
 http://www.gnu.org/licenses/gpl.html for details)

Process intercommunication requires smq,
which may be found at https://github.com/kd0kfo/smq

Interface module to POSIX shells.
"""

from dag import Process, States, strstate
import logging

L = logging.getLogger("dag.shell")

# Default number of cores to use when running multiple shell processes.
DEFAULT_NUMBER_OF_CORES = 1

# Messages Queue Fields
QUEUE_NAME = "dag"
CLI_SENDER_PREFIX = "cli"
MASTER_SENDER_NAME = "master"
KILL_SIGNAL = "kill"

# Module variables
kill_switch = False
running_children = []


class ShellProcess(Process):
    """
    Abstraction of POSIX Shell Process.

    Contains fields of dag.Process plus "nice" which corresponds to the
    "niceness" of the processes. The value of nice is added to the niceness
    of the process when it starts.
    """
    def __init__(self, cmd, args):
        """
        @param cmd: Command to be executed
        @type cmd: str
        @param args: List of arguments to be provided to the command (cmd)
        @type args: list
        """
        super(ShellProcess, self).__init__()
        self.cmd = cmd
        self.args = args
        self.nice = 0

    def __str__(self):
        """
        Returns a string summary of the process.

        @return: Summary of process
        @rtype: str
        """
        from dag import enum2string
        strval = "Workunit Name: {0}\n".format(self.workunit_name)
        strval += "Command: {0} {1}\n".format(self.cmd, " ".join(self.args))
        if self.children:
            strval += "Children: {0}\n".format(" ".join([child.workunit_name
                                               for child in self.children]))
        strval += "Status: {0}".format(enum2string(States, self.state))

        return strval

    def start(self):
        """
        Starts the shell processes.

        The nice value of this object is added to the nice value
        of the forked process.

        Standard output and error are piped to files
        named <workunit name>.stdout and <workunit name>.stderr, respectively.

        The process runs until it exists or receives KILL_SIGNAL
        from the master process.
        """
        import subprocess
        import time

        message_queue = self.message_queue

        def set_niceness():
            if self.nice:
                from os import nice
                L.info("Changing niceness by %d" % self.nice)
                nice(self.nice)

        L.info("Starting {0}".format(self.cmd))
        stdout_file = open("%s.stdout" % self.workunit_name, "w")
        stderr_file = open("%s.stderr" % self.workunit_name, "w")

        shell_process = subprocess.Popen([self.cmd] + self.args,
                                         preexec_fn=set_niceness,
                                         stdout=stdout_file,
                                         stderr=stderr_file)
        while shell_process.poll() is None:
            if message_queue.has_message(self.workunit_name):
                message = message_queue.next(self.workunit_name)
                if message.content == KILL_SIGNAL:
                    L.debug("%s got kill signal" % self.workunit_name)
                    shell_process.terminate()
                    shell_process.wait()
                    for F in (stdout_file, stderr_file):
                        F.close()
                    break
            time.sleep(5)
        exit_status = shell_process.poll()
        L.info("{0} Finished with exit code {1}".format(self.cmd, exit_status))
        if exit_status:
            self.state = States.FAIL
        else:
            self.state = States.SUCCESS


class Waiter(ShellProcess):
    """
    Subclass of shell process that can be used to monitor a process
    that is not tracked in the DAG object. This object will be added
    to the DAG. It will run until the process it is monitoring has
    ended. Default polling period when monitoring is 60 seconds.
    """
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
        L.debug("Waiting on pid %d" % pid)
        while check_pid(pid):
            time.sleep(self.POLL_PERIOD)
        L.debug("No longer waiting on pid %d" % pid)


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


def runprocess(proc, message_queue):
    """
    Called by the master shell program, this function forks a shell process.
    The master process returns the PID of the child. The child process runs
    the shell process and then sends a message back to the master process
    indicating its status.
    """
    import os
    import smq
    pid = os.fork()
    if pid:  # Master
        return pid

    # Child
    L.debug("Forked %s as %d" % (proc.workunit_name, os.getpid()))
    # Updating master
    proc.state = States.RUNNING
    proc.message_queue = message_queue
    message_queue.send(smq.Message("state:%d" % proc.state, "str",
                               proc.workunit_name, MASTER_SENDER_NAME))
    proc.start()
    # If proc.start does not update its state, assume SUCCESS
    if proc.state == States.RUNNING:
        proc.state = States.SUCCESS
    message_queue.send(smq.Message("state:%d" % proc.state, "str",
                               proc.workunit_name, MASTER_SENDER_NAME))
    L.debug("%d finished start() with status %s" % (os.getpid(),
                                                    strstate(proc.state)))
    exit(0)


def perform_operation(root_dag, message):
    """
    Parses a messages from a child process and acts on the request,
    generally calling update.modify_dag. Returns a string reply to
    that may be sent to the client.

    @param root_dag: Main DAG object
    @type root_dag: dag.DAG
    @param message: Message sent from client to master
    @type message: str
    @return: Message to be sent to the client
    @rtype: str
    """
    from dag.update_dag import modify_dag
    if not message:
        return
    tokens = message.content.split(" ")
    cmd = tokens[0]
    cmd_args = tokens[1:]

    # Forbidden processes
    if cmd in ["start"]:
        return "Shell command line client cannot run %s" % cmd

    L.debug("Shell Monitor is running %s" % cmd)
    try:
        retval = modify_dag(root_dag, cmd, cmd_args, True)
        root_dag.save()
        return retval
    except Exception as e:
        import traceback
        return "Error running %s: %s\n%s" % (cmd, e, traceback.format_exc())


def dump_state(root_dag, message_queue):
    """
    Returns information on the state of the running processes.

    @return: State information for the shell processes
    @rtype: str
    """
    from dag.update_dag import modify_dag
    retval = "Currently running %d processes\n" % len(running_children)
    retval += "Jobs by state:\n%s\n" % modify_dag(root_dag, "state",
                                                  ["all", "--count"], False)
    return retval


def process_messages(root_dag, message_queue):
    """
    Reads through messages in the queue for the master and acts on them.

    @see: perform_operation
    @param root_dag: Main DAG object
    @type root_dag: dag.DAG
    @param message_queue: Message queue being read
    @type message_queue: smq.Queue
    """
    global kill_switch
    from smq import Message
    from dag import FINISHED_STATES

    def send(text, recipient):
        message_queue.send(Message(text, "str", MASTER_SENDER_NAME, recipient))

    retval = None
    while message_queue.has_message(MASTER_SENDER_NAME):
        message = message_queue.next(MASTER_SENDER_NAME)
        L.debug("Processing Message from %s: %s..." %
                (message.sender, message.content[0:15]))
        if message.content == "shutdown":
            kill_switch = True
            retval = "Shutting down shell processes"
        elif message.content.startswith("state:"):
            proc = root_dag.get_process(message.sender)
            if not proc:
                retval = ("Cannot change state. Unknown process %s"
                          % message.sender)
                break
            newstate = message.content.replace("state:", "")
            proc.state = int(newstate)
            L.debug("Changed state of %s to %s"
                      % (proc.workunit_name, strstate(proc.state)))
            root_dag.save()
            if proc.state in FINISHED_STATES:
                for ended in [i for i in running_children
                              if i[0] == proc.workunit_name]:
                    running_children.remove(ended)
        elif message.content == "dump":
            retval = dump_state(root_dag, message_queue)
        else:
            retval = perform_operation(root_dag, message)
        if retval is not None:
            send(retval, message.sender)


def send_kill_signal(process_name, pid, message_queue):
    """
    Sends a kill signal to child processes.

    @param process_name: Name of process to be killed.
    @type process_name: str
    @param pid: PID to be killed
    @type: int
    @param message_queue: Queue to be used to send signal
    @type message_queue: smq.Queue
    """
    import smq
    L.debug("Sending kill signal to %s" % pid)
    message_queue.send(smq.Message(KILL_SIGNAL, "str",
                                   MASTER_SENDER_NAME,
                                   process_name))


def create_work(root_dag, dag_path):
    """
    Starts and monitors shell processes. This is the
    main process loop function.

    @param root_dag: Main DAG object
    @type root_dag: dag.DAG
    @param dag_path: Path to DAG file
    @type dag_path: str
    """
    import smq
    from os import getpid
    from dag import WAITING_STATES

    global kill_switch
    global running_children

    if root_dag.num_cores:
        num_cores = root_dag.num_cores
    else:
        num_cores = DEFAULT_NUMBER_OF_CORES

    # called before fork. All are therefore aware who master is.
    master_pid = getpid()

    # Setup Message queue used for update_dag to communicate with this
    # python thread, rather than directly modify root_dag
    message_queue = smq.Queue(QUEUE_NAME, root_dag.queue_filename, timeout=7)
    root_dag.message_queue = message_queue
    process_messages(root_dag, message_queue)

    torun = root_dag.generate_runnable_list()
    if not torun:
        return

    # doing loop so that in the future finished processes
    # may start other processes
    import time
    num_processes_left = len(root_dag.get_processes_by_state(WAITING_STATES))
    L.info("Doing work locally with %d cores. Master PID %d" % (num_cores,
                                                                getpid()))
    try:
        while torun or num_processes_left or running_children:
            for process in torun:
                if len(running_children) >= num_cores:
                    break
                pid = runprocess(process, message_queue)
                running_children.append((process.workunit_name, pid))
            num_processes_left = len(root_dag
                                     .get_processes_by_state(WAITING_STATES))
            process_messages(root_dag, message_queue)
            if kill_switch:
                break
            torun = root_dag.generate_runnable_list()
            time.sleep(5)
    finally:
        mypid = getpid()
        if mypid == master_pid and running_children:
            if not torun:
                L.debug("Finished loop because there are "
                        "no runnable processes left.")
            if not num_processes_left:
                L.debug("Finished loop because there are "
                        "no processes left in the waiting state.")
            if not running_children:
                L.debug("Finished loop because there are "
                        "no running processes at the moment.")
            if kill_switch:
                L.debug("Finished loop because kill switch was thrown.")
            from os import waitpid
            L.debug("%d is killing threads %d threads " %
                    (mypid, len(running_children)))
            for running_child in running_children:
                send_kill_signal(running_child[0],
                                 running_child[1],
                                 message_queue)
                waitpid(running_child[1], 0)


def cancel_workunits(root_dag, processes):
    """
    Takes a list of processes and sends a kill signal.

    @param root_dag: Main DAG object
    @type root_dag: dag.DAG
    @param processes: List of processes to be cancelled
    @type processes: list of dag.Process
    """
    L.debug("Canceling work units %s" % [p.workunit_name for p in processes])
    for proc in processes:
        childlist = [P for P in running_children if proc.workunit_name == P[0]]
        if childlist:
            pid = childlist[0][1]
            send_kill_signal(proc.workunit_name,
                             pid,
                             root_dag.message_queue)


def clean_workunit(root_dag, proc):
    """
    Removes output files for process

    @param root_dag: Main DAG object
    @type root_dag: dag.DAG
    @param proc: Process to be cleaned
    @type proc: dag.Proc
    """
    for outputfile in proc.output_files:
        outputfile.unlink()
