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
import logging

L = logging.getLogger("dag.shell")

DEFAULT_NUMBER_OF_CORES = 1

# Messages Queue Fields
QUEUE_NAME = "dag"
CLI_SENDER_PREFIX = "cli"
MASTER_SENDER_NAME = "master"
KILL_SIGNAL = "kill"

kill_switch = False
running_children = []


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
        # retval = subprocess.call([self.cmd] + self.args, preexec_fn=set_niceness, stdout=stdout_file, stderr=stderr_file)
        shell_process = subprocess.Popen([self.cmd] + self.args, preexec_fn=set_niceness, stdout=stdout_file, stderr=stderr_file)
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
    from os import getpid
    pid = getpid()
    print("Starting %s with pid %d" % (proc.workunit_name, pid))
    try:
        proc.state = States.RUNNING
        proc.queue_filename = message_queue
        queue.put((proc.workunit_name, proc.state, pid))
        proc.start()
        proc.state = States.SUCCESS
    except Exception as e:
        print("ERROR calling start")
        print("TYPE: %s" % type(proc))
        print(e)
        proc.state = States.FAIL
    queue.put((proc.workunit_name, proc.state, pid))
    return proc
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
    L.debug("%d finished start() with status %s" % (os.getpid(), strstate(proc.state)))
    exit(0)


def perform_operation(root_dag, message):
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
    from dag.update_dag import modify_dag
    retval = "Currently running %d processes\n" % len(running_children)
    retval += "Jobs by state:\n%s\n" % modify_dag(root_dag, "state",
                                                  ["all", "--count"], False)
    return retval

def process_messages(root_dag, message_queue):
    global kill_switch
    from smq import Message
    from dag import FINISHED_STATES

    def send(text, recipient):
        message_queue.send(Message(text, "str", MASTER_SENDER_NAME, recipient))

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
            retval = ("Changed state of %s to %s"
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
        send(retval, message.sender)


def create_work(root_dag, dag_path):
    import smq
    from os import getpid
    from dag import WAITING_STATES

    global kill_switch
    global running_children
    
    if root_dag.num_cores:
        num_cores = root_dag.num_cores
    else:
        num_cores = DEFAULT_NUMBER_OF_CORES
    master_pid = getpid()  # called before fork. All are therefore aware who master is.

    # Setup Message queue used for update_dag to communicate with this
    # python thread, rather than directly modify root_dag
    message_queue = smq.Queue(QUEUE_NAME, root_dag.queue_filename, timeout=7)
    process_messages(root_dag, message_queue)

    torun = root_dag.generate_runnable_list()
    if not torun:
        return

    # doing loop so that in the future finished processes
    # may start other processes
    import time
    num_processes_left = len(root_dag.get_processes_by_state(WAITING_STATES))
    L.info("Doing work locally with %d cores. Master PID %d" % (num_cores, getpid()))
    try:
        while torun or num_processes_left:
            for process in torun:
                if len(running_children) >= num_cores:
                    break
                pid = runprocess(process, message_queue)
                running_children.append((process.workunit_name, pid))
            num_processes_left = len(root_dag.get_processes_by_state(WAITING_STATES))
            process_messages(root_dag, message_queue)
            if kill_switch:
                break
            torun = root_dag.generate_runnable_list()
            time.sleep(5)
    finally:
        mypid = getpid()
        if mypid == master_pid and running_children:
            from os import kill
            L.debug("%d is killing threads %d threads " % (mypid, len(running_children)))
            for running_child in running_children:
                L.debug("Sending kill signal to %s" % running_child[1])
                message_queue.send(smq.Message(KILL_SIGNAL, "str",
                                               MASTER_SENDER_NAME,
                                               running_child[0]))



def clean_workunit(root_dag, proc):
    from os import unlink
    for outputfile in proc.output_files:
        outputfile.unlink()