#!/usr/bin/env python
"""
dag.update_dag
==============

@author: David Coss, PhD
@date: May 9, 2013
@license: GPL version 3 (see COPYING
 or http://www.gnu.org/licenses/gpl.html for details)

Utility function for DAG files.
"""
import dag

command_help = {
    "attach": ("Attaches a SHELL process to a process id (PID)."
               " Usage: attach <workunit name> <PID>"),
    "cancel": "Stops a workunit.",
    "help": "Displays help for commands. Usage: help <cmd>",
    "list": "Lists all processes.",
    "print": ("Print information about a process. If a workunit"
              " name is not given, all processes are listed."),
    "recreate": ("Regenerates specified temporary files."
                 " Options are: 'result_template'"),
    "reset": ("Clears generated values, such as workunit name,"
              " and moves process to CREATED state."),
    "remove": ("Removes a workunit. 'all' can be supplied instead"
               " of a workunit name to remove ALL of the workunits."),
    "run": ("Stars a specific process, by workunit name. This should be run"
            " after 'stage'"),
    "stage": ("Copies necessary files to their required locations"
              " on the server."),
    "start": "Starts ALL processes",
    "state": ("Prints processes in a given state. The optional \"--count\""
              " flag may be used to show only a count of the number "
              "of processes in that state. States are: {0}"
              .format(", ".join([dag.strstate(i)
                                 for i in range(0, dag.States.NUM_STATES)]))),
    "update": "Update the state of a workunit.",
    "uuid": "Gets UUID for a work unit."
    }


def get_help_string(command=None):
    """
    Prints help for a command using the command_help dict. Help is printed
     to standard output.

    @param command: Command for which help is required
    @type command: str
    """
    if not command in command_help:
        if command:
            return "Unknown command: %s" % command
        return "Commands are %s" % ", ".join(command_help)
    return "%s -- %s" % (command, command_help[command])


def create_work(root_dag, dagpath, show_progress, num_cores=None):
    """
    Takes a DAG and starts processes that are able to be started.

    @param root_dag: DAG to be processed
    @type root_dag: dag.DAG
    @param dagpath: Path of DAG file
    @type dagpath: str
    @param show_progress: Whether or not a progress bar is desired
     (if available).
    @type show_progress: bool
    @param num_cores: Optional number of cores used in local multiprocessing
    @type num_cores: int
    @raise dag.DagException: If the root dag contains an invalid engine.
    """
    from dag import Engine
    if root_dag.engine == Engine.BOINC:
        import dag.boinc
        dag.boinc.create_work(root_dag, dagpath, show_progress)
    elif root_dag.engine == Engine.LSF:
        import dag.lsf
        dag.lsf.create_work(root_dag, dagpath)
    elif root_dag.engine == Engine.SHELL:
        import dag.shell
        dag.shell.create_work(root_dag, dagpath)
    else:
        from dag import DagException
        raise DagException("Invalid engine id: %d"
                           % root_dag.engine)


def start_processes(root_dag, dagpath, show_progress, num_cores=None):
    """
    Starts process that can start.

    Calls create_work. Then saves DAG.
    """
    import os.path as OP

    create_work(root_dag, OP.abspath(dagpath), show_progress, num_cores)
    root_dag.save()


def clean_workunit(root_dag, proc):
    """
    Cleans up after a process. Removes temporary files.

    @param root_dag: DAG
    @type root_dag: dag.DAG
    @param proc: Process to be cleaned
    @type proc: dag.Process
    @raise dag.DagException: If the root dag contains an invalid engine.
    """
    from dag import Engine
    if root_dag.engine == Engine.BOINC:
        import dag.boinc
        dag.boinc.clean_workunit(root_dag, proc)
    elif root_dag.engine == Engine.LSF:
        import dag.lsf
        dag.lsf.clean_workunit(root_dag, proc)
    elif root_dag.engine == Engine.SHELL:
        import dag.shell
        dag.shell.clean_workunit(root_dag, proc)
    else:
        from dag import DagException
        raise DagException("Invalid engine id: %d"
                           % root_dag.engine)


def stage_files(root_dag, proc):
    """
    Creates temporary/intermediate files for a process.

    @param root_dag: DAG
    @type root_dag: dag.DAG
    @param proc: Process to be staged
    @type proc: dag.Process
    @raise dag.DagException: If the root dag contains an invalid engine.
    """
    from dag import Engine
    if root_dag.engine == Engine.BOINC:
        import dag.boinc
        dag.boinc.stage_files(proc)
    elif root_dag.engine == Engine.LSF:
        import dag.lsf
        dag.lsf.stage_files(proc)
    else:
        from dag import DagException
        raise DagException("Invalid engine id: %d" % root_dag.engine)


def schedule_work(root_dag, proc, dagfile):
    """
    Starts or Schedules a process

    @param root_dag: DAG
    @type root_dag: dag.DAG
    @param proc: Process to be scheduled/started
    @type proc: dag.Process
    @raise dag.DagException: If the root dag contains an invalid engine.
    """
    from dag import Engine, InternalProcess
    from dag.shell import ShellProcess

    if isinstance(proc, InternalProcess) or isinstance(proc, ShellProcess):
        proc.start()
        return
    if root_dag.engine == Engine.BOINC:
        import dag.boinc
        dag.boinc.schedule_work(proc, dagfile)
    elif root_dag.engine != Engine.LSF:
        from dag import DagException
        raise DagException("Invalid engine id: %d" % root_dag.engine)


def update_state(cmd_args, root_dag):
    """
    Updates the state of a process.

    @param cmd_args: Arguments used to do update. Values depend on engine,
     except for the first value, which is the name of the process.
    @type cmd_args: list
    @param root_dag: DAG
    @type root_dag: dag.DAG
    @raise dag.DagException: If the root dag contains an invalid engine
     or if specified proces is not in the DAG.
    """
    from dag import Engine, DagException
    if not cmd_args:
        raise DagException("Missing workunit name for update.")

    proc = root_dag.get_process(cmd_args[0])
    if not proc:
        raise DagException("{0} not found in workunit list.".format(cmd_args))

    if root_dag.engine == Engine.BOINC:
        if len(cmd_args) < 2:
            raise DagException("For BOINC, a state name is needed"
                               " to run update")
        new_state = cmd_args[1].upper()
        proc.state = dag.intstate(new_state)
        root_dag.save()
    elif root_dag.engine == Engine.LSF:
        from lsf import get_state
        if len(cmd_args) == 2:
            proc.state = dag.intstate(cmd_args[1].upper())
        else:
            proc.state = get_state(proc)
        root_dag.save()
    elif root_dag.engine == Engine.SHELL:
        proc.state = dag.intstate(cmd_args[1].upper())
        root_dag.save()
    else:
        raise DagException("Invalid engine id: %d" % root_dag.engine)


def modify_dag(root_dag, cmd, cmd_args, debug=False):
    """
    This is the main operating function. This takes a command and performs
    an action on the dag
    
    @param root_dag: DAG to be modified
    @type root_dag: dag.DAG
    @param cmd: Command to execute
    @type cmd: str
    @param cmd_args: Optional arguments for commands.
    @type cmd_args: list
    @param debug: Optional debug flag
    @type debug: bool
    """
    import os.path as OP
    import dag

    return_message = ""
    if cmd == "attach":
        from dag.shell import Waiter
        if len(cmd_args) != 2:
            raise Exception("Attach requires a workunit name"
                            " and a process id number (PID).")
        new_process = Waiter(cmd_args[0], [cmd_args[1], ])
        for process in root_dag.processes:
            if (process.state in [dag.States.CREATED, dag.States.STAGED]
                and not isinstance(process, Waiter)):
                new_process.children.append(process)
        root_dag.processes.append(new_process)
        root_dag.save()
        return "Attached %s" % cmd_args[0]
    elif cmd == "print":
        if len(cmd_args) == 0:
            return_message += "%s\n" % root_dag
        else:
            proc = root_dag.get_process(cmd_args[0])
            if proc:
                return_message += "%s\n" % proc
            else:
                return_message += "No such process found: {0}\n".format(cmd_args[0])
            return return_message
    elif cmd == "help":
        if not cmd_args:
            return get_help_string(None)
        else:
            return get_help_string(cmd_args[0])
    elif cmd == "list":
        for proc in root_dag.processes:
            return_message += "%s: %s\n" % (proc.workunit_name, proc.cmd)
        return return_message
    elif cmd in ["remove", "run", "stage"]:
        if len(cmd_args) == 0:
            raise Exception("%s requires at least one workunit name" % cmd)
        for wuname in cmd_args:
            proc = root_dag.get_process(wuname)
            if cmd == "remove":
                if wuname == "all":
                    from sys import stdin
                    print("Are you sure you want to remove ALL workunits"
                          " (yes or no)?")
                    if (not stdin.readline().strip()
                       in ["y", "Y", "yes", "Yes", "YES"]):
                        # Cancel workunit
                        print("Canceled.")
                        exit(1)
                    count = 0
                    progress_bar = None
                    if not debug:
                        from progressbar import ProgressBar, Percentage, Bar
                        num_processes = len(root_dag.processes)
                        if num_processes:
                            progress_bar = ProgressBar(widgets = [Percentage(), Bar()], maxval = num_processes).start()
                    for proc in root_dag.processes:
                        if debug:
                            print("Removing %s" % proc.workunit_name)
                        clean_workunit(root_dag, proc)
                        count += 1
                        if progress_bar:
                            progress_bar.update(count)
                    if progress_bar:
                        print("")  # reset line return
                    root_dag.processes = []  # clear process list
                else:
                    if debug:
                        print("Removing %s" % wuname)
                    clean_workunit(root_dag, proc)
                    root_dag.processes.remove(proc)  # remove process
                    return_message += "Removed %s\n" % wuname
            if cmd in ["run", "stage"]:
                print("Staging %s" % wuname)
                stage_files(root_dag, proc)
                if proc.state == dag.States.CREATED:
                    proc.state = dag.States.STAGED
                if cmd == "run":
                    return_message += "Starting %s\n" % wuname
                    if root_dag.incomplete_prereqs(proc):
                        raise Exception("Cannot start %s."
                                        " Missing dependencies.")
                    schedule_work(root_dag, proc, root_dag.filename)
                    if isinstance(proc, dag.InternalProcess):
                        proc.state = dag.States.SUCCESS
                        return_message += "Finished %s" % wuname
                    else:
                        proc.state = dag.States.RUNNING
            #save dag
            root_dag.save()
            print("updated dagfile")
    elif cmd == "start":
        start_processes(root_dag, OP.abspath(root_dag.filename),
                        True, root_dag.num_cores)
        return_message += "Started processes"
    elif cmd == "recreate":
        if not cmd_args:
            raise Exception("recreate requires a specific file type"
                            " to recreate.")
        if cmd_args[0] == "result_template":
            if root_dag.engine != dag.Engine.BOINC:
                raise dag.DagException("Can only make result template"
                                       " with BOINC jobs.")
            import dag.boinc
            proc = root_dag.get_process(cmd_args[1])
            dag.boinc.create_result_template(proc,
                                             proc.result_template.full_path())
            print("Created result template")
        else:
            print("Do not know how to recreate: '%s'" % cmd_args[0])
        return_message += "Recreated %s\n" % cmd_args[0]
    elif cmd == "reset":
        for wuname in cmd_args:
            proc = root_dag.get_process(wuname)
            if not proc:
                return_message += "No such workunit: %s\n" % wuname
                continue
            clean_workunit(root_dag, proc)
            proc.workunit_name = None
            proc.workunit_template = None
            proc.result_template = None
            proc.state = dag.States.CREATED
            root_dag.save()
            return_message += "Reset %s" % wuname
    elif cmd == "cancel":
        if root_dag.engine == dag.Engine.LSF:
            raise dag.DagException("Cannot yet cancel LSF jobs.")
        elif root_dag.engine == dag.Engine.SHELL:
            if not hasattr(root_dag, "message_queue"):
                raise dag.DagException("Cannot stop shell process "
                                       "without message queue")
        
        proc_list = [root_dag.get_process(wuname) for wuname in cmd_args]
        if root_dag.engine == dag.Engine.BOINC:
            dag.boinc.cancel_workunits(proc_list)
        elif root_dag.engine == dag.Engine.SHELL:
            dag.shell.cancel_workunits(root_dag, proc_list)
        root_dag.save()
        return_message += "Cancelled %s" % ", ".join(cmd_args)
    elif cmd == "update":
        update_state(cmd_args, root_dag)
        if root_dag.engine == dag.Engine.LSF:
            start_processes(root_dag, root_dag.filename, False)
        return_message += "Updated process"
    elif cmd == "state":
        count_only = False
        if "--count" in cmd_args:
            count_only = True

        if not cmd_args:
            raise dag.DagException("Missing state name.")
        states_to_view = cmd_args[0]
        if states_to_view == "all":
            states_to_view = ",".join([dag.strstate(i)
                                       for i in range(0,
                                                      dag.States.NUM_STATES)])

        for state_name in states_to_view.split(","):
            state = dag.intstate(state_name.upper())
            if state is None:
                print("%s is not a valid state." % state_name)
                print("States are %s"
                      % ", ".join([dag.strstate(i)
                                   for i in range(0, dag.States.NUM_STATES)]))
                raise dag.DagException("Invalid State")
            proc_list = root_dag.get_processes_by_state(state)
            if count_only:
                return_message += "%s: %d\n" % (dag.strstate(state), len(proc_list))
            else:
                for i in proc_list:
                    return_message += "%s" % i
    elif cmd == "uuid":
        proc = root_dag.get_process(cmd_args[0])
        return_message += str(proc.uuid)
    else:
        if not debug:
            return_message += "Unknown command: %s" % cmd
        raise Exception("Unknown command: %s" % cmd)
    return return_message

def update_dag(cmd, cmd_args, dagfile=dag.DEFAULT_DAGFILE_NAME, debug=False,
               num_cores=None, init_file=None):
    """
    This is the main forking function that operates on a DAG and its workunits

    Arguments:
    @param cmd: Command to run on DAG
    @type cmd: str
    @param cmd_args: List of string arguments for the specified command
    @type cmd_args: list
    @param dagfile: Optional filename for the DAG to be updated.
     Default: jobs.dag
    @type dagfile: str
    @param debug: Indicate whether or not debugging information is provided,
     including stack track if an exception is raised. Default: False
    @type debug: bool
    @param num_cores: Optional number of cores used in local multiprocessing
    @type num_cores: int

    @raise Exception: if the DAG file is missing or if the command is unknown.
    """
    from os import path as OP
    import dag

    def needs_dagfile(cmd):
        return cmd not in ["help"]

    # If we still don't have the init file, there is a problem.
    if not init_file:
        if init_file is None:
            raise dag.DagException("No init file provided.")
        else:
            raise dag.DagException("Could not open init file ({0}). File not found."
                               .format(init_file.name))

    parsers = {}
    init_code = compile(init_file.read(), init_file.name, "exec")
    exec(init_code)

    if debug:
        print("Running command: %s" % cmd)

    # If the dag is needed (probably), load it.
    root_dag = None
    if needs_dagfile(cmd):
        if not OP.isfile(dagfile):
            raise Exception("Could not open '%s'" % dagfile)
        root_dag = dag.load(dagfile)
        root_dag.filename = dagfile
    if num_cores:
        root_dag.num_cores = num_cores

    print(modify_dag(root_dag, cmd, cmd_args, debug))