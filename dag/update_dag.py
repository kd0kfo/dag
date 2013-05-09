#!/usr/bin/env python
"""
dag.update_dag
==============

@author: David Coss, PhD
@date: May 9, 2013
@license: GPL version 3 (see COPYING or http://www.gnu.org/licenses/gpl.html for details)

Utility function for DAG files.
"""
import dag

command_help = {
    "cancel": "Stops a workunit.",
    "help": "Displays help for commands. Usage: help <cmd>",
    "list": "Lists all processes.",
    "print": "Print information about a process. If a workunit name is not given, all processes are listed.",
    "recreate": "Regenerates specified temporary files. Options are: 'result_template'",
    "reset": "Clears generated values, such as workunit name, and moves process to CREATED state.",
    "remove": "Removes a workunit. 'all' can be supplied instead of a workunit name to remove ALL of the workunits.",
    "run": "Stars a specific process, by workunit name. This should be run after 'stage'",
    "stage": "Copies necessary files to their required locations on the server.",
    "start": "Starts ALL processes",
    "state": "Prints processes in a given state. The optional \"--count\" flag may be used to show only a count of the number of processes in that state. States are: {0}".format(", ".join([dag.strstate(i) for i in range(0,dag.States.NUM_STATES)])),
    "update": "Update the state of a workunit."
    
    }

def print_help(command = None):
    """
    Prints help for a command using the command_help dict. Help is printed to standard output.
    
    @param command: Command for which help is required
    @type command: str
    """
    if not command in command_help:
        if command:
            print("Unknown command: %s" % command)
        print("Commands are %s" % ", ".join(command_help))
        return
    print("%s -- %s" % (command,command_help[command]))

def create_work(root_dag,dagpath,show_progress):
    """
    Takes a DAG and starts processes that are able to be started.
    
    @param root_dag: DAG to be processed
    @type root_dag: dag.DAG
    @param dagpath: Path of DAG file
    @type dagpath: str
    @param show_progress: Whether or not a progress bar is desired (if available).
    @type show_progress: bool
    @raise dag.DagException: If the root dag contains an invalid engine.
    """
    from dag import Engine
    if root_dag.engine == Engine.BOINC:
        import dag.boinc
        dag.boinc.create_work(root_dag,dagpath,show_progress)
    elif root_dag.engine == Engine.LSF:
        import dag.lsf
        dag.lsf.create_work(root_dag,dagpath)
    else:
        from dag import DagException
        raise DagException("Invalid engine id: %d" % root_dag.engine)    

def clean_workunit(root_dag,proc):
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
        dag.boinc.clean_workunit(root_dag,proc)
    elif root_dag.engine == Engine.LSF:
        import dag.lsf
        dag.lsf.clean_workunit(root_dag,proc)
    else:
        from dag import DagException
        raise DagException("Invalid engine id: %d" % root_dag.engine)

def stage_files(root_dag,proc):
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

def schedule_work(root_dag,proc,dagfile):
    """
    Starts or Schedules a process
    
    @param root_dag: DAG
    @type root_dag: dag.DAG
    @param proc: Process to be scheduled/started
    @type proc: dag.Process
    @raise dag.DagException: If the root dag contains an invalid engine.
    """
    from dag import Engine,InternalProcess
    if isinstance(proc,InternalProcess):
        proc.start()
        return
    if root_dag.engine == Engine.BOINC:
        import dag.boinc
        dag.boinc.schedule_work(proc,dagfile)
    elif root_dag.engine != Engine.LSF:
        from dag import DagException
        raise DagException("Invalid engine id: %d" % root_dag.engine)

def update_state(cmd_args, root_dag, debug):
    """
    Updates the state of a process.
    
    @param cmd_args: Arguments used to do update. Values depend on engine, except for the first value, which is the name of the process.
    @type cmd_args: list
    @param root_dag: DAG
    @type root_dag: dag.DAG
    @raise dag.DagException: If the root dag contains an invalid engine or if specified proces is not in the DAG.
    """
    from dag import Engine
    if not cmd_args:
        from dag import DagException
        raise DagException("Missing workunit name for update.")

    proc = root_dag.get_process(cmd_args[0])
    if not proc:
        raise DagException("{0} not found in workunit list.".format(cmd_args))

    if root_dag.engine == Engine.BOINC:
        if len(cmd_args) < 2:
            raise DagException("For BOINC, a state name is needed to run update")
        new_state = cmd_args[1]
        proc.state = dag.intstate(new_state)
        root_dag.save()
    elif root_dag.engine == Engine.LSF:
        from lsf import get_state
        proc.state = get_state(proc)
        root_dag.save()
    else:
        raise DagException("Invalid engine id: %d" % root_dag.engine)
        

def update_dag(cmd, cmd_args, dagfile = dag.DEFAULT_DAGFILE_NAME, debug = False):
    """
    This is the main forking function that operates on a DAG and its workunits

    Arguments: 
    @param cmd: Command to run on DAG
    @type cmd: str
    @param cmd_args: List of string arguments for the specified command
    @type cmd_args: list
    @param dagfile: Optional filename for the DAG to be updated. Default: jobs.dag
    @type dagfile: str
    @param debug: Indicate whether or not debugging information is provided, including stack track if an exception is raised. Default: False
    @type debug: bool

    @raise Exception: if the DAG file is missing or if the command is unknown.
    """
    from os import path as OP
    
    def needs_dagfile(cmd):
        return cmd not in ["help"]

    if debug:
        print("Running command: %s" % cmd)

    # if the dag is needed (probably), load it.
    root_dag = None
    if needs_dagfile(cmd):
        if not OP.isfile(dagfile):
            raise Exception("Could not open '%s'" % dagfile)
        root_dag = dag.load(dagfile)

    if cmd == "print":
        if len(cmd_args) == 0:
            print(root_dag)
        else:
            for proc in root_dag.processes:
                if proc.workunit_name == cmd_args[0]:
                    print(proc)
            exit(0)
    elif cmd == "help":
        if not cmd_args:
            print_help(None)
        else:
            print_help(cmd_args[0])
    elif cmd == "list":
        for proc in root_dag.processes:
            print("%s: %s " % (proc.workunit_name,proc.cmd))
        exit(0)
    elif cmd in ["remove","run","stage"]:
        if len(cmd_args) == 0:
            raise Exception("%s requires at least one workunit name" % cmd)
        for wuname in cmd_args:
            proc = root_dag.get_process(wuname)
            if cmd == "remove":
                if wuname == "all":
                    from sys import stdin
                    print("Are you sure you want to remove ALL workunits (yes or no)?")
                    if not stdin.readline().strip() in ["y","Y","yes","Yes","YES"]:
                        print("Canceled.")
                        exit(1)
                    count = 0
                    progress_bar = None
                    if not debug:
                        from progressbar import ProgressBar, Percentage, Bar
                        progress_bar = ProgressBar(widgets = [Percentage(), Bar()], maxval=len(root_dag.processes)).start()
                    for proc in root_dag.processes:
                        if debug:
                            print("Removing %s" % proc.workunit_name)
                        clean_workunit(root_dag,proc)
                        count += 1
                        if progress_bar:
                            progress_bar.update(count)
                    if progress_bar:
                        print("")# reset line return
                    root_dag.processes = []# clear process list
                else:
                    if debug:
                        print("Removing %s" % wuname)
                    clean_workunit(root_dag,proc)
                    root_dag.processes.remove(proc)# remove process


            if cmd in ["run","stage"]:
                print("Staging %s" % wuname)
                stage_files(root_dag,proc)
                if proc.state == dag.States.CREATED:
                    proc.state = dag.States.STAGED
                if cmd == "run":
                    print("Starting %s" % wuname)
                    if root_dag.incomplete_prereqs(proc):
                        raise Exception("Cannot start %s. Missing dependencies.")
                    schedule_work(root_dag,proc,dagfile)
                    if isinstance(proc,dag.InternalProcess):
                        proc.state = dag.States.SUCCESS
                    else:
                        proc.state = dag.States.RUNNING

            #save dag
            root_dag.save(dagfile)
            print("updated dagfile")
    elif cmd == "start":
        create_work(root_dag,OP.abspath(dagfile),True)
        root_dag.save(dagfile)
    elif cmd == "recreate":
        if not cmd_args:
            raise Exception("recreate requires a specific file type to recreate.")
        if cmd_args[0] == "result_template":
            if root_dag.engine != dag.Engine.BOINC:
                raise dag.DagException("Can only make result template with BOINC jobs.")
            import dag.boinc
            proc = root_dag.get_process(cmd_args[1])
            dag.boinc.create_result_template(proc,proc.result_template.full_path())
            print("Created result template")
        else:
            print("Do not know how to recreate: '%s'" % cmd_args[0])
    elif cmd == "reset":
        for wuname in cmd_args:
            proc = root_dag.get_process(wuname)
            clean_workunit(root_dag,proc)
            proc.workunit_name = None
            proc.workunit_template = None
            proc.result_template = None
            proc.state = dag.States.CREATED
            root_dag.save()
    elif cmd == "cancel":
        if root_dag.engine != dag.Engine.BOINC:
            raise dag.DagException("Can only cancel BOINC jobs.")
        import dag.boinc
        proc_list = [root_dag.get_process(wuname) for wuname in cmd_args]
        dag.boinc.cancel_workunits(proc_list)
        root_dag.save()
    elif cmd == "update":
        update_state(cmd_args,root_dag,debug)
    elif cmd == "state":
        count_only = False
        if "--count" in  cmd_args:
            count_only = True
            
        if not cmd_args:
            raise dag.DagException("Missing state name.")
        states_to_view = cmd_args[0]
        if states_to_view == "all":
            states_to_view = ",".join([dag.strstate(i) for i in range(0,dag.States.NUM_STATES)])
            
        for state_name in states_to_view.split(","):
            state = dag.intstate(state_name)
            if state == None:
                print("%s is not a valid state." % state_name)
                print("States are %s" % ", ".join([dag.strstate(i) for i in range(0,dag.States.NUM_STATES)]))
                raise dag.DagException("Invalid State")
            proc_list = root_dag.get_processes_by_state(state)
            if count_only:
                print("%s: %d" % (dag.strstate(state),len(proc_list)))
            else:
                for i in proc_list:
                    print(i)
    else:
        if not debug:
            print("Unknown command: %s" % cmd)
        raise Exception("Unknown command: %s" % cmd)

