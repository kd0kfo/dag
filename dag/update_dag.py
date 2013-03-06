#!/usr/bin/env python
"""
dag.update_dag
==============

@author: David Coss, PhD
@date: November 7, 2012
@license: GPL version 3 (see COPYING or http://www.gnu.org/licenses/gpl.html for details)

This python module provides interface between BOINC C API and Python user code.
"""
import dag

command_help = {
    "cancel": "Stops a workunit.",
    "help": "Displays help for commands. Usage: help <cmd>",
    "list": "Lists all processes.",
    "print": "Print information about a process. If a workunit name is not given, all processes are listed.",
    "recreate": "Regenerates specified temporary files. Options are: 'result_template'",
    "remove": "Removes a workunit. 'all' can be supplied instead of a workunit name to remove ALL of the workunits.",
    "run": "Stars a specific process, by workunit name. This should be run after 'stage'",
    "stage": "Copies necessary files to their required locations on the server.",
    "start": "Starts ALL processes",
    "state": "Prints processes in a given state. The optional \"--count\" flag may be used to show only a count of the number of processes in that state. States are: {0}".format(", ".join([dag.strstate(i) for i in range(0,dag.States.NUM_STATES)]))
    
    }

def print_help(command = None):
    if not command in command_help:
        if command:
            print("Unknown command: %s" % command)
        print("Commands are %s" % ", ".join(command_help))
        return
    print("%s -- %s" % (command,command_help[command]))

def create_work(root_dag,dagpath,show_progress):
    from dag import Engine
    if root_dag.engine == Engine.BOINC:
        import dag.boinc
        dag.boinc.create_work(root_dag,dagpath,show_progress)
    elif root_dag.engine == Engine.LSF:
        import dag.lsf
        dag.lsf.create_work(root_dag,dagpath,show_progress)
    else:
        from dag import DagException
        raise DagException("Invalid engine id: %d" % root_dag.engine)    

def clean_workunit(root_dag,proc):
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
    from dag import Engine
    if root_dag.engine == Engine.BOINC:
        import dag.boinc
        dag.boinc.stage_files(proc)
    elif root_dag.engine != Engine.LSF:
        from dag import DagException
        raise DagException("Invalid engine id: %d" % root_dag.engine)

def schedule_work(root_dag,proc,dagfile):
    from dag import Engine
    if root_dag.engine == Engine.BOINC:
        import dag.boinc
        dag.boinc.schedule_work(proc,dagfile)
    elif root_dag.engine != Engine.LSF:
        from dag import DagException
        raise DagException("Invalid engine id: %d" % root_dag.engine)

def update_dag(cmd, cmd_args, dagfile = "jobs.dag", debug = False):
    """
    This is the main forking function that operates on a DAG and its workunits

    Arguments: 
    cmd -- String command to run on DAG
    cmd_args -- List of string arguments for the specified command

    Returns: No return value
    Raises Exception if the DAG file is missing or if the command is unknown.
    """
    from os import path as OP
    
    def needs_dagfile(cmd):
        return cmd not in ["help"]

    if debug:
        print("Running command: %s" % cmd)

    # if the dag is needed (probably), load it.
    root_dag = None
    if needs_dagfile(cmd):
        import dag
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
    elif cmd == "cancel":
        import dag.boinc
        if root_dag.engine != dag.Engine.BOINC:
            raise dag.DagException("Can only cancel BOINC jobs.")
        proc_list = [root_dag.get_process(wuname) for wuname in cmd_args]
        dag.boinc.cancel_workunits(proc_list)
        root_dag.save()
    elif cmd == "state":
        count_only = False
        if "--count" in  cmd_args:
            count_only = True

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

