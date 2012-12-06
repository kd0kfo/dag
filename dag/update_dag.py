#!/usr/bin/env python
"""
dag.update_dag
==============

@author: David Coss, PhD
@date: November 7, 2012
@license: GPL version 3 (see COPYING or http://www.gnu.org/licenses/gpl.html for details)

This python module provides interface between BOINC C API and Python user code.
"""
import dag,dag.boinc
import boinctools

def update_dag(cmd, cmd_args, dagfile = "jobs.dag"):
    """
    This is the main forking function that operates on a DAG and its workunits

    Arguments: 
    cmd -- String command to run on DAG
    cmd_args -- List of string arguments for the specified command

    Returns: No return value
    Raises Exception if the DAG file is missing or if the command is unknown.
    """
    from os import path as OP
    import dag,dag.boinc
    
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
                    for proc in root_dag.processes:
                        print("Removing %s" % proc.workunit_name)
                        dag.boinc.clean_workunit(root_dag,proc)
                        count += 1
                    root_dag.processes = []# clear process list
                else:
                    print("Removing %s" % wuname)
                    dag.boinc.clean_workunit(root_dag,proc)
                    root_dag.processes.remove(proc)# remove process


            if cmd in ["run","stage"]:
                print("Staging %s" % wuname)
                dag.boinc.stage_files(proc)
                if proc.state == dag.States.CREATED:
                    proc.state = dag.States.STAGED
                if cmd == "run":
                    print("Starting %s" % wuname)
                    if root_dag.incomplete_prereqs(proc):
                        raise Exception("Cannot start %s. Missing dependencies.")
                    dag.boinc.schedule_work(proc,dagfile)
                    proc.state = dag.States.RUNNING

            #save dag
            root_dag.save(dagfile)
            print("updated dagfile")
    elif cmd == "start":
        dag.boinc.create_work(root_dag,OP.abspath(dagfile))
        root_dag.save(dagfile)
    elif cmd == "recreate":
        if not cmd_args:
            raise Exception("recreate requires a specific file type to recreate.")
        if cmd_args[0] == "result_template":
            proc = root_dag.get_process(cmd_args[1])
            dag.boinc.create_result_template(proc,proc.result_template.full_path())
            print("Created result template")
        else:
            print("Do not know how to recreate: '%s'" % cmd_args[0])
    elif cmd == "cancel":
        proc_list = [root_dag.get_process(wuname) for wuname in cmd_args]
        dag.boinc.cancel_workunits(proc_list)
        root_dag.save()
    elif cmd == "state":
        state = dag.intstate(cmd_args[0])
        if state == None:
            print("%s is not a valid state." % cmd_args[0])
            print("States are %s" % ", ".join([dag.strstate(i) for i in range(0,dag.States.NUM_STATES)]))
            raise dag.DagException("Invalid State")
        proc_list = root_dag.get_processes_by_state(state)
        for i in proc_list:
            print(i)
    else:
        raise Exception("Unknown command: %s" % cmd)

