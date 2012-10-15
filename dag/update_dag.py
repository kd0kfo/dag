#!/usr/bin/env python

import dag
import dag.util as dag_utils

def print_help():
    from sys import argv
    print("Usage: %s <cmd> [args]" % argv[0])

def update_dag(cmd, cmd_args):
    """
    This is the main forking function that operates on a DAG and its workunits

    Arguments: 
    cmd -- String command to run on DAG
    cmd_args -- List of string arguments for the specified command

    Returns: No return value
    Raises Exception if the DAG file is missing or if the command is unknown.
    """
    from os import path as OP
    import dag

    dagfile = "jobs.dag"

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
                print("Removing %s" % wuname)
                dag_utils.remove_workunit(root_dag,proc)
            if cmd in ["run","stage"]:
                print("Staging %s" % wuname)
                dag_utils.stage_files(proc)
                if proc.state == dag.States.CREATED:
                    proc.state = dag.States.STAGED
                if cmd == "run":
                    print("Starting %s" % wuname)
                    if root_dag.incomplete_prereqs(proc):
                        raise Exception("Cannot start %s. Missing dependencies.")
                    dag_utils.schedule_work(proc,dagfile)
                    proc.state = dag.States.RUNNING

            #save dag
            root_dag.save(dagfile)
    elif cmd == "start":
        dag_utils.create_work(root_dag,OP.abspath(dagfile))
        root_dag.save(dagfile)
    elif cmd == "recreate":
        if not cmd_args:
            raise Exception("recreate requires a specific file type to recreate.")
        if cmd_args[0] == "result_template":
            proc = root_dag.get_process(cmd_args[1])
            dag_utils.create_result_template(proc,proc.result_template.full_path())
            print("Created result template")
        else:
            print("Do not know how to recreate: '%s'" % cmd_args[0])
    elif cmd == "cancel":
        proc_list = [root_dag.get_process(wuname) for wuname in cmd_args]
        dag_utils.cancel_workunits(proc_list)
        root_dag.save()
    else:
        raise Exception("Unknown command: %s" % cmd)

if __name__ == "__main__":
    from sys import argv
    if len(argv) < 2:
        print_help()
        exit(1)

    update_dag(argv[1],argv[2:])
