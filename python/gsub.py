#!/usr/bin/env python

import dag,dag_utils

def create_dag(input_filename, parsers):
    """
    Takes an input file that contains a list of commands and generates a dag.
    Jobs that have all of their prerequisites met are started, unless the
    --setup_only flag is provided.

    Arguments:
    input_filename -- String filename to be parsed into a DAG
    parsers -- Dictionary that maps string command names to functions that
    \tare used to create DAG.
    
    Returns: dag.DAG object if successful. Otherwise, None is returned
    """
    from os import path as OP
    
    # PROJECT SPECIFIC DEFINES. FACTOR OUT.
    init_file = dag_utils.open_user_init()

    if not init_file:
        raise dag_utils.DagException("Could not open init file. File not found.")

    exec(compile(init_file.read(), init_file.name, 'exec'))

    root_dag = dag.DAG()
    with open(input_filename,"rb") as file:
        for line in file:
            line = line.strip()
            if len(line) == 0:
                continue
            if line[0] == '#':
                continue
            tokens = line.split(' ')
            pname = tokens[0]
            parser_args = tokens[1:]
            if not pname in parsers.keys():
                print("No function for %s" % pname)
                print("Known functions: ", parsers.keys())
                return None
            print("Running %s(parser_args)" % parsers[pname])
            funct = "%s(parser_args)" % parsers[pname]
            proc_list = eval(funct)

            if proc_list is None:
                continue

            for i in proc_list:
                root_dag.add_process(i)

    return root_dag


def gsub(input,start_jobs = True):
    """
    Reads a file containing a list of commands and parses them
    into workunits to be run on the grid. if start_jobs is true,
    workunites that are ready to be run are submitted to the scheduler.

    Arguments:
    \tinput -- String filename of commands to be parsed
    \tstart_jobs -- Boolean indicating whether jobs should be started
    \t\tif they are ready (Default: True).
    """
    import os
    from os import path as OP

    def save_dag(the_dag, fn):
        import stat
        print("Saved DAG as %s" % the_dag.save(fn))
        os.chmod(fn, stat.S_IRUSR|stat.S_IWUSR|stat.S_IRGRP|stat.S_IWGRP)
        

    parsers = {}

    dagfile = "jobs.dag"
    if OP.isfile(dagfile):
        raise Exception("Jobs queue file already exists: \"%s\"" % dagfile)

    root_dag = create_dag(input,parsers)
    if root_dag is None:
        raise dag_utils.DagException("Could not create DAG using submission file %s" % input)

    save_dag(root_dag,dagfile)
    if not start_jobs:
        return root_dag
    
    abs_dag_path = OP.abspath(dagfile)
    try:
        dag_utils.create_work(root_dag,abs_dag_path)
    except Exception as e:
        import traceback
        print("Exception thrown creating work")
        print("Message: %s" % e.message)
        traceback.print_exc()

    save_dag(root_dag,dagfile)
    return root_dag
    


if __name__ == "__main__":
    from sys import argv
    from getopt import getopt

    start_jobs = True
    
    (optlist,args) = getopt(argv[1:],'sv',['setup_only','version'])
    
    for (opt,val) in optlist:
        while opt[0] == '-':
            opt = opt[1:]
        if opt in ['v','version']:
            print("0.0")
            exit(0)
        elif opt in ['s','setup_only']:
            start_jobs = False
        else:
            from sys import stderr
            stderr.write("Unknown option '%s'\n" % opt)
            exit(1)


    if len(args) == 0:
        print("Usage: gsub <filename>")
        exit(1)
    if gsub(args[0],start_jobs) == None:
        exit(1)
