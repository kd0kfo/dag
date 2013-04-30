#!/usr/bin/env python
"""
dag.gsub
========

@author: David Coss, PhD
@date: November 7, 2012
@license: GPL version 3 (see COPYING or http://www.gnu.org/licenses/gpl.html for details)

This python module provides interface between BOINC C API and Python user code.
"""
import dag
import dag.util as dag_utils

def create_dag(input_filename, parsers, init_file = None):
    """
    Takes an input file that contains a list of commands and generates a dag.
    Jobs that have all of their prerequisites met are started, unless the
    --setup_only flag is provided.

    Arguments:
    input_filename -- String filename to be parsed into a DAG
    parsers -- Dictionary that maps string command names to functions that
    are used to create DAG.
    
    Returns: dag.DAG object if successful. Otherwise, None is returned
    """
    
    # PROJECT SPECIFIC DEFINES. FACTOR OUT.
    if not init_file:
        init_file = dag_utils.open_user_init()

    if not init_file: # If we still don't have the init file, there is a problem.
        raise dag.DagException("Could not open init file. File not found.")

    exec(compile(init_file.read(), init_file.name, 'exec'))

    root_dag = dag.DAG()
    parser_kmap = {}
    with open(input_filename,"rb") as infile:
        for line in infile:
            line = line.strip()
            if len(line) == 0:
                continue
            if line[0] == '#':
                if line[0:7] == "#define":
                    kmap_tokens = line.split()
                    if not kmap_tokens or len(kmap_tokens) < 2:
                        dag.DagException("Invalid define line.\nExpected:\n#define key [value]\nReceived:\n{0}".format(line))
                    if len(kmap_tokens) == 2:
                        parser_kmap[kmap_tokens[1]] = True
                    else:
                        parser_kmap[kmap_tokens[1]] = kmap_tokens[2:]
                continue
            tokens = line.split(' ')
            for token in tokens:
                if not token:
                    tokens.remove(token)
            pname = tokens[0]
            parser_args = tokens[1:] # used by function below
            if not pname in parsers.keys():
                print("No function for %s" % pname)
                print("Known functions: ", parsers.keys())
                return None
            print("Running %s(parser_args,parser_kmap)" % parsers[pname])
            
            funct = "%s(parser_args,parser_kmap)" % parsers[pname]
            proc_list = eval(funct) # uses parser_args

            if proc_list is None:
                continue

            for i in proc_list:
                root_dag.add_process(i)

    return root_dag


def gsub(input_filename,start_jobs = True,dagfile = dag.DEFAULT_DAGFILE_NAME,init_filename=None,engine=dag.Engine.BOINC):
    """
    Reads a file containing a list of commands and parses them
    into workunits to be run on the grid. if start_jobs is true,
    workunites that are ready to be run are submitted to the scheduler.

    @type input: String
    @param input: filename of commands to be parsed
    
    @type start_jobs: Boolean
    @param start_jobs: Indicates whether jobs should be started if they are ready (Default: True).
    """
    import os
    from os import path as OP
    import stat
    
    def save_dag(the_dag, fn):
        from stat import S_IRUSR,S_IWUSR,S_IRGRP,S_IWGRP
        print("Saved DAG as %s" % the_dag.save(fn))
        os.chmod(fn, S_IRUSR|S_IWUSR|S_IRGRP|S_IWGRP)
        
    parsers = {}

    if OP.isfile(dagfile):
        raise Exception("Jobs queue file already exists: \"%s\"" % dagfile)

    # Init file
    init_file = None
    if init_filename:
        try:
            init_file = open(init_filename,"r")
        except IOError as ioe:
            from sys import stderr
            stderr.write("Could not read init file '%s'\n" % init_filename)
            stderr.write("Reason: %s\n" % ioe.stderr)
            exit(ioe.errno)
            
    root_dag = create_dag(input_filename,parsers,init_file)
    if root_dag is None:
        raise dag.DagException("Could not create DAG using submission file %s" % input)
    root_dag.engine = engine
    save_dag(root_dag,dagfile)
    
    # Check to see if the directory is writable. If not, issue warning.
    dir_stat = os.stat(os.getcwd())
    if dir_stat.st_mode & stat.S_IWUSR == 0:# Highly unlikely.
        print("Warning - User cannot write to directory.")
    if dir_stat.st_mode & stat.S_IWGRP == 0:# Quite possible.
        print("Warning - Group cannot write to directory.")

    if not start_jobs:
        return root_dag
    
    abs_dag_path = OP.abspath(dagfile)
    try:
        import dag
        if root_dag.engine == dag.Engine.BOINC:
            import dag.boinc
            dag.boinc.create_work(root_dag,abs_dag_path,True)
        elif root_dag.engine == dag.Engine.LSF:
            import dag.lsf
            dag.lsf.create_work(root_dag,abs_dag_path, True)
    except Exception as e:
        import traceback
        print("Exception thrown creating work")
        print("Message: %s" % e.message)
        traceback.print_exc()

    root_dag.save()
    return root_dag
    
