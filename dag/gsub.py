#!/usr/bin/env python
"""
dag.gsub
========

@author: David Coss, PhD
@date: November 7, 2012
@license: GPL version 3 (see COPYING or http://www.gnu.org/licenses/gpl.html
for details)

Parses job submission files and calls job preparation functions
for each command. A Directed Acyclic Graph of processes is produced.
Processes are run if a job submission engine is specified.
"""

import dag

internal_counter = 0


def preprocess_line(line, parser_kmap, dependencies):
    """
    Reads a line beginning with a '%' character and follows
    the associated logic.
    The line may produce new process for the DAG and keywords/values
    that are added to parser_kmap. The return value is a tuple that contains
    the new parser_kmap and a list of new processes, if any.


    @param line: Line from job submission script to be processed.
    @type line: string
    @param parser_kmap: Keyword map passed along between processes.
    These are populated by "%define" lines in the submission script.
    @type parser_kmap: dict
    @return: Tuple containing parser_kmap and a list of new processes
    @rtype: tuple
    """

    processes = []  # Extra processes that may be added to the DAG
    if line[0:7] == "%define":
        kmap_tokens = line.split()
        if not kmap_tokens or len(kmap_tokens) < 2:
            raise dag.DagException("Invalid define line.\n"
                                   "Expected:\n"
                                   "%define key [value]\n"
                                   "Received:\n{0}".format(line))
        if len(kmap_tokens) == 2:
            parser_kmap[kmap_tokens[1]] = True
        else:
            parser_kmap[kmap_tokens[1]] = kmap_tokens[2:]
    elif line[0:11] == "%dependency":
        depend_tokens = line.split()[1:]
        if not depend_tokens or len(depend_tokens) < 2:
            raise dag.DagException("Dependency directive requires a parent"
                                   " and a child process.")
        # item 1 is the parent of item 0
        if depend_tokens[1] in dependencies:
            dependencies[depend_tokens[1]].append(depend_tokens[0])
        else:
            dependencies[depend_tokens[1]] = [depend_tokens[0]]
    elif line[0:7] == "%python":
        from dag import InternalProcess, File
        import re
        global internal_counter
        list_syntax = "\[([^\[\]]*)\]"
        match = re.search("%python\s*{0}\s*{0}\s(.*)$".format(list_syntax),
                          line)
        if not match or len(match.groups()) != 3:
            raise dag.DagException("Invalid python line.\nRequire: input file"
                                   "list, output file list and python code.")
        params = match.groups()
        proc = InternalProcess(params[2].strip())
        proc.input_files = [File(f) for f in params[0].split(',') if f]
        proc.output_files = [File(f) for f in params[1].split(',') if f]
        proc.workunit_name = "internal-{0}".format(internal_counter)
        internal_counter += 1
        processes.append(proc)
    elif line[0:5] == "%nice":
        nice_increment = line.split()[-1]
        parser_kmap["nice"] = int(nice_increment)

    return (parser_kmap, processes, dependencies)


def create_dag(input_filename, parsers, init_file=None,
               engine=dag.Engine.SHELL, num_cores=None):
    """
    Takes an input file that contains a list of commands and generates a dag.
    Jobs that have all of their prerequisites met are started, unless the
    --setup_only flag is provided.

    @param input_filename: Filename to be parsed into a DAG
    @type input_filename: str
    @param parsers: Dictionary that maps string command names
    to functions that are used to create DAG.
    @type parsers: dict
    @param init_file: Optional file to be used for startup variables
    and routines.
    @type init_file: file
    @param num_processors: Optional number of processors used in multiprocessing.
    @type num_processors: int
    @return:  DAG object if successful. Otherwise, None is returned
    @rtype: dag.DAG
    """

    import dag.util as dag_utils
    from dag import DAG, Engine, DagException

    # PROJECT SPECIFIC DEFINES. FACTOR OUT.
    if not init_file:
        init_file = dag_utils.open_user_init()

    # If we still don't have the init file, there is a problem.
    if not init_file:
        if init_file is None:
            raise DagException("No init file provided.")
        else:
            raise DagException("Could not open init file ({0}). File not found."
                               .format(init_file.name))

    init_code = compile(init_file.read(), init_file.name, "exec")
    exec(init_code)

    root_dag = DAG()
    root_dag.engine = engine
    root_dag.num_cores = num_cores
    parser_kmap = {}  # used as the second argument of parser functions (below)
    # dependencies dict is used to allow the user
    # to define explicit dependencies.
    dependencies = {}
    with open(input_filename, "r") as infile:
        for line in infile:
            line = line.strip()
            if len(line) == 0:
                continue
            if line[0] == '#':  # Comment line continue
                continue
            if line[0] == '%':  # Preprocess
                (parser_kmap, extra_processes,
                 dependencies) = preprocess_line(line,
                                                 parser_kmap, dependencies)
                for extra_proc in extra_processes:
                    root_dag.add_process(extra_proc)
                continue
            tokens = line.split(' ')
            for token in tokens:
                if not token:
                    tokens.remove(token)
            pname = tokens[0]
            parser_args = tokens[1:]  # used by function below
            
            # Is the process name set explicitly?
            process_name = None # This is an option internal name for the process, AKA workunit_name
            if pname[0] == '@':
                process_name = pname[1:]
                pname = parser_args[0]
                parser_args = parser_args[1:]

            if root_dag.engine == Engine.SHELL:
                import dag.shell
                proc_list = dag.shell.parse_shell(pname, parser_args,
                                                  parser_kmap, parsers, init_code)
                num_procs = len(root_dag.processes)
                for proc in proc_list:
                    proc.workunit_name = "%s-%d" % (proc.cmd, num_procs)
                    num_procs += 1
            else:
                if not pname in parsers.keys():
                    print("No function for %s" % pname)
                    print("Known functions: ", parsers.keys())
                    raise DagException("Unknown Function: {0}".format(pname))

                funct = "%s(parser_args,parser_kmap)" % parsers[pname]
                print("Running %s" % funct)
                proc_list = eval(funct)   # uses parser_args

            if proc_list is None:
                continue

            # If given explicitly set workunit name
            if process_name:
                proc_count = 1
                use_suffix = len(proc_list) > 1
                for i in proc_list:
                    if use_suffix:
                        i.workunit_name = "%s-%d" % (process_name, proc_count)
                    else:
                        i.workunit_name = process_name
                    proc_count += 1


            for i in proc_list:
                root_dag.add_process(i)

    # Set explicit dependencies, if any
    for parent_name in dependencies:
        print("Added dependency of %s" % parent_name)
        NO_SUCH_FMT = "No such process '%s'"
        parent_process = root_dag.get_process(parent_name)
        if not parent_process:
            print(NO_SUCH_FMT % parent_name)
            continue
        for child in dependencies[parent_name]:
            child_proc = root_dag.get_process(child)
            if not child_proc:
                print(NO_SUCH_FMT % child)
                continue
            if child not in [proc.workunit_name for proc in parent_process.children]:
                parent_process.children.append(child_proc)
                print("%s depends on %s" % (child, parent_name))

    return root_dag


def gsub(input_filename, start_jobs=True, dagfile=dag.DEFAULT_DAGFILE_NAME,
         init_filename=None, engine=dag.Engine.BOINC, num_cores=None,
         queue_filename=None):
    """
    Reads a file containing a list of commands and parses them
    into workunits to be run on the grid. if start_jobs is true,
    workunites that are ready to be run are submitted to the scheduler.

    Lines beginning with '%' are considered directives for gsub itself.
    Current gsub directives are: %define, %python
    If '%' is followed by something other than the directive,
    the line is ignored.

    %define lines are passed to the parser function using a dict
    as the second argument, which maps the first string in the line
    (after %define) to the remain lines (as a list of strings).

    %python lines are interpreted by the python interpreter.

    @param input_filename: filename of commands to be parsed
    @type input_filename: String
    @param start_jobs: Indicates whether jobs should be started
    if they are ready (Default: True).
    @type start_jobs: Boolean
    @param dagfile: Optional DAG filename to be used to save the DAG object.
    File must not already exist.
    @type dagfile: str
    @param init_filename: File name of user parameters that is parsed before
    reading the job submission file.
    @type init_filename: str
    @param engine: System used to run processes.
    @type engine: dag.States
    @param queue_filename: Path to Message Queue File
    @type queue_filename: str 
    @return: DAG contain processes created by the job submission script.
    @rtype: dag.DAG
    @raise dag.DagException: If DAG file already exists or cannot be created or
    if the init file cannot be read.
    """
    import os
    from os import path as OP
    import stat
    from dag import Engine
    import dag

    def save_dag(the_dag, fn):
        from stat import S_IRUSR, S_IWUSR, S_IRGRP, S_IWGRP
        print("Saved DAG as %s" % the_dag.save(fn))
        os.chmod(fn, S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP)

    parsers = {}

    if OP.isfile(dagfile):
        raise dag.DagException("Jobs queue file already "
                               "exists: \"%s\"" % dagfile)

    # Init file
    init_file = None
    if init_filename:
        try:
            init_file = open(init_filename, "r")
        except IOError as ioe:
            errmsg = "Could not read init file '%s'\n" % init_filename
            if hasattr(ioe, "errno") and hasattr(ioe, "stderr"):
                errmsg += "Reason{0}: {1}\n".format(ioe.errno, ioe.stderr)
            raise dag.DagException(errmsg)
    else:
        from dag.util import open_user_init
        init_file = open_user_init()

    root_dag = create_dag(input_filename, parsers, init_file, engine, num_cores)
    if root_dag is None:
        raise dag.DagException("Could not create DAG using submission "
                               "file %s" % input)
    if not queue_filename:
        root_dag.queue_filename = "%s.mq" % dagfile
    else:
        root_dag.queue_filename = queue_filename
    save_dag(root_dag, dagfile)

    # Check to see if the directory is writable. If not, issue warning.
    dir_stat = os.stat(os.getcwd())
    if dir_stat.st_mode & stat.S_IWUSR == 0:  # Highly unlikely.
        print("Warning - User cannot write to directory.")
    if dir_stat.st_mode & stat.S_IWGRP == 0:  # Quite possible.
        print("Warning - Group cannot write to directory.")

    if not start_jobs:
        return root_dag

    abs_dag_path = OP.abspath(dagfile)
    try:
        if root_dag.engine == Engine.BOINC:
            import dag.boinc
            dag.boinc.create_work(root_dag, abs_dag_path, True)
        elif root_dag.engine == Engine.LSF:
            import dag.lsf
            dag.lsf.create_work(root_dag, abs_dag_path)
        elif root_dag.engine == Engine.SHELL:
            import dag.shell
            dag.shell.create_work(root_dag, abs_dag_path)
    except Exception as e:
        import traceback
        print("Exception thrown creating work")
        print("Message: %s" % e)
        traceback.print_exc()
        raise e

    root_dag.save()
    return root_dag
