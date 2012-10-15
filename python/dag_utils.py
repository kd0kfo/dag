import dag

project_path = '/boinc/projects/stjudeathome'

class NoDagMarkerException(dag.DagException):
    """
    Missing marker for workunit's DAG
    """
    pass

def dump_traceback(e):
    """
    Writes traceback information to standard output.

    Arguments: Exception
    No return value.
    """
    
    import sys
    import traceback
    exc_type, exc_value, exc_traceback = sys.exc_info()
    print("*** print_tb:")
    traceback.print_tb(exc_traceback, limit=1, file=sys.stdout)
    print("*** print_exception:")
    traceback.print_exception(exc_type, exc_value, exc_traceback,
                              limit=2, file=sys.stdout)
    print("*** print_exc:")
    traceback.print_exc()
    print("*** format_exc, first and last line:")
    formatted_lines = traceback.format_exc().splitlines()
    print(formatted_lines[0])
    print(formatted_lines[-1])
    print "*** format_exception:"
    print(repr(traceback.format_exception(exc_type, exc_value,exc_traceback)))
    print("*** extract_tb:")
    print(repr(traceback.extract_tb(exc_traceback)))
    print("*** format_tb:")
    print(repr(traceback.format_tb(exc_traceback)))
    print("*** tb_lineno:", exc_traceback.tb_lineno)
        
def open_user_init():
    from os import getenv
    import os.path as OP

    file_path = getenv('HOME')
    if not file_path:
        raise dag.DagException("Could not obtain Home Directory variable: $HOME")

    file_path = OP.join(file_path,".boincdag")

    if not OP.isfile(file_path):
        return None

    return open(file_path,"r")
    

def unique_input_name(proc, file):
    """
    Creates a unique file name for input files associated with a work unit.
    
    Arguments: proc -- dag.Process of the workunit
    file -- dag.File
    
    Returns: String representation of the file name
    """    
    import os.path as OP
    return "%s-%s" % (proc.workunit_name, file.physical_name)

def dir_hier_path(original_filename):
    """
    Wrapper for BOINC dir_hier_path.

    Arguments: original_filename String representation of the filename
    
    Returns: UTF-8 representation of the full path to file under the download hierarchy tree

    Raises: dag.DagException if dir_hier_path fails.
    """
    import subprocess as SP
    import os
    from os import path as OP
    orig_pwd = os.getcwd()
    os.chdir(project_path)

    retval = SP.Popen(["bin/dir_hier_path",original_filename],stdout=SP.PIPE,stderr=SP.PIPE)
    (tmpl_path,errmsg) = retval.communicate()
    os.chdir(orig_pwd)
    if retval.wait() != 0:
            
        raise dag.DagException("Error calling dir_hier_path for file \"%s\"\nError Message: %s" % (file.physical_name,errmsg))
    return tmpl_path.decode('utf-8').strip()
        

def create_workunit_template(proc):
    """
    Creates a workunit template file and returns the full path of the file.

    Arguments: proc -- dag.Process to be used to create template
    Results: dag.File object for the template
    """
    import tempfile
    import os,os.path
    tmpl_path = os.path.join(project_path,"templates")
    with tempfile.NamedTemporaryFile(mode='w',delete=False,dir=tmpl_path) as file:
        file.write("""
<input_template>""")
        for i in list(range(len(proc.input_files))):
            file.write("""
     <file_info>
          <number>%s</number>
     </file_info>\n""" % i)
        file.write("     <workunit>\n")
        child_counter = 0
        for i in proc.input_files:
            file.write("""
          <file_ref>
               <file_number>%d</file_number>
               <file_name>%s</file_name>
               <open_name>%s</open_name>
               <copy_file/>
          </file_ref>
        """ % (child_counter, unique_input_name(proc,i), i.logical_name))
            child_counter += 1
        if len(proc.args) != 0:
            file.write("""
        <command_line> %s </command_line>\n""" % " ".join(proc.args))
        file.write("<rsc_fpops_bound>%e</rsc_fpops_bound>\n" % proc.rsc_fpops_bound)
        file.write("<rsc_fpops_est>%e</rsc_fpops_est>\n" % proc.rsc_fpops_est)
        file.write("<rsc_memory_bound>%e</rsc_memory_bound>\n" % proc.rsc_memory_bound)
        file.write("""
     </workunit>
</input_template>""")
      
        return dag.File(file.name)

def create_result_template(proc,filename=None):
    """
    Creates a result template file and returns the full path of the file.

    Arguments: proc -- dag.Process to be used to create template
    filename -- Option string filename to be used as the template
    
    Results: dag.File object for the template
    """
    import tempfile
    import os,os.path
    if filename == None:
        tmpl_path = os.path.join(project_path,"templates")
        file = tempfile.NamedTemporaryFile(mode='w',delete=False,dir=tmpl_path)
    else:
        file = open(filename,"w")
    
    file.write("""
<output_template>""")
        
    file_counter = 0
    for i in proc.output_files:
        file.write("""
<file_info>
    <name><OUTFILE_%d/></name>
    <generated_locally/>
    <upload_when_present/>
    <max_nbytes>%d</max_nbytes>
    <url><UPLOAD_URL/></url>
</file_info>
""" % (file_counter,i.max_nbytes))
        file_counter += 1
        
    file.write("""<result>""")
    file_counter = 0
    for i in proc.output_files:
        file.write("""
    <file_ref>
        <file_name><OUTFILE_%d/></file_name>
        <open_name>%s</open_name>
        <copy_file/>
    </file_ref>
""" % (file_counter,i.physical_name))
        file_counter += 1

    file.write("""</result>
</output_template>
""")
    return dag.File(file.name)

def stage_files(proc,source_dir = None, set_grp_perms = True, overwrite = True):
    """
    Marshals input files to the grid server

    Arguments: proc -- dag.Process to be staged
    \tsource_dir -- String directory of the input files that are relative
    \t\tfilenames (Optional. Default: None)
    \tset_grp_perms -- Boolean indicator as to whether or not the group
    \t\tshould be given write access to the destination files. (Optional.
    \t\tDefault: True)
    \toverwrite -- Boolean indicator as to whether or not the destination files
    \t\tshould be overwritten. (Optional. Default: True)
    No Return value
    Raises dag.DagException if the file copy fails
    """
    import os.path as OP
    import os
    import subprocess as SP
    import dag
    import stat
    
    if not source_dir:
        source_dir = os.getcwd()
    orig_pwd = os.getcwd()
    os.chdir(project_path)
    for file in proc.input_files:
        unique_name = unique_input_name(proc,file)
        tmpl_path = dir_hier_path(unique_name)
        if not overwrite and OP.exists(tmpl_path):
            continue
        if file.dir:
            source_path = file.full_path()
        else:
            source_path = OP.join(source_dir,file.physical_name)
        retval = SP.Popen("cp {0} {1}".format(source_path,tmpl_path).split(),stdout=SP.PIPE,stderr=SP.PIPE)
        if retval.wait() != 0:
            errmsg = retval.communicate()[1]
            raise dag.DagException("Could not copy file {0} to {1}.\nError Message: {2}".format(source_path,tmpl_path,errmsg))
        if set_grp_perms:
            os.chmod(tmpl_path,stat.S_IRUSR|stat.S_IWUSR|stat.S_IRGRP|stat.S_IWGRP)
    os.chdir(orig_pwd)

def dag_marker_filename(wuname):
    """
    Takes a workunit name and returns a string path within
    the dag_lists directory of the project. This is a fan out directory 
    tree similar to downloads/

    Arguments: wuname -- String representation of the workunit name
    Returns: String filename for a dag marker (file not created).
    Raises dag.DagException if dag_lists is not created
    """
    from os import path as OP
    import os

    cwd = os.getcwd()
    if cwd != project_path:
        os.chdir(project_path)

    if not OP.isdir("dag_lists"):
        print("In dir: %s" % os.getcwd())
        raise dag.DagException("Missing dag_lists in project directory: '%s'" % project_path)
    os.chdir("dag_lists")

    marker_filename = dir_hier_path(wuname).replace("download/","dag_lists/")
    marker_dir = OP.dirname(marker_filename)
    if not OP.isdir(marker_dir):
        os.mkdir(marker_dir)
        os.chmod(marker_dir,0777)

    if cwd != os.getcwd():
        os.chdir(cwd)
        
    return marker_filename

def make_dag_marker(wuname, dag_path):
    """
    Creates the dag marker file.

    Arguments: wuname -- String representation of the workunit name
    dag_path -- String representation of the path to the DAG

    Return: Filename of dag marker file
    Throws Exception if the dag_lists directory does not exist.
    """
    from os import getuid
    
    marker_filename = dag_marker_filename(wuname)
    dag_marker = open(marker_filename,"w")
    dag_marker.write("%d %s\n" % (getuid(), dag_path))
    dag_marker.close()

    return marker_filename

def marker_to_dagpath(filename):
    """
    Parses a DAG marker file and returns the path to the DAG
    
    Arguments: filename -- String representation of the marker's path
    Returns: String path to DAG file
    """
    with open(filename,"r") as file:
        line = file.readline()
        (uid, dagpath) = line.split(" ")
        return dagpath.strip()
    
def result_to_dag(result_name):
    """
    Takes a BOINC result name and returns the corresponding DAG.

    Arguments: result_name -- String representation of the result name.
    Returns: dag.DAG object for the result
    Raises NoDagMarkerException if the result does not have a dag marker file.
    """
    import re
    import os.path as OP
    
    wuname = re.findall(r"^(.*)_\d*$",result_name)
    if len(wuname) == 0:
            print("Malformed result name")
            return None
    wuname = wuname[0]
    marker_path = dag_marker_filename(wuname)

    try:
        dagpath = marker_to_dagpath(marker_path)
    except IOError as ioe:
        raise NoDagMarkerException("Missing DAG marker.\nError Message: %s\nFile: %s" % (ioe.strerror, marker_path))
    
    dagdir = OP.split(dagpath)[0]
    try:
        return dag.load(dagpath)
    except Exception as e:
        print("Error loading dag file '%s' listed in marker '%s'" % (dagpath, marker_path))
        raise e

def cancel_workunits(procs):
    """
    Calls a custom cancel_jobs that will remove workunits based on
    workunit name.

    Arguments: List of Processes.
    Returns: no return value
    Throws Exception if create_work fails
    """
    import os.path as OP
    import os
    import subprocess as SP
    import dag

    if not procs:
        return

    cmd_list = ["bin/cancel_jobs","--by_name"] + [proc.workunit_name for proc in procs]

    orig_pwd = os.getcwd()
    os.chdir(project_path)

    retval = SP.Popen(cmd_list,stdout=SP.PIPE,stderr=SP.PIPE)
    if retval.wait() != 0:
        errmsg = retval.communicate()[1]
        raise dag.DagException("Could not cancel work unit.\nRan: %s\n\nError Message: %s\n" % (" ".join(cmd_list), errmsg.decode('utf-8')))

    for proc in procs:
        proc.state = dag.States.FAIL

    os.chdir(orig_pwd)

def schedule_work(proc, dag_path):
    """
    Calls create_work. If create_work fails, an exception is raised.
    If create_work succeeds, a file is created that lists the dag file for the work unit.
    
    Parameters: Process, Absoulte path to dag file
    Returns: no return value
    Throws Exception if create_work fails or if the dag marker file cannot be created.
    """
    
    import os.path as OP
    import os
    import subprocess as SP
    import dag
    orig_pwd = os.getcwd() + OP.sep
    os.chdir(project_path)
    wu_tmpl = OP.split(proc.workunit_template.physical_name)[1]
    res_tmpl = OP.split(proc.result_template.physical_name)[1]
    cmd_args = "--appname %s --wu_name %s --wu_template templates/%s --result_template templates/%s" % (proc.cmd, proc.workunit_name, wu_tmpl, res_tmpl)
    for input in proc.input_files:
        cmd_args += " %s" % unique_input_name(proc,input)
    cmd_list = ["{0}{1}bin{1}create_work".format(project_path, OP.sep)]
    cmd_list.extend(cmd_args.split())
    retval = SP.Popen(cmd_list,stdout=SP.PIPE,stderr=SP.PIPE)
    if retval.wait() != 0:
        errmsg = retval.communicate()[1]
        raise dag.DagException("Could not create work.\nRan: %s\n\nError Message: %s\n" % (" ".join(cmd_list), errmsg.decode('utf-8')))
    make_dag_marker(proc.workunit_name,dag_path)
    os.chdir(orig_pwd)

def create_work(the_dag,dagfile):
    """
    Creates a workunit by processing the dag and running stage_files and schedule_work.

    Sets the workunit information in the dag.Process objects

    Arguments: dag and dagfile path to dag file
    Returns: no value
    Throws Exception if an input file does not exist and is not part of a parent process.
    """
    import os
    import os.path as OP
    import random, stat
    import dag
    

    if the_dag.processes == None:
        return
    for proc in the_dag.processes:
        defer = False

        #create process name
        proc.workunit_name = "%s-%09d" % (proc.cmd,int(random.random()*1000000000))
        #setup workunit templates
        wu_tmpl = proc.workunit_template
        if wu_tmpl == None or not OP.isfile(wu_tmpl.physical_name):
            wu_tmpl = create_workunit_template(proc)
            OP.os.chmod(wu_tmpl.full_path(),stat.S_IROTH | stat.S_IRUSR | stat.S_IWUSR | stat.S_IRGRP | stat.S_IWGRP)
        res_tmpl = proc.result_template
        if res_tmpl == None or not OP.isfile(res_tmpl.physical_name):
            res_tmpl = create_result_template(proc)
            OP.os.chmod(res_tmpl.full_path(),stat.S_IROTH | stat.S_IRUSR | stat.S_IWUSR | stat.S_IRGRP | stat.S_IWGRP)
        proc.workunit_template = wu_tmpl # update process objects
        proc.result_template = res_tmpl

        for input in proc.input_files: # validate input files
            if input in the_dag.graph.keys():
                for parent_proc in the_dag.graph[input]:
                    parent_proc.children.append(proc)
                    defer = True
                continue # input file loop
            if not OP.isfile(input.full_path()):
                print("Missing input file not produced by other process: \"%s\"" % input.physical_name)
                print("Have the following files:")
                for i in the_dag.graph.keys():
                    print("%s (%s)" % (i.physical_name, i.logical_name))
                raise dag.DagException("Missing File")
                
        if defer:
            print("Deferring %s" % proc.cmd)
            continue
        # Job not deferred. Run it!
        stage_files(proc)
        proc.state = dag.States.STAGED
        the_dag.save()
        schedule_work(proc,dagfile)
        proc.state = dag.States.RUNNING
        the_dag.save()

def remove_templates(proc):
    """
    Removes (unlinks) template files of the dag.Process object
    
    No return value
    """
    import os
    import os.path as OP

    for fn in [proc.result_template,proc.workunit_template]:
        if not fn:
            continue
        if OP.isfile(fn.full_path()):
            os.unlink(fn.full_path())

def remove_workunit(root_dag, proc):
    """
    Removes and cleans a process from the DAG. This function
    will remove temporary files and connections to child processes.
    
    Arguments: root_dag -- dag.DAG object containing processes
    proc -- dag.Process object to be removed
    
    No return value
    """
    import os.path as OP
    import os
    
    if proc == None:
        return

    #Remove connection(s) to child node(s)
    for output in proc.output_files:
        root_dag.graph[output].remove(proc)
        if len(root_dag.graph[output]) == 0:
            root_dag.graph.pop(output)

    remove_templates(proc)
    for tmpl in proc.input_files + proc.output_files:
        if tmpl.temp_file:
            tmpl.unlink()
        
    if proc.workunit_name:
        marker_filename = dag_marker_filename(proc.workunit_name)
        if OP.isfile(marker_filename):
            os.unlink(marker_filename)

    #Remove process from list of processes
    root_dag.processes.remove(proc)

def start_children(proc,root_dag,dag_filename):
    """
    Checks to see if children may be run. If they can, they are started.
    Assumes the children's files are located in the same directory as the DAG,
    if the file paths are not absolute.

    Raises Exception if the create_work call fails.
    """
    import dag_utils
    import dag
    import os.path as OP
    
    defer = False
    for child in proc.children:
        print("Can we start %s" % child)
        if child.state == dag.States.RUNNING:
            print("Already running")
            continue
        for input in child.input_files:
            if input in root_dag.graph:
                for parent_proc in root_dag.graph[input]:
                    if parent_proc.state != dag.States.SUCCESS:
                        print("No. Not all Processes are finished.")
                        defer = True
                        break
            if defer:
                break;
        if defer:
            defer = False
            continue
        
        dag_utils.stage_files(child,source_dir = OP.dirname(root_dag.filename),overwrite = False)
        child.state = dag.States.STAGED
        root_dag.save()
        dag_utils.schedule_work(child,root_dag.filename)
        child.state = dag.States.RUNNING
        root_dag.save()

def update_process(result):
    """
    Takes a BOINC result object and updates the state of the 
    corresponding process. Based on the result name, the dag
    file is found from the dag marker. Then the process in the
    dag file is updated based on the workunit name.
    
    Arguments: result -- BoincResult object to be updated.
    
    No return value
    
    Raises: dag.DagException if the result name is malformed or if
    there is no such process by the result name.
    """
    import dag
    import re

    proc_name = result.name
    
    wuname = re.findall(r"^(.*)_\d*$",proc_name)
    if len(wuname) == 0:
        raise dag.DagException("Malformed result name: '%s'" % proc_name)
    wuname = wuname[0]

    try:
        root_dag = result_to_dag(proc_name)
    except NoDagMarkerException as ndme:
        print("Missing dag %s\Skipping update.")
        return

    proc = root_dag.get_process(wuname)

    if not proc:
        raise dag.NoSuchProcess("No such process: %s" % proc_name)

    print("Update process has proc for %s which exited %d" % (proc.workunit_name, result.exit_status))

    if result.validate_state not in [1,2]: # 1 = valid, 2 = invalid
        print("Result %s not yet validated. Skipping" % result.name)
        return
    if result.validate_state == 1 and result.exit_status == 0:
        proc.state = dag.States.SUCCESS
    else:
        proc.state = dag.States.FAIL
    root_dag.save()
    print("Changed process state to %s" % dag.strstate(proc.state))

def continue_children(proc_name):
    """
    Tries to start child process of the process corresponding 
    to the provided BOINC result name.

    Arguments: proc_name -- String representation of the result name
    
    No return value.
    
    Raises: dag.DagException if the result name is malformed or if there
    is not dag file for the given result name.
    """    
    import dag,dag_utils
    import re

    wuname = re.findall(r"^(.*)_\d*$",proc_name)
    if len(wuname) == 0:
            raise dag.DagException("Malformed result name")
    wuname = wuname[0]

    try:
        root_dag = result_to_dag(proc_name)
        dag_filename = dag_marker_filename(wuname)
    except dag_utils.NoDagMarkerException as ndme:
        print("Missing dag %s\nSkipping clean up")
        return
    except Exception as e:
        print("Error loading dag for %s" % proc_name)
        raise e
    
    if not root_dag:
        raise dag.DagException("Could not load DAG for process: %s" % proc_name)

    print("Getting process for %s" % wuname)
    proc = root_dag.get_process(wuname)

    if not proc:
        raise dag.NoSuchProcess("No such process: %s" % proc_name)

    print("Starting children... State is %s" % dag.strstate(proc.state))
    start_children(proc,root_dag,dag_filename)
    print("Started Children")

def assimilate_workunit(result_list,canonical_result):
    """
    Takes a list of BoincResult objects and updates their processes
    in the DAG based on the outcome of the canonical result.
    
    Arguments: result_list -- List of BoincResult objects for a workunit
    canonical_result -- BoincResult object representing the canonical result
    
    No return value.
    
    Raises: dag.DagException if no canonical result is found
    """
    if not canonical_result:
        raise dag.DagException("No canonical result provided.")
    try:
        if canonical_result.id == 0:
            print("Non canonical result found")
            if result_list:
                update_process(result_list[0])
        else:
            update_process(canonical_result)
    
    except NoDagMarkerException as de:
        print("Missing Dag Marker. Skipping...")
    

def save_bad_res_output(filename,wuname):
    """
    Copies a result output file and saves it to the invalid_results
    subdirectory of the project. The file is put in a subdirectory
    named after the workunit.

    Arguments:
    \tfilename -- String name of file to be copied
    \twuname -- String name of workunit

    Returns nothing.

    Raises a dag.DagException if the invalid_results directory does not
    exist in the project path.
    """
    
    import os.path as OP
    import shutil

    if not OP.isfile(filename):
        return
    
    bad_res_path = OP.join(project_path,"invalid_results")
    if OP.isdir(bad_res_path):
        import os
        bad_wu_path = OP.join(bad_res_path,wuname)
        if not OP.isdir(bad_wu_path):
            try:
                os.mkdir(bad_wu_path)
            except:
                bad_wu_path = bad_res_path
        shutil.copy(filename,bad_wu_path)
    else:
        raise dag.DagException("'invalid_results' directory does not exist. Data lost.")
