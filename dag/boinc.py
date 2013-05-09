#!/usr/bin/env python
"""
dag.boinc
=========


@author: David Coss, PhD
@date: November 7, 2012
@license: GPL version 3 (see COPYING or http://www.gnu.org/licenses/gpl.html for details)

Interface module to boinctools. This allows processes in a DAG to interpret and use BOINC data.
"""

import boinctools

forbidden_wu_names = ["all"]

def unique_input_name(proc, file):
    """
    Creates a unique file name for input files associated with a work unit.
    
    @type proc: dag.Process
    @param proc: Process of the workunit
    @type file: dag.File
    @param file: File for which a BOINC-unique name is needed.
    
    @rtype: String
    @return: Unique file name
    """    
    import os.path as OP
    return "%s-%s" % (proc.get_unique_name(), file.physical_name)

def create_workunit_template(proc):
    """
    Creates a workunit template file and returns the full path of the file.

    @type proc: dag.Process
    @param proc: Process to be used to create template
    @rtype: dag.File
    """
    import dag
    import tempfile
    import os,os.path
    
    tmpl_path = os.path.join(boinctools.project_path,"templates")
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
               <open_name>%s</open_name>
               <copy_file/>
          </file_ref>
        """ % (child_counter, i.logical_name))
            child_counter += 1
        if len(proc.args) != 0:
            file.write("""
        <command_line> %s </command_line>\n""" % proc.args)
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

    @type proc: dag.Process
    @param proc: Process to be used to create template
    @type filename: String
    @param filename: Filename to be used as the template
    
    @rtype: dag.File
    """

    import dag
    import tempfile
    import os,os.path
    if filename == None:
        tmpl_path = os.path.join(boinctools.project_path,"templates")
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

    @param proc: Process to be staged
    @type proc: dag.Process
    @type source_dir: String
    @param source_dir: String directory of the input files that are relative filenames (Default: None)
    @param set_grp_perms: Boolean
    @type set_grp_perms: Indicator as to whether or not the group should be given write access to the destination files. (Default: True)
    @type overwrite: Boolean
    @param overwrite: indicator as to whether or not the destination files should be overwritten. (Default: True)
    @raise dag.DagException: if the file copy fails
    """

    import boinctools
    import os.path as OP
    from dag import GridProcess
    
    unique_names = {}

    if not isinstance(proc,GridProcess):
        return

    if not source_dir:
        from os import getcwd
        source_dir = getcwd()

    for file in proc.input_files:
        if file.dir:
            source_path = file.full_path()
        else:
            source_path = OP.join(source_dir,file.physical_name)
        unique_names[source_path] = unique_input_name(proc,file)
    
    boinctools.stage_files(unique_names,source_dir, set_grp_perms, overwrite)


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
    import dag
    project_path = boinctools.project_path
    cwd = os.getcwd()
    if cwd != project_path:
        os.chdir(project_path)

    if not OP.isdir("dag_lists"):
        raise dag.DagException("Missing dag_lists in project directory: '%s'\nCurrent working directory: %s" % (project_path,os.getcwd()))
    os.chdir("dag_lists")

    marker_filename = boinctools.dir_hier_path(wuname).replace("download/","dag_lists/")
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

def name_result2workunit(result_name):
    """
    Takes a BOINC result name and returns the workunit name

    @param result_name: Name of Result
    @type result_name: String
    @return: Name of Workunit/Process
    @rtype: String
    """
    import re
    wuname = re.findall(r"^(.*)_\d*$",result_name)
    if not wuname:
            print("Malformed result name '%s'" % result_name)
            return None
    return wuname[0]
    
    
def result_to_dag(result_name):
    """
    Takes a BOINC result name and returns the corresponding DAG.

    Arguments: result_name -- String representation of the result name.
    Returns: dag.DAG object for the result
    Raises dag_utils.NoDagMarkerException if the result does not have a dag marker file.
    """
    import dag
    import dag.util as dag_utils
    import os.path as OP

    wuname = name_result2workunit(result_name)
    
    marker_path = dag_marker_filename(wuname)

    try:
        dagpath = marker_to_dagpath(marker_path)
    except IOError as ioe:
        raise dag_utils.NoDagMarkerException("Missing DAG marker.\nError Message: %s\nFile: %s" % (ioe.strerror, marker_path))
    
    dagdir = OP.split(dagpath)[0]
    if not OP.isfile(dagpath):
        raise dag.MissingDAGFile("Missing dag file '%s' listed in marker '%s'" % (dagpath, marker_path))
    try:
        return dag.load(dagpath)
    except Exception as e:
        print("Error loading dag file '%s' listed in marker '%s'" % (dagpath, marker_path))
        raise e

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
    import boinctools

    if proc.workunit_name in forbidden_wu_names:
        raise Exception("The name '%s' is not allowed because it is a reserved word in the DAG system." % proc.workunit_name)

    wu_tmpl = OP.split(proc.workunit_template.physical_name)[1]
    res_tmpl = OP.split(proc.result_template.physical_name)[1]
    input_filenames = [unique_input_name(proc,input) for input in proc.input_files]
    delay_bounds = None
    if hasattr(proc,"deadline"):
        delay_bounds = proc.deadline
    boinctools.schedule_work(proc.cmd,proc.get_unique_name(),wu_tmpl,res_tmpl,input_filenames,delay_bounds)
    make_dag_marker(proc.workunit_name,dag_path)

def create_work(the_dag,dagfile, show_progress = False):
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
    
    progress_bar = None
    progress_bar_counter = 0

    if the_dag.processes == None:
        return

    if show_progress:
        from progressbar import ProgressBar, Percentage, Bar
        progress_bar = ProgressBar(widgets = [Percentage(), Bar()], maxval=len(the_dag.processes)).start()
        print("Submitting %d jobs" % len(the_dag.processes))
        

    for proc in the_dag.processes:
        if progress_bar:
            progress_bar_counter += 1
            progress_bar.update(progress_bar_counter)

        defer = False
        if proc.state not in [dag.States.CREATED,dag.States.STAGED]:
            continue

        #create process name, if one does not already exist
        if not proc.workunit_name:
            proc.workunit_name = "%s-%09d" % (proc.cmd,int(random.random()*1000000000))
        #setup workunit templates
        if isinstance(proc,dag.GridProcess):
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

        
        if the_dag.incomplete_prereqs(proc):
            print("Missing input file not produced by other process: \"%s\"" % input.physical_name)
            print("Have the following files:")
            for i in the_dag.graph.keys():
                print("%s (%s)" % (i.physical_name, i.logical_name))
            raise dag.DagException("Missing File")
        
                
        if defer:
            continue
        # Job not deferred. Run it!
        if isinstance(proc,dag.GridProcess):
            stage_files(proc)
            proc.state = dag.States.STAGED
            the_dag.save()
            schedule_work(proc,dagfile)
            proc.state = dag.States.RUNNING
        else:
            try:
                proc.start()
                proc.state = dag.States.SUCCESS
            except Exception as e:
                proc.state = dag.States.FAIL
                the_dag.save()
                raise e
            
        the_dag.save()

    # Restore use of line returns from progress bar
    if progress_bar:
        print("")

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

def clean_workunit(root_dag, proc):
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
        if not output in root_dag.graph:
            continue
        if proc in root_dag.graph[output]:
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

def start_children(proc,root_dag,dag_filename):
    """
    Checks to see if children may be run. If they can, they are started.
    Assumes the children's files are located in the same directory as the DAG,
    if the file paths are not absolute.

    Raises Exception if the create_work call fails.
    """
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
        
        stage_files(child,source_dir = OP.dirname(root_dag.filename),overwrite = False)
        child.state = dag.States.STAGED
        root_dag.save()
        schedule_work(child,root_dag.filename)
        child.state = dag.States.RUNNING
        root_dag.save()

def cancel_workunits(proc_list):
    import boinctools
    boinctools.cancel_workunits([proc.workunit_name for proc in proc_list])
