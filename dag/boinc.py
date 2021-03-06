#!/usr/bin/env python
"""
dag.boinc
=========


@author: David Coss, PhD
@date: May 9, 2013
@license: GPL version 3 (see COPYING or http://www.gnu.org/licenses/gpl.html
 for details)

Interface module to boinctools. This allows processes in a DAG to interpret
 and use BOINC data.
"""

import boinctools

forbidden_wu_names = ["all"]


def unique_input_name(proc, infile):
    """
    Creates a unique file name for input files associated with a work unit.

    @type proc: dag.Process
    @param proc: Process of the workunit
    @type file: dag.File
    @param file: File for which a BOINC-unique name is needed.

    @rtype: String
    @return: Unique file name
    """
    return "%s-%s" % (proc.get_unique_name(), infile.physical_name)


def create_workunit_template(proc):
    """
    Creates a workunit template file and returns the full path of the file.

    @type proc: dag.Process
    @param proc: Process to be used to create template
    @rtype: dag.File
    """
    import dag
    import tempfile
    import os.path

    tmpl_path = os.path.join(boinctools.project_path, "templates")
    with tempfile.NamedTemporaryFile(mode='w',
                                     delete=False, dir=tmpl_path) as outfile:
        outfile.write("""
<input_template>""")
        for i in list(range(len(proc.input_files))):
            outfile.write("""
     <file_info>
          <number>%s</number>
     </file_info>\n""" % i)
        outfile.write("     <workunit>\n")
        child_counter = 0
        for i in proc.input_files:
            outfile.write("""
          <file_ref>
               <file_number>%d</file_number>
               <open_name>%s</open_name>
               <copy_file/>
          </file_ref>
        """ % (child_counter, i.logical_name))
            child_counter += 1
        if len(proc.args) != 0:
            outfile.write("""
        <command_line> %s </command_line>\n""" % proc.args)
        outfile.write("<rsc_fpops_bound>%e</rsc_fpops_bound>\n"
                      % proc.rsc_fpops_bound)
        outfile.write("<rsc_fpops_est>%e</rsc_fpops_est>\n"
                      % proc.rsc_fpops_est)
        outfile.write("<rsc_memory_bound>%e</rsc_memory_bound>\n"
                      % proc.rsc_memory_bound)
        outfile.write("""
     </workunit>
</input_template>""")

        return dag.File(outfile.name)


def create_result_template(proc, filename=None):
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
    import os.path

    if filename is None:
        tmpl_path = os.path.join(boinctools.project_path, "templates")
        outfile = tempfile.NamedTemporaryFile(mode='w',
                                              delete=False, dir=tmpl_path)
    else:
        outfile = open(filename, "w")

    outfile.write("""
<output_template>""")

    file_counter = 0
    for i in proc.output_files:
        outfile.write("""
<file_info>
    <name><OUTFILE_%d/></name>
    <generated_locally/>
    <upload_when_present/>
    <max_nbytes>%d</max_nbytes>
    <url><UPLOAD_URL/></url>
</file_info>
""" % (file_counter, i.max_nbytes))
        file_counter += 1

    outfile.write("""<result>""")
    file_counter = 0
    for i in proc.output_files:
        outfile.write("""
    <file_ref>
        <file_name><OUTFILE_%d/></file_name>
        <open_name>%s</open_name>
        <copy_file/>
    </file_ref>
""" % (file_counter, i.physical_name))
        file_counter += 1

    outfile.write("""</result>
</output_template>
""")
    return dag.File(outfile.name)


def stage_files(proc, source_dir=None, set_grp_perms=True, overwrite=True):
    """
    Marshals input files to the grid server

    @param proc: Process to be staged
    @type proc: dag.Process
    @type source_dir: String
    @param source_dir: String directory of the input files that are relative
     filenames (Default: None)
    @param set_grp_perms: Boolean
    @type set_grp_perms: Indicator as to whether or not the group should be
     given write access to the destination files. (Default: True)
    @type overwrite: Boolean
    @param overwrite: indicator as to whether or not the destination files
     should be overwritten. (Default: True)
    @raise dag.DagException: if the file copy fails
    """

    import os.path as OP
    from dag import GridProcess

    unique_names = {}

    if not isinstance(proc, GridProcess):
        return

    if not source_dir:
        from os import getcwd
        source_dir = getcwd()

    for infile in proc.input_files:
        if infile.dir:
            source_path = infile.full_path()
        else:
            source_path = OP.join(source_dir, infile.physical_name)
        unique_names[source_path] = unique_input_name(proc, infile)

    boinctools.stage_files(unique_names, source_dir, set_grp_perms, overwrite)


def dag_marker_filename(wuname):
    """
    Takes a workunit name and returns a string path within
    the dag_lists directory of the project. This is a fan out directory
    tree similar to downloads/

    @param wuname:String representation of the workunit name
    @type wuname: str
    @return: String filename for a dag marker (file not created).
    @rtype: str
    @raise dag.DagException: if dag_lists is not created
    """
    from os import path as OP
    import os
    import dag
    project_path = boinctools.project_path
    cwd = os.getcwd()
    if cwd != project_path:
        os.chdir(project_path)

    if not OP.isdir("dag_lists"):
        raise dag.DagException("Missing dag_lists in project directory: '%s'\n"
                               "Current working directory: %s"
                               % (project_path, os.getcwd()))
    os.chdir("dag_lists")

    marker_filename = (boinctools.dir_hier_path(wuname)
                       .replace("download/", "dag_lists/"))
    marker_dir = OP.dirname(marker_filename)
    if not OP.isdir(marker_dir):
        os.mkdir(marker_dir)
        os.chmod(marker_dir, 0777)

    if cwd != os.getcwd():
        os.chdir(cwd)

    return marker_filename


def make_dag_marker(wuname, dag_path):
    """
    Creates the dag marker file.

    @param wuname:String representation of the workunit name
    @type wuname: str
    @param dag_path: Path to the DAG
    @type dag_path: str

    @return: Filename of dag marker file
    @rtype: str
    @raise dag.DagException: If the dag_lists directory does not exist.
    """
    from os import getuid

    marker_filename = dag_marker_filename(wuname)
    dag_marker = open(marker_filename, "w")
    dag_marker.write("%d %s\n" % (getuid(), dag_path))
    dag_marker.close()

    return marker_filename


def marker_to_dagpath(filename):
    """
    Parses a DAG marker file and returns the path to the DAG

    @param filename: String representation of the marker's path
    @type filename: str
    @return: String path to DAG file
    @rtype: str
    """
    with open(filename, "r") as infile:
        line = infile.readline()
        dagpath = line.split(" ")[1]
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
    wuname = re.findall(r"^(.*)_\d*$", result_name)
    if not wuname:
            print("Malformed result name '%s'" % result_name)
            return None
    return wuname[0]


def result_to_dag(result_name):
    """
    Takes a BOINC result name and returns the corresponding DAG.

    @param result_name: String representation of the result name.
    @type result_name: str
    @return: DAG object for the result
    @rtype: dag.DAG
    @raise dag.utils.NoDagMarkerException: if the result does not have
     a dag marker file.
    @raise dag.utils.MissingDAGFile: If the file in the dag marker is missing.
    """
    import dag
    import dag.util as dag_utils
    import os.path as OP

    wuname = name_result2workunit(result_name)

    marker_path = dag_marker_filename(wuname)

    try:
        dagpath = marker_to_dagpath(marker_path)
    except IOError as ioe:
        raise dag_utils.NoDagMarkerException("Missing DAG marker.\n"
                                             "Error Message: %s\nFile: %s"
                                             % (ioe.strerror, marker_path))

    if not OP.isfile(dagpath):
        raise dag.MissingDAGFile("Missing dag file '%s' listed in marker '%s'"
                                 % (dagpath, marker_path))
    try:
        return dag.load(dagpath)
    except Exception as e:
        print("Error loading dag file '%s' listed in marker '%s'"
              % (dagpath, marker_path))
        raise e


def schedule_work(proc, dag_path):
    """
    Calls create_work. If create_work fails, an exception is raised.
    If create_work succeeds, a file is created that lists the dag file
     for the work unit.

    @param proc: Process to be scheduled
    @type proc: dag.Process
    @param dag_path: Absoulte path to DAG file
    @type dag_path: dag.DAG
    @raise Exception: If create_work fails or if the dag marker file
     cannot be created.
    """
    import os.path as OP

    if proc.workunit_name in forbidden_wu_names:
        raise Exception("The name '%s' is not allowed because it is a "
                        "reserved word in the DAG system."
                        % proc.workunit_name)

    wu_tmpl = OP.split(proc.workunit_template.physical_name)[1]
    res_tmpl = OP.split(proc.result_template.physical_name)[1]
    input_filenames = [unique_input_name(proc, infile)
                       for infile in proc.input_files]
    delay_bounds = None
    if hasattr(proc, "deadline"):
        delay_bounds = proc.deadline
    boinctools.schedule_work(proc.cmd, proc.get_unique_name(), wu_tmpl,
                             res_tmpl, input_filenames, delay_bounds)
    make_dag_marker(proc.get_unique_name(), dag_path)


def create_work(the_dag, dagfile, show_progress=False):
    """
    Creates a workunit by processing the dag and running stage_files
     and schedule_work.

    Sets the workunit information in the dag.Process objects

    @type the_dag: dag.DAG
    @param dagfile: Path to DAG file
    @type dagfile: str
    @raise Exception: If an input file does not exist and is not part
     of a parent process.
    """
    import os.path as OP
    import random
    import dag
    import stat

    progress_bar = None
    progress_bar_counter = 0

    if the_dag.processes is None:
        return

    if show_progress:
        from progressbar import ProgressBar, Percentage, Bar
        progress_bar = ProgressBar(widgets=[Percentage(), Bar()],
                                   maxval=len(the_dag.processes)).start()
        print("Submitting %d jobs" % len(the_dag.processes))

    for proc in the_dag.processes:
        if progress_bar:
            progress_bar_counter += 1
            progress_bar.update(progress_bar_counter)

        defer = False
        if proc.state not in [dag.States.CREATED, dag.States.STAGED]:
            continue

        # Create process name, if one does not already exist
        if not proc.workunit_name:
            proc.workunit_name = "%s-%09d" % (proc.cmd, int(random.random()
                                                            * 1000000000))
        # Setup workunit templates
        if isinstance(proc, dag.GridProcess):
            wu_tmpl = proc.workunit_template
            if wu_tmpl is None or not OP.isfile(wu_tmpl.physical_name):
                wu_tmpl = create_workunit_template(proc)
                OP.os.chmod(wu_tmpl.full_path(),
                            stat.S_IROTH | stat.S_IRUSR | stat.S_IWUSR
                            | stat.S_IRGRP | stat.S_IWGRP)
            res_tmpl = proc.result_template
            if res_tmpl is None or not OP.isfile(res_tmpl.physical_name):
                res_tmpl = create_result_template(proc)
                OP.os.chmod(res_tmpl.full_path(),
                            stat.S_IROTH | stat.S_IRUSR | stat.S_IWUSR
                            | stat.S_IRGRP | stat.S_IWGRP)
            proc.workunit_template = wu_tmpl  # update process objects
            proc.result_template = res_tmpl

        if the_dag.incomplete_prereqs(proc):
            print("Process '{0}' is missing input file(s) produced"
                  " by other processes".format(proc.workunit_name))
            print("Have the following files:")
            for i in the_dag.graph.keys():
                print("%s (%s)" % (i.physical_name, i.logical_name))
            raise dag.DagException("Missing File")

        if defer:
            continue

        # Job not deferred. Run it!
        if isinstance(proc, dag.GridProcess):
            stage_files(proc)
            proc.state = dag.States.STAGED
            the_dag.save()
            schedule_work(proc, dagfile)
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

    for fn in [proc.result_template, proc.workunit_template]:
        if not fn:
            continue
        if OP.isfile(fn.full_path()):
            os.unlink(fn.full_path())


def clean_workunit(root_dag, proc):
    """
    Removes and cleans a process from the DAG. This function
    will remove temporary files and connections to child processes.

    @param root_dag: DAG containing proc
    @type root_dag: dag.DAG
    @param proc: Process to be cleaned
    @type proc: dag.Process
    """
    import os.path as OP
    import os

    if proc is None:
        return

    proc.clean(root_dag)

    remove_templates(proc)

    if proc.get_unique_name():
        marker_filename = dag_marker_filename(proc.get_unique_name())
        if OP.isfile(marker_filename):
            os.unlink(marker_filename)


def start_children(proc, root_dag, dag_filename):
    """
    Checks to see if children may be run. If they can, they are started.
    Assumes the children's files are located in the same directory as the DAG,
    if the file paths are not absolute.

    @raise Exception: If the create_work call fails.
    """
    import dag
    import os.path as OP

    defer = False
    for child in proc.children:
        print("Can we start %s" % child)
        if child.state == dag.States.RUNNING:
            print("Already running")
            continue
        for infile in child.input_files:
            if infile in root_dag.graph:
                for parent_proc in root_dag.graph[infile]:
                    if parent_proc.state != dag.States.SUCCESS:
                        print("No. Not all Processes are finished.")
                        defer = True
                        break
            if defer:
                break
        if defer:
            defer = False
            continue

        stage_files(child, source_dir=OP.dirname(root_dag.filename),
                    overwrite=False)
        child.state = dag.States.STAGED
        root_dag.save()
        schedule_work(child, root_dag.filename)
        child.state = dag.States.RUNNING
        root_dag.save()


def cancel_workunits(proc_list):
    """
    Calls boinctools.cancel_workunits to cancel work units.
    """
    boinctools.cancel_workunits([proc.get_unique_name()
                                 for proc in proc_list])
