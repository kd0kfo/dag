"""
dag
===

@author: David Coss, PhD
@date: November 7, 2012
@license: GPL version 3 (see COPYING or http://www.gnu.org/licenses/gpl.html for details)

This python module provides interface between BOINC C API and Python user code.
"""
DEFAULT_DAGFILE_NAME = "jobs.dag"

class DagException(Exception):
    """
    Generic Exception for DAG related problems.
    """
    pass

class NoSuchProcess(DagException):
    pass

class MissingDAGFile(DagException):
    pass

def enum(*sequential,**vals):
    the_enums = dict(zip(sequential, range(len(sequential))), **vals)
    return type('Enumeration',(),the_enums)

Engine = enum('BOINC','LSF','NUM_ENGINES')
States = enum('CREATED','STAGED','RUNNING','SUCCESS','FAIL','NUM_STATES')
def enum2string(enum,val):
    """
    Looks up the given integer and provides the string representation within the enum. If there is not name for the provided integer, None is returned.

    @param enum: Enum to be searched
    @ptype enum: enum
    @param val: Integer to be found
    @ptype val: int
    @return: Name of enum value
    @rtype: str
    """
    for i in dir(enum):
        if getattr(enum,i) == val:
            return i
    return None

def string2enum(enum,string):
    """
    Translates an string name for a value in an enum to the integer value of the enum.

    Returns None if the string is not a valid value.

    @param enum: enum to search
    @ptype enum: enum
    @param string: String value of enum int
    @ptype string: str
    @rtype: int
    @return: Enum value
    """
    if not string in enum.__dict__:
        return None
    return int(enum.__dict__[string])

def strstate(state):
    """
    Translates a state integer value to a String name
    
    @param: State enum value
    @ptype state: int
    @return: Name of state
    @rtype: str
    """
    if state == None:
        return 'None'
    string = enum2string(States,state)               
    if string:
        return string
    return 'UNKNOWN'

def intstate(state):
    """
    Translates an string name for a state to the integer value of the enum.

    Returns None if the state string is not a valid state.

    @param state: Name of state
    @ptype state: str
    @rtype: int
    @return: State enum value
    """
    if state == "NUM_STATES":
        return None
    return string2enum(States,state)

class File:
    """
    File description abstraction

    Attributes:
    physical_name -- String base name of the actual file on disk
    logical_name -- String abstract alias of the file provided for the user
    temp_file -- Boolean indicating whether or not the file is temporary
    max_nbytes -- Byte limit of the files on the BOINC client
    dir -- String Path of the directory in which the file is located on disk. Null string indicates the file is in the same directory as the dag file.
    """
    def __init__(self,physical_name,logical_name=None,temporary_file=False,max_nbytes=15000000):
        import os.path as OP
        self.physical_name = OP.basename(physical_name)
        if logical_name == None:
            self.logical_name = self.physical_name
        else:
            self.logical_name = logical_name
        self.temp_file = temporary_file
        self.max_nbytes = max_nbytes
        self.dir = OP.dirname(physical_name)

    def __eq__(self, other):
        if isinstance(other,self.__class__):
            if self.logical_name != other.logical_name:
                return False
            else:
                return True
        else:
            return False

    def __ne__(self,other):
        return not self.__eq__(other)

    def __hash__(self):
        return hash(self.logical_name)
        
    def unlink(self):
        """
        Removes the file based on the physical_name.

        Returns: Boolean indicating whether or not the file
        could be removed. Note: Exceptions are not caught.
        """
        import os.path as OP
        if OP.isfile(self.physical_name):
            OP.os.unlink(self.physical_name)
            return True
        return False

    def full_path(self):
        """
        Returns a full path of the File, if the directory is provided. Otherwise the file name is returned.

        Returns: Full path to file        
        """
        import os.path
        return os.path.join(self.dir,self.physical_name)
        

class Process(object):
    def __init__(self):
        import uuid
        self.input_files = []
        self.output_files = []
        self.state = States.CREATED
        self.children = []
        self.temp_files = []
        self.uuid = uuid.uuid4()
        
    
    def __str__(self):
        raise DagException("String function must be overloaded classes that extend dag.Process")
    
    def start(self):
        raise DagException("start function must be overloaded classes that extend dag.Process")
    
    def get_unique_name(self):
        """
        Returns the UUID of the process as a String.
        
        @return: UUID
        @type: str
        """
        return str(self.uuid)

class InternalProcess(Process):
    def __init__(self,command):
        super(InternalProcess,self).__init__()
        self.cmd = command
        
    def __str__(self):
        return "Command: {0}".format(self.cmd)
    
    def start(self):
        retval = eval(compile(self.cmd,self.workunit_name,'exec'))
        return retval
    
class GridProcess(Process):
    """
    Process is an abstraction of work units.
    """
    def __init__(self,cmd,input_files, output_files,arguments, rsc_fpops_est = 10**10, rsc_fpops_bound = 10**11, rsc_memory_bound = 536870912,deadline = None):
        self.cmd = cmd
        self.input_files = input_files
        self.output_files = output_files
        self.workunit_name = ""
        self.workunit_template = None
        self.result_template = None
        self.temp_files = [] # Temporary files that will be removed when the process is removed from the DAG
        self.children = [] # Processes that depend on self
        self.args = arguments
        self.state = States.CREATED
        self.rsc_fpops_est = rsc_fpops_est
        self.rsc_fpops_bound = rsc_fpops_bound
        self.rsc_memory_bound = rsc_memory_bound
        if deadline:
            self.deadline = int(deadline)
        else:
            self.deadline = None
        
    def __str__(self):
        retval = "{0}\n".format(self.cmd)
        file_list = []
        for i in self.input_files:
            file_list.append(i.physical_name)
            if i.logical_name and i.logical_name != i.physical_name:
                file_list[-1] += " (%s)" % i.logical_name
        retval += "\nState: %s(%d)" % (strstate(self.state),self.state)
        if self.rsc_memory_bound:
            retval += "\nMemory Limit: {0}".format(self.rsc_memory_bound)
        if self.rsc_fpops_bound:
            retval += "\nFloating Point Op Limit: {0}".format(self.rsc_fpops_bound)
        retval += "State: %s(%d)\n" % (strstate(self.state),self.state)
        if hasattr(self,"deadline") and self.deadline:
            retval += "Deadline (hours): {0}\n".format(self.deadline/3600) # Convert to hours
        retval += "Input: "
        file_list = []
        for i in self.output_files:
            file_list.append(i.physical_name)
            if i.logical_name and i.logical_name != i.physical_name:
                file_list[-1] += " (%s)" % i.logical_name
        retval += "\n"
        retval += "Output: " + ", ".join(file_list) + "\n"
        if self.workunit_name:
            retval += "%s: %s\n" % ("Workunit Name",self.workunit_name)
        if self.workunit_template and self.workunit_template.physical_name:
            retval += "%s: %s\n" % ("Workunit Template", self.workunit_template.physical_name)
        if self.result_template and self.result_template.physical_name:
            retval += "%s: %s\n" % ("Result Template", self.result_template.physical_name)
        retval += "args: %s\n" % self.args
        retval += "Children:\n"
        for i in self.children:
            retval += "%s(%s) " % (i.workunit_name, i.cmd)
        return retval

class Graph(dict):
    """
Graph is a dict that maps process output files names to the processes that produce them.
    """
    def add_process(self,proc):
        for filename in proc.output_files:
            if filename not in self.keys():
                self[filename] = [proc]
            else:
                self[filename].append(proc)
                
    def debug(self):
        retval = ""
        for i in self.keys():
            retval += "File %s produced by:\n" % i
            for j in self[i]:
                retval += str(j)
            retval += "\n"
        return retval

class DAG:
    """
DAG is a directed acyclic graph. It stores a list of processes (nodes in the graph). The Graph class connects nodes by their input/output files.
"""
    def __init__(self,engine = Engine.BOINC):
        self.processes = []
        self.graph = Graph()
        self.filename = ""
        self.engine = engine

    def add_process(self,proc):
        self.processes.append(proc)
        self.graph.add_process(proc)
        return proc

    def get_process(self,wuname):
        for i in self.processes:
            if i.workunit_name == wuname:
                return i
        return None

    def get_processes_by_state(self,state):
        return [proc for proc in self.processes if proc.state == state]

    def is_empty(self):
        return self.processes == [] and self.graph == {}

    def save(self,filename=None,backup_first = True):
        """
        Serializes the DAG

        Argument: String filename (optional)
        Returns: Filename of DAG
        """
        import cPickle
        import tempfile
        import os
        import os.path as OP
        import lockfile

        file = None
        # Check filename.
        # * If filename is None *and* self.filename is None,
        #   use a temporary file
        # * If filename is not None and if filename is not equal
        #   to self.filename, we are changing self.filename.
        if not filename:
            if not self.filename:
                file = tempfile.NamedTemporaryFile(mode='wb',delete=False,dir=os.getcwd())
        elif (self.filename != filename):
            self.filename = filename
                
        lock = lockfile.FileLock(self.filename)

        try:
            lock.acquire(timeout=10)
        except lockfile.LockTimeout:
            raise DagException("Error saving DAG. DAG file %s is locked." % self.filename)

        backup_filename = self.filename + ".bak"
        if file == None:
            try:
                # Backup in case of problems
                if backup_first and OP.isfile(self.filename) and not OP.isfile(backup_filename):
                    import shutil
                    shutil.copyfile(self.filename,backup_filename)
                file = open(self.filename,"wb")
            except Exception as e:
                raise DagException("Saving DAG failed.\nCWD: %s\nMessage: %s" % (os.getcwd(),e.message))

        
        self.filename = OP.abspath(file.name) # Update filename

        # Dump the pickle
        cPickle.dump(self,file)
        retval = file.name
        file.close()
        if backup_first and OP.isfile(backup_filename):
            try:
                os.remove(backup_filename)
            except Exception as e:
                raise DagException("Could not remove backup DAG file '%s'.\nReason: %s" % (backup_filename, e.message))
        lock.release()
        
        return retval

    def incomplete_prereqs(self,proc):
        """
        Checks a process instance within a DAG to see if all of its prerequisites are met.

        Returns: Boolean. True if the process is ready to start
        """

        from os import path as OP
        uncompleted_prereqs = []
        for input in proc.input_files:
            if input in self.graph:
                for parent in self.graph[input]:
                    if parent.state != States.SUCCESS:
                        uncompleted_prereqs.append(parent)
            if not OP.isfile(input.full_path()):
                uncompleted_prereqs.append(input)
        return uncompleted_prereqs

    def __str__(self):
        import os.path as OP
        from sys import stderr
        if self.is_empty():
            return "Empty"
        retval = ""
        for proc in self.processes:
            retval += "------------\n"
            retval += str(proc)+"\n"
            for f in proc.input_files:
                    if f in self.graph.keys():
                        retval += "Depends on: %s\n" % ",".join([i.cmd for i in self.graph[f]])

            proc_prereqs = self.incomplete_prereqs(proc)
            if proc_prereqs:
                retval += "Unfinished Dependencies\n"
                for i in proc_prereqs:
                    if isinstance(i,File):
                        retval += "File: %s\n" % i.logical_name
                    elif isinstance(i,GridProcess):
                        retval += "Process: %s\n" % i.workunit_name
                    elif isinstance(i,InternalProcess):
                        retval += "Python Code\n"
                    else:
                        retval += "%s\n" % i
            retval += "\n\n"
        return retval
    

def load(pickle_filename):
    """
    Loads a DAG object saved in a file.

    Argument: String Filename
    Returns: DAG object
    """
    
    import cPickle
    import types
    import lockfile

    lock = lockfile.FileLock(pickle_filename)
    try:
        lock.acquire(timeout=10)
    except lockfile.LockTimeout:
        raise DagException("Error saving DAG. DAG file %s is locked." % pickle_filename)


    file = open(pickle_filename,"rb")
    retval = cPickle.load(file)
    file.close()
    lock.release()
    return retval

