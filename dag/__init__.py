"""
dag
===

@author: David Coss, PhD
@date: May 9, 2013
@license: GPL version 3 (see COPYING or http://www.gnu.org/licenses/gpl.html for details)

Creates and manages a Directed Acyclic Graph of embarrassingly parallel work units. 
"""
DEFAULT_DAGFILE_NAME = "jobs.dag"
DEFAULT_DAG_CONFIG_FILE = ".dagrc"

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
    @type enum: enum
    @param val: Integer to be found
    @type val: int
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
    @type enum: enum
    @param string: String value of enum int
    @type string: str
    @rtype: int
    @return: Enum value
    """
    if not string in enum.__dict__:
        return None
    return int(enum.__dict__[string])

def strstate(state):
    """
    Translates a state integer value to a String name
    
    @param state: State enum value
    @type state: int
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
    @type state: str
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
    @ivar physical_name: Base name of the actual file on disk
    @type physical_name: str
    @ivar logical_name: Abstract alias of the file provided for the user
    @type logical_name: str
    @ivar temp_file: Indicates whether or not the file is temporary
    @type temp_file: bool
    @ivar max_nbytes: Byte limit of the files on the BOINC client
    @type max_nbytes: int
    @ivar dir: Path of the directory in which the file is located on disk. Null string indicates the file is in the same directory as the dag file.
    @type dir: str
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
        
    def full_path(self):
        """
        Returns a full path of the File, if the directory is provided. Otherwise the file name is returned.

        Returns: Full path to file        
        """
        import os.path
        return os.path.join(self.dir,self.physical_name)
    
    def unlink(self):
        """
        Removes the file based on the physical_name.

        Returns: Boolean indicating whether or not the file
        could be removed. Note: Exceptions are not caught.
        """
        import os.path as OP
        full_path = self.full_path()
        if OP.isfile(full_path):
            OP.os.unlink(full_path)
            return True
        return False

    
        

class Process(object):
    """
    Base class for process abstraction in work unit graph.
    
    @ivar input_files: List of input files
    @type input_files: list
    @ivar output_files: List of output files
    @type output_files: list
    @ivar state: Current state of process as listed in dag.States enum
    @type state: dag.States
    @ivar temp_files: Optional list of temporary files
    @type temp_files: list
    @ivar uuid: Unique ID of process
    @type uuid: uuid.UUID
    """
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
        @rtype: str
        """
        return str(self.uuid)
    
    def clean_temp_files(self):
        """
        Removes files listed as temporary.
        
        Calls File.unlink()
        """
        for f in self.temp_files + self.input_files + self.output_files:
            if f.temp_file:
                f.unlink() # safe to call if file doesn't exist.
                
    def clean(self,root_dag):
        """
        Cleans a Process's temporary files and removes it from a DAG (if provided).
        
        @param root_dag: Optional DAG which contains the Process.
        @type root_dag: dag.DAG
        """
        #Remove connection(s) to child node(s)
        if root_dag:
            for output in self.output_files:
                if not output in root_dag.graph:
                    continue
                if self in root_dag.graph[output]:
                    root_dag.graph[output].remove(self)
                if len(root_dag.graph[output]) == 0:
                    root_dag.graph.pop(output)
                
        self.clean_temp_files()

class InternalProcess(Process):
    """
    Process to be perform by the python interpreter
    
    @ivar cmd: Python code to be run
    @type cmd: str
    """
    def __init__(self,command):
        super(InternalProcess,self).__init__()
        self.cmd = command
        
    def __str__(self):
        return "Command: {0}".format(self.cmd)
    
    def start(self):
        """
        Runs the process.
        
        @return: Value of the evaluated process
        @rtype: object
        """
        retval = eval(compile(self.cmd,self.workunit_name,'exec'))
        return retval
        
class GridProcess(Process):
    """
    Process is an abstraction of work units.
    
    @ivar cmd: String command to be run.
    @type cmd: str
    @ivar input_files: Input files
    @type input_files: list
    @ivar output_files: Output files
    @type output_files: list
    @ivar workunit_name: User determined name of process.
    @type workunit_name: str
    @ivar workunit_template: File name of template for process
    @type workunit_template: str
    @ivar result_template: File name of template for process
    @type result_template: str
    @ivar temp_files: List of Temporary Files
    @type temp_files: list
    @ivar children: List of Child processes
    @type children: list
    @ivar args: Command line arguments
    @type args: str
    @ivar state: State of process
    @type state: dag.States
    @ivar rsc_fpops_est: Estimated number of floating point operations
    @type rsc_fpops_est: float
    @ivar rsc_fpops_bound: Limit of number of floating point operations
    @type rsc_fpops_bound: float
    @ivar rsc_memory_bound: Memory Limit
    @type rsc_memory_bound: int
    @ivar deadline: limit of CPU time in seconds
    @type deadline: int
    """
    def __init__(self,cmd,input_files, output_files,arguments, rsc_fpops_est = 10**10, rsc_fpops_bound = 10**11, rsc_memory_bound = 536870912,deadline = None):
        super(GridProcess,self).__init__()
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
            retval += "\nFloating Point Op Limit: {0}\n".format(self.rsc_fpops_bound)
        if hasattr(self,"deadline") and self.deadline:
            retval += "Deadline (hours): {0}\n".format(self.deadline/3600) # Convert to hours
        retval += "Input: {0}\n".format(", ".join(make_file_list(self.input_files)))
        retval += "Output:{0}\n ".format(", ".join(make_file_list(self.output_files)))
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
        """
        Adds a process to the graph. Links processes by input and output files.
        
        @param proc: Process to be added
        @type proc: dag.Process
        """
        for filename in proc.output_files:
            if filename not in self.keys():
                self[filename] = [proc]
            else:
                self[filename].append(proc)
                
    def debug(self):
        """
        Creates a string list of files and processes that produce them.
        
        @return: Debug information about file producers
        @rtype: str
        """
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

    @ivar processes: List of processes contained within the graph.
    @type processes: list
    @ivar graph: Graph of processes
    @type graph: dag.Graph
    @ivar filename: File name last used to save or load the graph.
    @type filename: str
    @ivar engine: Method used to run processes.
    @type engine: dag.Engine
    """
    def __init__(self,engine = Engine.BOINC):
        self.processes = []
        self.graph = Graph()
        self.filename = ""
        self.engine = engine

    def add_process(self,proc):
        """
        Adds a process to the DAG.
        
        @param proc: Process to be added
        @type proc: dag.Process
        @return: Process added to graph
        @rtype: dag.Process
        """
        self.processes.append(proc)
        self.graph.add_process(proc)
        return proc

    def get_process(self,wuname):
        """
        Finds a process based on its name. If the workunit name is not found, UUID is tried instead.
        
        @param wuname: Process name
        @type wuname: str
        @return: Process
        @rtype: dag.Process
        """
        for i in self.processes:
            if i.workunit_name == wuname:
                return i
        for i in self.processes:
            if str(i.uuid) == wuname:
                return i
        return None

    def get_processes_by_state(self,state):
        """
        Returns a list of processes based on their state.
        
        @param state: State to be found
        @type state: dag.States
        @see: dag.States
        """
        return [proc for proc in self.processes if proc.state == state]

    def is_empty(self):
        """
        Determines whether or not a DAG is empty.
        
        @return: Indication of emptyness 
        @rtype: bool
        """
        return self.processes == [] and self.graph == {}

    def save(self,filename=None,backup_first = True):
        """
        Serializes the DAG

        @param filename: File name to be save
        @type filename: str
        @param backup_first: Whether or not the DAG file should be backuped before overwritting. Default: True
        @type backup_first: bool
        @return: File name of DAG
        @rtype: str
        """
        import cPickle
        import tempfile
        import os
        import os.path as OP
        import lockfile

        outfile = None
        # Check filename.
        # * If filename is None *and* self.filename is None,
        #   use a temporary file
        # * If filename is not None and if filename is not equal
        #   to self.filename, we are changing self.filename.
        if not filename:
            if not self.filename:
                outfile = tempfile.NamedTemporaryFile(mode='wb',delete=False,dir=os.getcwd())
        elif (self.filename != filename):
            self.filename = filename
                
        lock = lockfile.FileLock(self.filename)

        try:
            lock.acquire(timeout=10)
        except lockfile.LockTimeout:
            raise DagException("Error saving DAG. DAG file %s is locked." % self.filename)

        backup_filename = self.filename + ".bak"
        if outfile == None:
            try:
                # Backup in case of problems
                if backup_first and OP.isfile(self.filename) and not OP.isfile(backup_filename):
                    import shutil
                    shutil.copyfile(self.filename,backup_filename)
                outfile = open(self.filename,"wb")
            except Exception as e:
                raise DagException("Saving DAG failed.\nCWD: %s\nMessage: %s" % (os.getcwd(),e.message))

        
        self.filename = OP.abspath(outfile.name) # Update filename

        # Dump the pickle
        cPickle.dump(self,outfile)
        retval = outfile.name
        outfile.close()
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

        @return: Indication of there being missing prerequisite processes or files
        @rtype: bool
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
    

def load(pickle_filename = None):
    """
    Loads a DAG object saved in a file.

    @param pickle_filename: File name
    @type pickle_filename: str
    @return: DAG object
    @rtype: dag.DAG
    """
    
    import cPickle
    import types
    import lockfile

    if not pickle_filename:
        pickle_filename = DEFAULT_DAGFILE_NAME

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

    
def make_file_list(files):
    """
    Converts a list of dag.Files and returns a list of string filenames
    
    @param files: List of dag.Files
    @type files: list
    @return: List of string filenames
    @rtype: list
    """
    file_list = []
    for i in files:
        file_list.append(i.physical_name)
        if i.logical_name and i.logical_name != i.physical_name:
            file_list[-1] += " (%s)" % i.logical_name
    return file_list

