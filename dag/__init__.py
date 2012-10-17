class DagException(Exception):
    """
    Generic Exception for DAG related problems.
    """
    pass

class NoSuchProcess(DagException):
    pass

def enum(*sequential,**vals):
    the_enums = dict(zip(sequential, range(len(sequential))), **vals)
    return type('Enumeration',(),the_enums)

States = enum('CREATED','STAGED','RUNNING','SUCCESS','FAIL')
def strstate(state):
    if state == None:
        return 'None'
    for i in dir(States):
        if getattr(States,i) == state:
            return i
    return 'UNKNOWN'

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
        

class Process:
    """
    Process is an abstraction of work units.
    """
    def __init__(self,cmd,input_files, output_files,arguments, rsc_fpops_est = 10**10, rsc_fpops_bound = 10**11, rsc_memory_bound = 536870912):
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
        
    def __str__(self):
        retval = str(self.cmd)
        file_list = []
        for i in self.input_files:
            file_list.append(i.physical_name)
            if i.logical_name and i.logical_name != i.physical_name:
                file_list[-1] += " (%s)" % i.logical_name
        retval += "\nState: %s(%d)" % (strstate(self.state),self.state)
        retval += "\nInput: " + ", ".join(file_list)
        file_list = []
        for i in self.output_files:
            file_list.append(i.physical_name)
            if i.logical_name and i.logical_name != i.physical_name:
                file_list[-1] += " (%s)" % i.logical_name
        retval += "\nOutput: " + ", ".join(file_list)
        if self.workunit_name:
            retval += "\n%s: %s" % ("Workunit Name",self.workunit_name)
        if self.workunit_template and self.workunit_template.physical_name:
            retval += "\n%s: %s" % ("Workunit Template", self.workunit_template.physical_name)
        if self.result_template and self.result_template.physical_name:
            retval += "\n%s: %s" % ("Result Template", self.result_template.physical_name)
        retval += "\nargs: %s\n" % self.args
        retval += "\nChildren:\n"
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
    def __init__(self):
        self.processes = []
        self.graph = Graph()
        self.filename = ""

    def add_process(self,proc):
        self.processes.append(proc)
        self.graph.add_process(proc)
        return proc

    def get_process(self,wuname):
        for i in self.processes:
            if i.workunit_name == wuname:
                return i
        return None

    def is_empty(self):
        return self.processes == [] and self.graph == {}

    def save(self,filename=None):
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
            raise DagException("Error saving DAG. DAG file %s is locked.")

        if file == None:
            try:
                file = open(self.filename,"wb")
            except Exception as e:
                raise DagException("Saving DAG failed.\nCWD: %s" % os.getcwd())

        self.filename = OP.abspath(file.name) # Update filename

        # Dump the pickle
        cPickle.dump(self,file)
        retval = file.name
        file.close()
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
                    elif isinstance(i,Process):
                        retval += "Process: %s\n" % i.workunit_name
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
        raise DagException("Error saving DAG. DAG file %s is locked.")


    file = open(pickle_filename,"rb")
    retval = cPickle.load(file)
    file.close()
    lock.release()
    return retval


    
