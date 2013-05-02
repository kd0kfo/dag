import dag

# Defaults
segment_fmt = ".CLUSTER/segmented-%s"
temp_directory = ".CLUSTER"
default_fpops_est = 6E12
default_chunk_size = 15000 
default_deadline = 42 # Doesn't matter on cluster
default_memory_limit = 1e9

def unique_name(prefix):
    import random as R
    i = R.randint(1,10000000)
    return "%s-%d" % (prefix, i)

class TridentInstance:
    def __init__(self, mirna, dna, args):
        self.mirna = mirna
        self.dna = dna
        self.args = args

    def create_job_xml(self):
        import os.path as OP
        dna_basename = OP.basename(self.dna)
        mirna_basename = OP.basename(self.mirna)
        jobfilename = dna_basename + '_' + mirna_basename
        jobfilename = unique_name(jobfilename) + "-job.xml"
        jobfilename = OP.join(temp_directory,jobfilename)
        jobfilename = OP.join(OP.os.getcwd(),jobfilename)
        jobfile = dag.File(jobfilename,"job.xml",temporary_file=True)
        with open(jobfile.full_path(),"w") as file:
            file.write("""
<job_desc>
    <task>
        <application>trident</application>
        <stdout_filename>{0}_{2}.stdout</stdout_filename> 
        <stderr_filename>{0}_{2}.stderr</stderr_filename> 
        <command_line> {2} {3} {1} </command_line> 
    </task>
</job_desc>
""".format(dna_basename,self.args,mirna_basename, dna_basename))
        return jobfile

    def get_dag_node(self):
        import re
        import os.path as OP
        from Bio import SeqIO

        # Estimate compute time based on mirna count
        fpops_est = 0
        for i in SeqIO.parse(self.mirna,"fasta"):
            fpops_est += default_fpops_est
        
        input = [dag.File(self.mirna),dag.File(self.dna)]
        for infile in input:
            if "segmented" in infile.physical_name:
                infile.temp_file = True
                infile.dir = OP.abspath(infile.dir)
        output = []
        if self.args:
            match = re.findall("-out\s*(\S*)",self.args)
            if match:
                output.append(dag.File(match[0],max_nbytes=100e9))
        retval = dag.Process("trident",input,output,arguments = self.args,rsc_fpops_est = fpops_est,rsc_fpops_bound = fpops_est*5,rsc_memory_bound = self.rsc_memory_limit)
        retval.executable_name = self.executable_name
        if hasattr(self,"application_profile"):
            retval.application_profile = self.application_profile
        if hasattr(self,"project_name"):
            retval.project_name = self.project_name
        return retval
            


def parse(args,kmap = {}):
    import os.path as OP
    import getopt
    from trident import chromosome_chopper as chopper

    if len(args) < 2:
        print("trident processes require at least two filenames")
        return None

    header_map = {}
    rsc_memory_limit = default_memory_limit
    app_profile = None
    project_name = None
    if 'assembly' in kmap:
        header_map['assembly'] = " ".join(kmap['assembly'])
        print("Labeling Assembly: {0}".format(header_map['assembly']))
    if 'species' in kmap:
        header_map['species'] = kmap['species']
        if len(header_map['species']) > 2:
            print("WARNING: Species contains more than three words.")
        print("Labeling Species: {0}".format(header_map['species']))
    if 'memory_limit' in kmap:
        rsc_memory_limit = int(kmap['memory_limit'][0])
        print("Using memory limit of {0} bytes".format(rsc_memory_limit))
    if 'app' in kmap:
        app_profile = kmap['app'][0]
    if 'project_name' in kmap:
        project_name = kmap['project_name'][0]

    mirna = args[0]
    dna = args[1]

    print("Running trident with miRNA %s and DNA %s" % (mirna, dna))
    if len(args) > 2:
        print ("with flags %s" % " ".join(args[2:]))

    if not OP.isdir(temp_directory):
        OP.os.mkdir(temp_directory)

    num_files = 1
    if OP.isfile(dna):
        chunk_size = default_chunk_size
        if "-sc" in args:
            for i in range(0,len(args)):
                if args[i] == "-sc":
                    if i+1 < len(args):
                        if int(args[i+1]) < 140:
                            chunk_size /= 3
        num_files = chopper.chopper(dna,segment_fmt % OP.basename(dna),chunk_size,overwrite = False,header_map = header_map)

    # sanity check chopper. No need to chop the sequence into one segment file
    if num_files == 1:
        chopped_file = (segment_fmt + "-1") % OP.basename(dna)
        if OP.isfile(chopped_file):
            OP.os.unlink(chopped_file)
    else:
        print("Created %d dna files" % num_files)

    retval = []

    strargs = " ".join(args[2:])

    have_output = ("-out" in args[2:])
    
    if num_files == 1:
        # in case the dna did not need to be segmented
        proc = TridentInstance(mirna,dna,strargs)
        proc.rsc_memory_limit = rsc_memory_limit
        proc.executable_name = "trident.centos6.1"
        if app_profile:
            proc.application_profile = app_profile
        if not have_output:
            proc.args += " -out %s.out" % dna
        retval.append(proc.get_dag_node())
    else:
        for i in list(range(1,num_files+1)):
            #iterate through segments and setup work units
            # for the sake of running on a grid, an output file is required.
            segment_name = (segment_fmt + "-%s") % (OP.basename(dna),i)
            proc = TridentInstance(mirna,segment_name,strargs)
            proc.rsc_memory_limit = rsc_memory_limit
            proc.executable_name = "trident.centos6.1"
            if app_profile:
                proc.application_profile = app_profile
            if project_name:
                proc.project_name = project_name
            if not have_output:
                proc.args += " -out {0}.out".format(segment_name)
            proc.args = proc.args.replace(".out",".out-{0}".format(i))
            retval.append(proc.get_dag_node())
    
        
    return retval

    
