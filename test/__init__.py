def test_dag_objects():

    def end(retval):
        os.chdir(orig_dir)
        return retval

    print("Hello, test world!")
    import dag
    import os
    orig_dir = os.getcwd()
    os.chdir("test")

    # Create DAG
    d = dag.DAG()
    p = dag.Process()
    p.cmd = "test"
    p.input_files = []
    p.output_files = []
    p.args = "-arg1 -arg2 FILE"
    p.workunit_name = "test1"
    p2 = p
    p2.workunit_name = "test2"
    for i in [p, p2]:
        d.add_process(i)

    # Save DAG
    dag_filename = d.save()

    # Load DAG
    d2 = dag.load(dag_filename)

    # Do processes in DAGs match?
    procs = d2.processes
    if len(procs) != 2:
        print("Saving DAG caused process count to change.")
        print("Expected %d but have %d" % (2, len(d2.processes)))
        return end(False)

    for proc in procs:
        for attr in ["workunit_name", "cmd", "args"]:
            matches = False
            for orig_proc in [p, p2]:
                if getattr(proc, attr) == getattr(orig_proc, attr):
                    matches = True
                    break
            if not matches:
                print("Saving DAG caused process to change")
                print("Original 1: %s" % p)
                print("Original 2: %s" % p2)
                print("Loaded: %s" % proc)
                return end(False)

    os.unlink(dag_filename)

    return end(True)


def test_gsub():
    from dag import gsub, DEFAULT_DAGFILE_NAME, Engine
    from os.path import isfile

    expected_command = "print(\"Not a comment\")"

    if isfile(DEFAULT_DAGFILE_NAME):
        from os import unlink
        unlink(DEFAULT_DAGFILE_NAME)
    test_dag = gsub.gsub("test/internal.sub", init_filename="test/dagrc",
                         engine=Engine.SHELL)
    processes = test_dag.processes
    if len(processes) != 1:
        print("Invalid number of processes produced by submission file")
        print("Expected 1 but have {0}".format(len(processes)))
        return False

    process = processes[0]
    if process.cmd != expected_command:
        print("Command parsed from file is invalid.")
        print("Expected: {0}".format(expected_command))
        print("Have: {0}".format(process.cmd))
        return False

    return True


def test_shell_processes():
    from dag import gsub, DEFAULT_DAGFILE_NAME, States, Engine
    from os.path import isfile

    if isfile(DEFAULT_DAGFILE_NAME):
        from os import unlink
        unlink(DEFAULT_DAGFILE_NAME)

    test_dag = gsub.gsub("test/shell.sub", init_filename="test/dagrc",
                         engine=Engine.SHELL)

    for proc in test_dag.processes:
        if proc.state != States.SUCCESS and proc.workunit_name != "cat-2":
            return False

    return True


def test_progress_bar():
    from progressbar import ProgressBar, Percentage, Bar
    from time import sleep
    import os

    progress_bar = ProgressBar(widgets=[Percentage(), Bar()],
                               maxval=10).start()
    for i in range(1, 5):
        progress_bar.update(i)
        sleep(1)
    print("")
    return True
