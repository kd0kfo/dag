"""
dag.shell
=======


@author: David Coss, PhD
@date: May 9, 2013
@license: GPL version 3 (see COPYING or
 http://www.gnu.org/licenses/gpl.html for details)

Interface module to POSIX shells. This allows processes in a DAG to interpret
 and use LSF data.
"""

from dag import Process


class ShellProcess(Process):
    def __init__(self, cmd, args):
        super(ShellProcess, self).__init__()
        self.cmd = cmd
        self.args = args

    def __str__(self):
        from dag import enum2string, States
        strval = "Command: {0} {1}\n".format(self.cmd, " ".join(self.args))
        strval += "Status: {0}".format(enum2string(States, self.state))

        return strval

    def start(self):
        import subprocess

        print("Starting {0}".format(self.cmd))
        subprocess.call([self.cmd] + self.args)
        print("{0} Finished".format(self.cmd))


def parse_shell(cmd, args, header_map):
    return [ShellProcess(cmd, args)]


def create_work(root_dag, dag_path):
    from dag import States

    print("SHELL: Starting {0} processes".format(len(root_dag.processes)))
    for proc in root_dag.processes:
        if proc.state not in [States.CREATED, States.SUCCESS]:
            continue

        proc.start()
        proc.state = States.SUCCESS
