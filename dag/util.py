"""
dag.utils
=========

@author: David Coss, PhD
@date: May 9, 2013
@license: GPL version 3 (see COPYING or http://www.gnu.org/licenses/gpl.html for details)

"""
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
    
