#!/usr/bin/env python
"""
dag.lsf
=========


@author: David Coss, PhD
@date: November 7, 2012
@license: GPL version 3 (see COPYING or http://www.gnu.org/licenses/gpl.html for details)

Interface module to LSF. This allows processes in a DAG to interpret and use LSF data.
"""


def create_work(the_dag,dagfile, show_progress = False):
    """
    Creates a workunit by processing the dag and running stage_files and schedule_work.

    Sets the workunit information in the dag.Process objects

    Arguments: dag and dagfile path to dag file
    Returns: no value
    Throws Exception if an input file does not exist and is not part of a parent process.
    """
    
    import dag
    raise dag.DagException("Need to add create work function!!")
