"""
parquet_writer.py

A python script that contains simple parquet file reader and writer methods
"""
from deephaven.parquet import read, write

import os

def write_tables(tables=None, table=None, path=None):
    """
    Writes a list of tables to the given path

    Parameters:
        tables (list<Table>): A list of Deephaven tables to write
        table (Table): A single Deephaven table to write
        path (str): The path to write tables to. Defaults to "/data/"
    Returns:
        None
    """
    if tables is None:
        tables = []
    if not (table is None):
        tables.append(table)

    if path is None:
        path = "/data/"
    for i in range(len(tables)):
        write(tables[i], f"{path}{i}.parquet")

def read_tables(path=None):
    """
    Reads all of the parquet files in the given path

    Parameters:
        path (str): The path to read tables from. Should end with /. Defaults to "/data/"
    Returns:
        list<tables>: A list of tables read
    """
    tables = []
    if path is None:
        path = "/data/"
    for file_path in os.popen(f"find {path} -type f -name \"*.parquet\"").read().split("\n"):
        if len(file_path) > 0:
            tables.append(read(file_path))
    return tables
