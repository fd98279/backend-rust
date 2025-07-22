
import os
import sys
import unittest

# Get the parent directory of the current file
current_file_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(f"{current_file_dir}/../../")
src_dir = os.path.dirname(f"{current_file_dir}/../../sravz_rust_py/")

# Add the parent directory to the Python path
sys.path.append(parent_dir)
sys.path.append(src_dir)
