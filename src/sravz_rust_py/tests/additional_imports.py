
import os
import sys

# Get the parent directory of the current file
current_file_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(f"{current_file_dir}/../../")
src_dir = os.path.dirname(f"{current_file_dir}/../")

# Add the parent directory to the Python path
sys.path.append(parent_dir)
sys.path.append(src_dir)
print(sys.path)
