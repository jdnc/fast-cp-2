import os
import sys
import getopt

"""
usage : python workload.py <root> <num_files> <num_levels> <file_size>
"""

def generate_random(file_name, num_kbytes):
    with open(file_name, 'wb') as f:
        for i in range(num_kbytes):
            rand_kb = os.urandom(1024)
            f.write(rand_kb)

def create_tree(root, num_files, num_levels, file_size):    
    for i in range(num_files):
        fname = 'file' + str(i)
        path = os.path.join(root, fname)
        generate_random(path, file_size)
    if (num_levels > 0) :
        left_path = os.path.join(root, 'left')
        os.mkdir(left_path)
        right_path = os.path.join(root, 'right')
        os.mkdir(right_path)
        create_tree(left_path, num_files, num_levels - 1, file_size)
        create_tree(right_path, num_files, num_levels - 1, file_size)

def main():
    root = sys.argv[1]
    num_files = int(sys.argv[2])
    num_levels = int(sys.argv[3])
    file_size = int(sys.argv[4])
    create_tree(root, num_files, num_levels, file_size)


if __name__ == '__main__':
    main()
