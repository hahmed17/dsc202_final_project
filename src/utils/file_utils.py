import os

def get_file_contents(file):
    with open(file) as f:
        return f.read()