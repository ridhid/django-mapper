import os
import sys

rel = lambda x: os.path.join(os.path.dirname(__file__), x)


def load_source_abs_path(file_path):
    abs_path = rel(file_path)
    if os.path.exists(abs_path):
        return abs_path
    raise ValueError('file not exists')


def is_test_env():
    return 'test' in sys.argv