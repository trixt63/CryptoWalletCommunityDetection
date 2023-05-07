# import contextlib
# import pathlib
# import sys
import os
import json


def read_json(file_path):
    with open(file_path, 'r') as f:
        return json.load(f)


def write_json(file_path, data):
    with open(file_path, 'w') as f:
        json.dump(data, f, indent=2)

# @contextlib.contextmanager
# def smart_open(filename=None, mode='w', binary=False, create_parent_dirs=True):
#     fh = get_file_handle(filename, mode, binary, create_parent_dirs)
#
#     try:
#         yield fh
#     finally:
#         fh.close()
#
#
# def get_file_handle(filename, mode='w', binary=False, create_parent_dirs=True):
#     if create_parent_dirs and filename is not None:
#         dirname = os.path.dirname(filename)
#         pathlib.Path(dirname).mkdir(parents=True, exist_ok=True)
#     full_mode = mode + ('b' if binary else '')
#     is_file = filename and filename != '-'
#     if is_file:
#         fh = open(filename, full_mode)
#     elif filename == '-':
#         fd = sys.stdout.fileno() if mode == 'w' else sys.stdin.fileno()
#         fh = os.fdopen(fd, full_mode)
#     else:
#         raise FileNotFoundError(filename)
#     return fh
#
#
# def init_last_synced_file(start_block, last_synced_block_file):
#     if os.path.isfile(last_synced_block_file):
#         raise ValueError(
#             '{} should not exist if --start-block option is specified. '
#             'Either remove the {} file or the --start-block option.'.format(last_synced_block_file, last_synced_block_file))
#     write_last_synced_file(last_synced_block_file, start_block)
#
#
# def write_last_synced_file(file, last_synced_block):
#     write_to_file(file, str(last_synced_block) + '\n')
#
#
# def read_last_synced_file(file):
#     with smart_open(file, 'r') as file_handle:
#         return int(file_handle.read())
#
#
# def write_to_file(file, content):
#     with smart_open(file, 'w') as file_handle:
#         file_handle.write(content)
#