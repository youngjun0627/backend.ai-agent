'''
The kernel main program.
'''

import argparse
import importlib
from . import lang_map


def parse_args(args=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--debug', action='store_true', default=False)
    parser.add_argument('lang', type=str, choices=lang_map.keys())
    parser.add_argument('runtime_path', type=str, nargs='?', default=None)
    return parser.parse_args(args)


cmdargs = parse_args()
cls_name = lang_map[cmdargs.lang]
imp_path, cls_name = cls_name.rsplit('.', 1)
mod = importlib.import_module(imp_path)
cls = getattr(mod, cls_name)
runner = cls()
runner.run(cmdargs)
