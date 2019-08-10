#!/usr/bin/python3
from pathlib import Path
import os
import torch
import sys
import json
import os.path
import time
import math
import random
import platform
import subprocess

import beanstalkc as BSC


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(
        description='Inject a job into the dnn work queue',
        epilog='Example: python dnninject.py -w=./ -c=config.json '
               '-r=runs -m=main.py')
    parser.add_argument('-w', '--workdir', type=str, default='./',
        help='Set the working path. All other specified relative paths '
             'will be set relative to this one')
    parser.add_argument('-c', '--jobconfig', type=str, required=True,
        help='Path to .json file containing information needed to run the job '
             '(like a config file).')
    parser.add_argument('-r', '--rundir', type=str, required=True,
        help='Path to directory where the job output will be saved.')
    parser.add_argument('-m', '--module', type=str, default='main.py',
        help='Path to python module containing the main function.')
    parser.add_argument('--config', type=str, default='queue_config.json',
        help='Path to queue config file.')
    parser.add_argument('--rid', type=int, default=0, 
        help='Run id, used for resuming runs that have finished.')
    parser.add_argument('-a', '--args', type=str, default='',
        help='A string containing any additional arguments to be passed to '
             'the main module. If there are multiple arguments, make sure to '
             'wrap everything in quotes. Example: "-e 100 --batch_size=32".')
    args = parser.parse_args()

    hostname = platform.node()
    CONFIG = json.load(open(args.config, 'r'))
    config = CONFIG['universal']
    if hostname in CONFIG['machine_specific']:
        local = CONFIG['machine_specific'][hostname]
        for k in local.keys():
            config[k] = local[k]

    bs_conn = BSC.Connection( host=config['beanstalk_host'],
                              port=config['beanstalk_port'] ) 
    bs_conn.use('jobs_incoming')

    def fix_path(path_str, workdir):
        path = Path(path_str)
        path = path if path.is_absolute() else workdir / path
        return str(path)

    workdir = Path(args.workdir).resolve()
    job_config = fix_path(args.jobconfig, workdir)
    rundir = fix_path(args.rundir, workdir)
    main_file = fix_path(args.module, workdir)

    msg = dict(
        job_config=job_config,
        main_file=main_file,
        rundir=rundir,
        runid=args.rid,
        other_params=args.args,
    )

    bs_conn.put(json.dumps(msg))
    bs_conn.close()

