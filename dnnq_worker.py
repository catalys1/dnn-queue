#!/usr/bin/python3
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
from pathlib import Path
import importlib

import beanstalkc as BSC


def count_gpus():
    result = subprocess.check_output(['nvidia-smi','-L'])
    num_gpu = len( [1 for x in result.decode('ascii').split('\n') if 'GPU' in x] )
    return num_gpu


def validate_gpus(gpus, ngpus):
    for gpu in gpus:
        if not (gpu >= 0 and gpu < ngpus):
            return (False, gpu)
    return (True,)


class DNNWorker:

    def __init__(self, gpus, config_file):
        hostname = platform.node()

        ngpus = count_gpus()
        gpu_val = validate_gpus(gpus, ngpus)
        if not gpu_val[0]:
            print(f'\rERROR! Only found {ngpu} GPUs [0..{ngpus-1}], cannot assign GPU {gpu_val[1]}')
            sys.exit(-1)
        else:
            gpustr = ",".join(str(x) for x in gpus)
            os.environ['CUDA_VISIBLE_DEVICES'] = gpustr
            self.gpu_info = (hostname, gpus, ngpus)
            self.my_id    = f'{hostname}-gpu{gpustr}'

        CONFIG = json.load(open(config_file,'r'))

        self.config = CONFIG['universal']
        local = CONFIG['machine_specific'][hostname]
        for k in local.keys():
            self.config[k] = local[k]

        self.current_jid = None
        self.bs_conn = BSC.Connection(host=self.config['beanstalk_host'],
                                      port=self.config['beanstalk_port']) 

    def start(self):
        self.__workerLoop()

    def __jobProgressCallback(self, updateDictionary):
        # SEND UPDATE TO MANAGER
        self.bs_conn.use('jobs_progress')
        update = {'job_id': self.current_jid,'progress': updateDictionary}
        self.bs_conn.put(json.dumps(update))

    def __workerLoop(self):
        print(f'\rWorker ID={self.my_id} Running...')

        self.bs_conn.watch('jobs_todo')
        self.bs_conn.use('jobs_completed')

        processingQueries = True

        while processingQueries:

            print(f'\r{self.my_id}: waiting for another message', end='')
            msg = self.bs_conn.reserve(1)

            if msg:
                jbody = json.loads(msg.body)
                print(f'\n\n{self.my_id}: Found Message: {jbody}...')
                msg.delete()

                # PERFORM THE REQUESTED JOB
                job_id = jbody['job_id']
                self.current_jid = job_id

                job_config = jbody['job_config']
                print(f'\r    Loading job info from file:  {job_config}')

                # TODO SHOULD CHECK HERE IF VALID JOB FILE
                #jpath = jbody['job_path']
                main_file = Path(jbody['main_file'])
                
                # Import the main module
                print(main_file.parent, main_file.stem)
                sys.path.append(str(main_file.parent))
                main = importlib.import_module(main_file.stem, package=sys.path[-1])

                rundir = jbody['rundir']
                runid = jbody['runid']
                if runid == 0:
                    start = f'start {job_config}'
                else:
                    start = f'continue --rid={runid}'
                # TODO NEED TO HANDLE RESUME WITH SPECIFIC RUN ID

                args = jbody['other_params']
                command_string = f'-D {rundir} {args} {start}'.split()

                print(f'\r    Running job {job_id}...')
                # TODO Do ACTUAL Processing here

                main.main(commands=command_string,
                          callback=(lambda response:
                                    self.__jobProgressCallback(response)))
                print(f'\rjob {job_id} completed...\n\n')
                # Delete the module
                sys.path.pop()
                del sys.modules[main_file.stem]
                del main

                # RESPONSE
                response = {'job_id':job_id,'completed_by':self.my_id}
                print(f'\r{self.my_id}: trying to send completion message... ', end="")
                self.bs_conn.put(json.dumps(response))
                print('\rdone')
                #self.sendLock.release()

                self.current_jid = None

            else:
                time.sleep(1)


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--gpus', type=int, required=True, nargs='+')
    parser.add_argument('--config', type=str, default='queue_config.json')
    args = parser.parse_args()

    worker = DNNWorker(gpus=args.gpus, config_file=args.config)
    worker.start()

