# DNN Queue

A worker queue for deep learning jobs. Spawn multiple workers with access to different computational resources and queue up jobs for them run

**Early beta version**

## Requirements
- python 3.6+ 
- beanstalkd

## Installation

## Usage

## Planned improvements

- Worker restarts itself after completing a job in order to purge the module namespace and have access to the most recent versions of code files 
- Some way of keeping jobs from starting too close together, in order to avoid conflicting usage of file system resources (trying to write to the same files/directories, etc) 
- Accept more general types of jobs (currently only works with the dnnutil package)
