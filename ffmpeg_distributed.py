#!/usr/bin/env python3
from queue import SimpleQueue, Empty
from subprocess import Popen, DEVNULL, PIPE, TimeoutExpired
from shutil import rmtree
from glob import glob
from os import mkdir, unlink, listdir
from os.path import basename, abspath, expanduser, isfile, isdir
from threading import Thread
from time import sleep
from typing import List, NamedTuple
from signal import signal, SIGINT
from sys import exit, stderr
from shlex import quote
from hashlib import md5
from os import environ

DEBUG = 'DEBUG' in environ

def dprint(*args, **kwargs):
    if DEBUG:
        print(*args, *kwargs)

def _popen(args, **kwargs):
    dprint(f'calling subprocess: {args}')
    kwargs['stderr'] = DEVNULL
    kwargs['stdout'] = DEVNULL
    #kwargs['universal_newlines'] = True
    return Popen(args, **kwargs)

class Task(NamedTuple):
    input_file: str
    output_file: str
    ffmpeg_args: str = ''

class TaskThread(Thread):
    def __init__(self, host: str, task_queue: SimpleQueue):
        super().__init__()
        self._should_stop = False
        self._host = host
        self._task_queue = task_queue

    def stop(self):
        self._should_stop = True

    def run(self):
        try:
            while not self._should_stop:
                task = self._task_queue.get(False)

                print(f'{basename(task.input_file)} -> {self._host}')
                ffmpeg_cmd = f'ffmpeg -f matroska -i pipe: {task.ffmpeg_args} -f matroska pipe:'
                if self._host == 'localhost':
                    proc = _popen(f'{ffmpeg_cmd} <{task.input_file} >{task.output_file}', shell=True)
                else:
                    proc = _popen(f'ssh {self._host} "{ffmpeg_cmd}" <{task.input_file} >{task.output_file}', shell=True)

                while proc.poll() is None and not self._should_stop:
                    sleep(0.1)
                try:
                    out, err = proc.communicate(timeout=1)
                except TimeoutExpired as ex:
                    pass

                if proc.returncode != 0 or self._should_stop:
                    unlink(task.output_file)
                    print(f'"{task.input_file}" failed on "{self._host}"', file=stderr)
                    # re-queue if the task failed
                    self._task_queue.put(task)
        except Empty:
            pass
        pass

def encode(hosts: List[str], input_file: str, output_file: str, segment_seconds: float, ffmpeg_args: str = '', tmp_dir: str = None, keep_tmp=False, resume=False, split_args=''):
    input_file = abspath(expanduser(input_file))
    output_file = abspath(expanduser(output_file))
    tmp_dir = tmp_dir or 'ffmpeg_segments_'+md5(input_file.encode()).hexdigest()
    tmp_in = f'{tmp_dir}/in'
    tmp_out = f'{tmp_dir}/out'
    try:
        mkdir(tmp_dir)
        mkdir(tmp_in)
        mkdir(tmp_out)
    except FileExistsError:
        if not resume:
            raise

    # skip splitting on resume
    if len(listdir(tmp_in)) == 0 or not resume:
        # TODO: check returncode
        dprint('splitting input file')
        _popen(f'ffmpeg -i {quote(expanduser(input_file))} -c copy {split_args} -f segment -reset_timestamps 1 -segment_time {str(segment_seconds)}s {tmp_in}/%08d.mkv', shell=True).communicate()

    task_queue = SimpleQueue()
    for f in sorted(glob(tmp_in+'/*')):
        output_segment = tmp_out+f'/{basename(f)}'
        # skip already encoded segments
        if not isfile(output_segment):
            task_queue.put(Task(f, output_segment, ffmpeg_args))

    threads = [TaskThread(host, task_queue) for host in hosts]

    def sigint(sig, stack):
        print('Got SIGINT, stopping...')
        for thread in threads:
            thread.stop()
        for thread in threads:
            thread.join()
        exit(1)

    signal(SIGINT, sigint)

    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    with open('output_segments.txt', 'w') as f:
        f.write('\n'.join([f"file '{file}'" for file in sorted(glob(tmp_out+'/*'))]))

    _popen(['ffmpeg', '-f', 'concat', '-safe', '0', '-i', 'output_segments.txt', '-c', 'copy', output_file]).communicate()
    unlink('output_segments.txt')

    if not keep_tmp:
        rmtree(tmp_dir)

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='Splits a file into segments and processes them on multiple hosts in parallel using ffmpeg and SSH.')
    parser.add_argument('input_file', help='File to encode.')
    parser.add_argument('output_file', help='Path to encoded output file.')
    parser.add_argument('ffmpeg_args', help='Arguments to pass to the (remote) ffmpeg instances. For example: "-c:v libx264 -crf 23 -preset fast"')
    parser.add_argument('-s', '--segment-length', type=float, default=10, help='Segment length in seconds.')
    parser.add_argument('-H', '--host', action='append', help='SSH hostname(s) to encode on. Use "localhost" to include the machine you\'re running this from. Can include username.', required=True)
    parser.add_argument('-k', '--keep-tmp', action='store_true', help='Keep temporary segment files instead of deleting them on successful exit.')
    parser.add_argument('-r', '--resume', action='store_true', help='Don\'t split the input file again, keep existing segments and only process the missing ones.')
    parser.add_argument('-t', '--tmp-dir', default=None, help='Directory to use for temporary files. Should not already exist and will be deleted afterwards.')
    parser.add_argument('--ffmpeg-split-args', default='', help='Arguments to pass to the ffmpeg instance splitting the input file into segments. For example "-an" to get rid of audio.')
    args = parser.parse_args()
    encode(
        args.host,
        args.input_file,
        args.output_file,
        args.segment_length,
        args.ffmpeg_args,
        tmp_dir=args.tmp_dir,
        keep_tmp=args.keep_tmp,
        resume=args.resume,
        split_args=args.ffmpeg_split_args,
    )
