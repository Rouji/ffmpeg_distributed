#!/usr/bin/env python3
from queue import SimpleQueue, Empty
from subprocess import Popen, DEVNULL, PIPE, TimeoutExpired
import select
from shutil import rmtree
from glob import glob
from os import mkdir, unlink, listdir, environ
from os.path import basename, abspath, expanduser, isfile, isdir, getsize
from threading import Thread
from time import sleep
from typing import List, NamedTuple, Callable, Union
from signal import signal, SIGINT
from sys import exit, stderr
from shlex import split, join
from hashlib import md5
from time import strptime
from tqdm import tqdm
import re

DEBUG = 'DEBUG' in environ

def dprint(*args, **kwargs):
    if DEBUG:
        print(*args, *kwargs)

def _popen(args, **kwargs):
    dprint(f'calling subprocess: {args}')
    kwargs['stderr'] = PIPE
    kwargs['stdout'] = DEVNULL
    kwargs['universal_newlines'] = True
    return Popen(args, **kwargs)

class Task(NamedTuple):
    input_file: str
    output_file: str
    ffmpeg_args: List[str] = []

class FFMPEGProc:
    _duration_re = re.compile(r'.*Duration:\s*-?(?P<time_h>[0-9]+):(?P<time_m>[0-9]+):(?P<time_s>[0-9.]+),')
    _progress_re = re.compile(r'frame=\s*(?P<frame>[0-9]+)\s+fps=\s*(?P<fps>[0-9]+).*time=-?(?P<time_h>[0-9]+):(?P<time_m>[0-9]+):(?P<time_s>[0-9,.]+)\s+.*speed=(?P<speed>[0-9\.]+)x')

    @staticmethod
    def _match_to_sec(match):
        return int(match.group('time_h'))*3600+int(match.group('time_m'))*60+float(match.group('time_s'))

    def __init__(self, cmd: Union[list, str], shell=False, stdin=DEVNULL, stdout=DEVNULL, update_callback: Callable[[int,int,float,float,float], None] = None):
        self._cmd = cmd
        self._update_callback = update_callback
        self._should_stop = False
        self._shell = shell
        self._duration = None
        self._stdin = stdin
        self._stdout = stdout
        self.stderr = ''

    def stop(self):
        self._should_stop = True

    def run(self):
        self._proc = Popen(self._cmd, shell=self._shell, stderr=PIPE, stdin=self._stdin, stdout=self._stdout, universal_newlines=True)
        poll = select.poll()
        poll.register(self._proc.stderr)
        while self._proc.poll() is None and not self._should_stop:
            if not poll.poll(1):
                sleep(0.1)
                continue
            sleep(0.001)
            line = self._proc.stderr.readline()
            match = self._progress_re.match(line)
            if not match:
                self.stderr += line

            if match and self._update_callback:
                self._update_callback(
                    int(match.group('frame')),
                    int(match.group('fps')),
                    self._match_to_sec(match),
                    self._duration,
                    float(match.group('speed'))
                )
            elif self._duration is None:
                match = self._duration_re.match(line)
                if match:
                    self._duration = self._match_to_sec(match)

        try:
            out, err = self._proc.communicate(timeout=1)
            self.stderr += err
        except TimeoutExpired as ex:
            pass
        return self._proc.returncode

class TqdmAbsolute(tqdm):
    def __init__(self, *args, **kwargs):
        kwargs['bar_format'] = '{l_bar}{bar}|{n:.1f}/{total:.1f} [{elapsed}<{remaining}]'
        kwargs['dynamic_ncols'] = True
        if not 'total' in kwargs:
            kwargs['total'] = 99999999
        if not 'leave' in kwargs:
            kwargs['leave'] = False
        super().__init__(*args, **kwargs)
    def update(self, to):
        super().update(to - self.n)  # will also set self.n = b * bsize

class TaskThread(Thread):
    def __init__(self, host: str, arg: str, task_queue: SimpleQueue, bar_pos):
        super().__init__()
        self._should_stop = False
        self._host = host
        self._arg = arg
        self._task_queue = task_queue
        self._ffmpeg = None
        self._bar = TqdmAbsolute(desc=host, position=bar_pos)
        self._current_file = None

    def stop(self):
        self._should_stop = True
        if self._ffmpeg:
            self._ffmpeg.stop()

    def run(self):
        def upd(frames, fps, time, duration, speed):
            self._bar.total = duration or 999
            self._bar.desc = self._host + ': ' + self._current_file
            self._bar.update(time)
        try:
            while not self._should_stop:
                task = self._task_queue.get(False)

                self._current_file = basename(task.input_file)
                with open(task.input_file, 'r') as infile, open(task.output_file, 'w') as outfile:
                    ffmpeg_cmd = [
                        'nice', '-n10', 'ionice', '-c3',
                        'ffmpeg', '-f', 'matroska', '-i', 'pipe:',
                        *task.ffmpeg_args,
                        '-f', 'matroska', 'pipe:'
                    ]
                    if self._host != 'localhost':
                        ffmpeg_cmd = ['ssh', self._host, self._arg, join(ffmpeg_cmd)]
                    self._ffmpeg = FFMPEGProc(ffmpeg_cmd, stdin=infile, stdout=outfile, update_callback=upd)

                    ret = self._ffmpeg.run()
                    if ret != 0:
                        tqdm.write(f'task for {self._current_file} failed on host {self._host}', file=stderr)
                        tqdm.write(self._ffmpeg.stderr, file=stderr)
                        self._task_queue.put(task)
        except Empty:
            pass
        self._bar.close()

def encode(hosts: List[str], args: List[str],input_file: str, output_file: str, segment_seconds: float = 60, remote_args: str = '', concat_args: str = '', tmp_dir: str = None, keep_tmp=False, resume=False, copy_input=False):
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
        cv = ['copy'] if copy_input else ['libx264', '-crf', '0', '-preset', 'ultrafast', '-bf', '0']
        with TqdmAbsolute(desc="splitting input file") as bar:
            def upd(frames, fps, time, duration, speed):
                bar.total = duration
                bar.update(time)
            ffmpeg = FFMPEGProc([
                    'ffmpeg', '-i', expanduser(input_file),
                    '-an', '-sn',
                    '-c:v', *cv,
                    '-f', 'segment', '-reset_timestamps', '1', '-segment_time', str(segment_seconds) + 's',
                    tmp_in + '/%08d.mkv'
                ],
                update_callback=upd
            )
            ret = ffmpeg.run()
        if ret != 0:
            tqdm.write(ffmpeg.stderr, file=stderr)
            return

    task_queue = SimpleQueue()
    for f in sorted(glob(tmp_in+'/*')):
        output_segment = tmp_out+f'/{basename(f)}'
        # skip already encoded segments
        if not isfile(output_segment):
            task_queue.put(Task(f, output_segment, split(remote_args)))

    threads = [TaskThread(host, arg, task_queue, pos) for host,arg,pos in zip(hosts,args,range(len(hosts)))]

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

    with TqdmAbsolute(desc='concatenating output segments') as bar:
        def upd(frames, fps, time, duration, speed):
            bar.total = duration
            bar.update(time)
        ffmpeg = FFMPEGProc([
                'ffmpeg', '-i', input_file,
                '-f', 'concat', '-safe', '0', '-i', 'output_segments.txt',
                '-map_metadata', '0:g',
                '-map', '1:v',
                '-map', '0:a?',
                '-map', '0:s?',
                '-c:v', 'copy',
                '-c:s', 'copy',
                *split(concat_args),
                '-y', output_file
            ],
            update_callback=upd
        )
        if ffmpeg.run()  != 0:
            tqdm.write(ffmpeg.stderr, file=stderr)
            return
    unlink('output_segments.txt')

    if not keep_tmp:
        rmtree(tmp_dir)

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='Splits a file into segments and processes them on multiple hosts in parallel using ffmpeg over SSH.')
    parser.add_argument('input_file', help='File to encode.')
    parser.add_argument('output_file', help='Path to encoded output file.')
    parser.add_argument('remote_args', help='Arguments to pass to the remote ffmpeg instances. For example: "-c:v libx264 -crf 23 -preset fast"')
    parser.add_argument('concat_args', default='', help='Arguments to pass to the local ffmpeg concatenating the processed video segments and muxing it with the original audio/subs/metadata. Mainly useful for audio encoding options, or "-an" to get rid of it.')
    parser.add_argument('-s', '--segment-length', type=float, default=10, help='Segment length in seconds.')
    parser.add_argument('-H', '--host', action='append', help='SSH hostname(s) to encode on. Use "localhost" to include the machine you\'re running this from. Can include username.', required=True)
    parser.add_argument('-A', '--args', default=None, action='append', help='SSH arguments to use with the previous host.', required=False)
    parser.add_argument('-k', '--keep-tmp', action='store_true', help='Keep temporary segment files instead of deleting them on successful exit.')
    parser.add_argument('-r', '--resume', action='store_true', help='Don\'t split the input file again, keep existing segments and only process the missing ones.')
    parser.add_argument('-t', '--tmp-dir', default=None, help='Directory to use for temporary files. Should not already exist and will be deleted afterwards.')
    parser.add_argument('-c', '--copy-input', action='store_true', help='Don\'t (losslessly) re-encode input while segmenting. Only use this if your input segments frame-perfectly with "-c:v copy" (i.e. it has no B-frames)')
    args = parser.parse_args()
    encode(
        args.host,
        args.args,
        args.input_file,
        args.output_file,
        segment_seconds=args.segment_length,
        remote_args=args.remote_args,
        concat_args=args.concat_args,
        tmp_dir=args.tmp_dir,
        keep_tmp=args.keep_tmp,
        resume=args.resume,
        copy_input=args.copy_input
    )
