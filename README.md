# ffmpeg_distributed
"Simple" little script I use for distributed ffmpeg stuff that  
* Splits input file into segments
* Pipes them through SSH, through ffmpeg on remote hosts in parallel
* Concats the processed segments
* ???
* Profit

Neither very foolproof, nor very feature rich. *Caveat emptor.*

# Help Message
```
usage: ffmpeg_distributed.py [-h] [-s SEGMENT_LENGTH] -H HOST [-k] [-r] [-t TMP_DIR] [-c]
                             input_file output_file remote_args concat_args

Splits a file into segments and processes them on multiple hosts in parallel using ffmpeg over SSH.

positional arguments:
  input_file            File to encode.
  output_file           Path to encoded output file.
  remote_args           Arguments to pass to the remote ffmpeg instances. For example: "-c:v libx264 -crf 23 -preset
                        fast"
  concat_args           Arguments to pass to the local ffmpeg concatenating the processed video segments and muxing it
                        with the original audio/subs/metadata. Mainly useful for audio encoding options, or "-an" to
                        get rid of it.

optional arguments:
  -h, --help            show this help message and exit
  -s SEGMENT_LENGTH, --segment-length SEGMENT_LENGTH
                        Segment length in seconds.
  -H HOST, --host HOST  SSH hostname(s) to encode on. Use "localhost" to include the machine you're running this from.
                        Can include username.
  -k, --keep-tmp        Keep temporary segment files instead of deleting them on successful exit.
  -r, --resume          Don't split the input file again, keep existing segments and only process the missing ones.
  -t TMP_DIR, --tmp-dir TMP_DIR
                        Directory to use for temporary files. Should not already exist and will be deleted afterwards.
  -c, --copy-input      Don't (losslessly) re-encode input while segmenting. Only use this if your input segments
                        frame-perfectly with "-c:v copy" (i.e. it has no B-frames)
```
