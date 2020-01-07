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
usage: ffmpeg_distributed.py [-h] [-s SEGMENT_LENGTH] -H HOST [-k] [-r]
                             [-t TMP_DIR]
                             [--ffmpeg-split-args FFMPEG_SPLIT_ARGS]
                             input_file output_file ffmpeg_args

Splits a file into segments and processes them on multiple hosts in parallel
using ffmpeg and SSH.

positional arguments:
  input_file            File to encode.
  output_file           Path to encoded output file.
  ffmpeg_args           Arguments to pass to the (remote) ffmpeg instances.
                        For example: "-c:v libx264 -crf 23 -preset fast"

optional arguments:
  -h, --help            show this help message and exit
  -s SEGMENT_LENGTH, --segment-length SEGMENT_LENGTH
                        Segment length in seconds.
  -H HOST, --host HOST  SSH hostname(s) to encode on. Use "localhost" to
                        include the machine you're running this from. Can
                        include username.
  -k, --keep-tmp        Keep temporary segment files instead of deleting them
                        on successful exit.
  -r, --resume          Don't split the input file again, keep existing
                        segments and only process the missing ones.
  -t TMP_DIR, --tmp-dir TMP_DIR
                        Directory to use for temporary files. Should not
                        already exist and will be deleted afterwards.
  --ffmpeg-split-args FFMPEG_SPLIT_ARGS
                        Arguments to pass to the ffmpeg instance splitting the
                        input file into segments. For example "-an" to get rid
                        of audio.
```

# Note About Audio
While video usually splits and concats cleanly, audio inside video files sometimes doesn't (dropouts/clicking between segments). Thankfully it's not too hard to process that separately and combine it with the output file by hand. Bit of a pain but oh well 🤷
