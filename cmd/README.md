# Commands

| Commands | Description |
| --- | --- |
| convert.go | migrates existing standard whisper files to compressed format |
| dump.go | prints out data in whipser files (support both compressed and standard format) |
| verify.go | checks if two whisper files are containing the same data, made for verify migration result |
| icheck.go | checks if a compressed whipser file is corrupted through crc32 values saved in the headers |
| summarize.go | generates md5 sums of values in different archives (for quick comparsion of whisper files on different hosts) |
| estimate.go | to estimate the file compression ratio for specific retention policy on different compressed data point sizes |

## Caveats of convert.go

* convert.go locks a whisper file during convertion (by creating a temporary .cwsp files in the same directory)
* it might cause some inaccurate or lost of data points (usually 2-4) in lower archives (because of the buffer design in higher archives)
* it generates logs and progress files
