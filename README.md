# hack_tikv_io
An Experiment on TiKV's Disk I/O Scheduling

# Build and Run

```bash
$ bash make.sh
$ ll hack.so hack_go.so
$ LD_PRELOAD=$PWD/hack.so bin/tikv-server #...
```
