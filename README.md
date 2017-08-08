# `dedup` - a deduplication tool (+ library)

## Why

Deduplication can be thought of as a coarse grained compression that detects
duplicate data over much larger windows than most compressors work across. As
a result, deduplication is a good first step and passing deduplicated data into
a downstream compressor often results in much better compression performance
(in terms of compression ratio and sometimes speed)

## Library

`dedup` is a golang lib that allows for arbitrary data to be deduplicated
(input from an io.Reader). The `dedup.Deduplicator` and `dedup.Reduplicator`
are the workhorses that actually do all the work. For examples on how to use
them, see the dedup tool itself (in `cmd/dedup/main.go`).

```
err := dedup.NewDeduplicator(windowSize, mask).Do(os.Stdin, os.Stdout)

err := dedup.NewReduplicator().Do(os.Stdin, os.Stdout)
```

## Binary

This codebase also builds a cmdline tool named `dedup` (see `cmd/dedup`) that
can be used to deduplicate data. For example, consider this workload where we
save two similar docker containers:

```
akshay@spitfire:~/$ time docker save redmine bitnami/redmine | gzip | wc --bytes
497548816 # <-- 474.49 MB (or MiB)

real  1m7.900s
user  0m58.536s
sys   0m1.780s

akshay@spitfire:~/$ time docker save redmine bitnami/redmine | dedup | gzip | wc --bytes
295793793 # <-- 282.09 MB (or MiB)

real  0m50.261s
user  0m56.312s
sys   0m3.688s
```

As you can see, some workloads can benefit greatly from a combination of
deduplication + compression (in terms of both compression ratio and speed)

## Compression

Note that this lib (and tool) probably won't ever support built-in support for compression of the output stream. You should pick an appropriate compressor "downstream" from this lib/tool. You'll find that standalone compressors such as
`gzip`, `bzip2`, `xz` (and their parallel implementations - `pigz`, `pbzip2`,
`pxz`) are readily available on most linux distributions. These compressors
support pipelining (i.e. i/o can be pipelined via the shell) so there is no need
for this library to provide this functionality.

## TODO:

- Currently the deduplication lib consumes memory that is proportional to the
  size of the input file. (See issue #1)
- Document the usage and impact of the windowSize and zeroBits parameters used
  by the `Deduplicator`
- Add progress reporting when input is a large file (not stdin)
- Make cmdline args fully compatible with other compression tools ('-k', '-v')
