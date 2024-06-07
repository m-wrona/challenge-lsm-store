# challenge-lsm-store

Coding challenge 

Tasks:
* store segments on disk in an efficient format
* calculate TF/IDF score of the documents



## Performance


#### JSON reading

```go
pkg: segments-disk-writer
Benchmark_JSON_Read/standard_json-10                  12         189195451 ns/op        186907830 B/op    360114 allocs/op
Benchmark_JSON_Read/standard_json-10                  12         188350660 ns/op        186907842 B/op    360114 allocs/op
Benchmark_JSON_Read/jsoniter-10                       45          51591748 ns/op        199886143 B/op    360120 allocs/op
Benchmark_JSON_Read/jsoniter-10                       46          51684510 ns/op        199886141 B/op    360120 allocs/op

```