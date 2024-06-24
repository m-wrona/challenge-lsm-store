# challenge-lsm-store

Coding challenge 

Tasks:
* store segments on disk in an efficient format
* calculate TF/IDF score of the documents


## Local development 

* packages (`lsm`, `memtable`, `sstable` etc.) should have unit tests without OS related dependencies when possible (in-memory, no file access etc. )

* `test` package contains integration tests (or at least component tests) to check real-life scenarios 

Usage:

1) Test preparation

```shell
make download-segments
````

2) Run tests

```shell
make test
```

or for shorter version

```shell
make test-short
```

which won't run some of long-running test cases which can be checked later during MR phase.

Those tests in general include more detailed checks for race conditions and concurrency aspects. 

3) Check code coverage

```shell
make coverage-show
```

or for interactive version: 

```shell
make coverage-html
```

## Lessons learned

It's my first implementation of LSM tree inspired mainly by Martin Kleppman's book "Designing Data-Intensive Applications".

During this challenge I've also checked & analysed other solutions, like:

* [Mini-LSM](https://skyzh.github.io/mini-lsm/)

* [CockroachDB - Pebble](https://github.com/cockroachdb/pebble)
 
And, like usual, first implementation verified my academic knowledge and proved that book knowledge and experience are totally different things. 

<br/> LSM tree is great for sure for keeping plain data, but it's not the Holy Grail solving all issues.  

Mainly, it can become a bottleneck pretty quickly when too many writes are performed at the same time.

Necessity of keeping WAL up-to-date during multiple writes becomes quite an issue and a challenge from performance and synchronisation point of view. 

To avoid that single LSM-tree can of course be using many WAL log files, but, I guess, it could complicate reading them in case of a failure more tricky.

I think another, and much more reasonable, way would be just introducing concept of partitions and/or buckets.

So we reach the point where DBs start defining tables which are divided in many partitions where in the end LSM store appears underneath.

Of course each table is supported by many indices which help to make more complex searches than just by ID.

And it's all noticeable in `test/DB.go` tests where, while counting TF-IDF and importing data, you can tell that additional abstraction layer

is missing for accessing data.

<br/> However, all that is just a tip of an iceberg. We haven't even reached topics yet, like:

* merging and compaction of data segments

* optimizing WAL by introducing `copy-on-write scheme` 

* all the read aspects of LSM tree (i.e. making sure that if data is not found in-memory, we don't search via all SSTable files by using `bloom filters` etc.)   

I haven't started addressing those yet, therefore I'm not able to predict how many more obstacles would arise there.

Based on my draft implementation I'm able to state that I don't like the way how I'm controlling concurrent access in `lsm` package.

I mean, I guess it's acceptable since it's based on basic `locks` in GoLang. However, based on my experience, I noticed

that `mutexes` are a shortcut (or very first draft approach) to tackle this issue - and the worst one since deadlock-prone. 

When you get better overview of a problem, it occurs quite often it can be replaced by using `channels` and possibly by other structures (i.e. `fractal trees`).

For now, I don't have any ideas how to simplify it though. I guess, it'd require more comprehensive implementation of aforementioned topics

and access to DB table. 

And perhaps appearance of server requests would simplify assertion whether locks are really required at given stage & level or not.
