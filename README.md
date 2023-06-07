# Distributed Systems
本项目完成[6.5840](https://pdos.csail.mit.edu/6.824/index.html) - Spring 2023课程的四个lab，根据实验相关要求通过全部测试（见[test](./test)）。

lab1: 本实验构建一个 [MapReduce](http://static.googleusercontent.com/media/research.google.com/zh-CN//archive/mapreduce-osdi04.pdf) 系统。实现一个调用应用程序 Map 和 Reduce 函数并处理读写文件的 worker 进程，以及一个将任务分发给 worker 并处理失败的worker的 coordinator 进程。

lab2-4: 构建容错键/值存储系统。lab2构建一种复制的状态机协议 [Raft](https://pdos.csail.mit.edu/6.824/papers/raft-extended.pdf)。lab3在 [Raft](https://pdos.csail.mit.edu/6.824/papers/raft-extended.pdf) 之上构建键/值服务。lab4在多个复制的状态机上构建“分片”服务，以获得更高的性能。
系统交互：https://pdos.csail.mit.edu/6.824/notes/raft_diagram.pdf
