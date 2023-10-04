# lab1 MapReduce

## 前置知识

Golang语法及rpc相关

## 环境搭建

任务文件下载：详见任务书

```
git clone git://g.csail.mit.edu/6.5840-golabs-2023 6.5840
```

安装Golang环境（如果已有Golang环境，推荐卸载）：

```
wget -qO- https://dl.google.com/go/go1.13.6.linux-amd64.tar.gz | sudo tar xz -C /usr/local
```

如果还是失败，尝试更新路径：

```
vim ~/.bashrc
export PATH=$PATH:/usr/local/go/bin
source ~/.bashrc
```

## 任务说明

给定若干个文本文件，识别单词统计个数，并输出到文件 mr-out-0

形如:

```
A 1
B 23
...
Z 456
```

### 文件解释

下列给出本实验重要的相关文件：

```
src
├── go.mod
├── go.sum
├── kvraft
├── labgob
├── labrpc
├── main
│   ├── crash.so
│   ├── diskvd.go
│   ├── lockc.go
│   ├── lockd.go             
│   ├── mrcoordinator.go            入口函数：调用coordinator
│   ├── mrsequential.go             示例函数：用来测试、参考
│   ├── mrworker.go                 入口函数：调用worker
│   ├── pbc.go
│   ├── pbd.go
│   ├── pg-being_ernest.txt         文件输入
│   ├── pg-dorian_gray.txt          |
│   ├── pg-frankenstein.txt         |
│   ├── pg-grimm.txt                |
│   ├── pg-huckleberry_finn.txt     |
│   ├── pg-metamorphosis.txt        |
│   ├── pg-sherlock_holmes.txt      |
│   ├── pg-tom_sawyer.txt           文件输入
│   ├── test-mr-many.sh             测试脚本
│   ├── test-mr.sh                  测试脚本
│   ├── viewd.go
│   └── wc.so
├── models
├── mr                              lab1需要修改的文件
│   ├── coordinator.go              *修改文件1
│   ├── rpc.go                      *修改文件2
│   └── worker.go                   *修改文件3
├── mrapps                          测试脚本载入的不同插件
│   ├── crash.go                    work会随机退出（最困难） 
│   ├── early_exit.go
│   ├── indexer.go
│   ├── jobcount.go
│   ├── mtiming.go
│   ├── nocrash.go
│   ├── rtiming.go
│   ├── wc.go                       正常版本（最简单）   
├── porcupine
├── raft
├── shardctrler
└── shardkv
```

### 指令解释

下面解释本实验出现的部分指令：

把wc.go加载成插件的形式，后续会在work.go里用到mapf和reducef就是这个文件提供的函数（map和reduce）

```
go build -buildmode=plugin ../mrapps/wc.go
```
自测的时候在两个终端分别输入以下两个指令，分别启动一个coordinator和worker用来测试（视情况而定，自己也可以启动多个worker）

```
go run mrcoordinator.go pg-*.txt
go run mrworker.go wc.so
```

运行测试脚本，多组数据（通过不同的plugin来实现）

会启动1个coordinator和3个worker进行测试

测试脚本会在终端打印PASS/FAIL来提示是否通过

```
bash test-mr.sh
```

由于单组测试不稳定（crash会随机退出），推荐最终使用多组测试验证是否完成lab1任务

```
bash test-mr-many.sh 10
```

注：测试脚本wait -n运行不了，修改成wait

## 任务目标

补全coordinator.go/worker.go/rpc.go的代码

生成mr-out-0，mr-out-1，mr-out-2...mr-out-nReduce-1共nReduce个文件

下标通过哈希区分，对应的字符串写入对应的文件里。哈希函数在worker.go里

### coordinator.go：

1.预处理map任务：

假设文件数量为n，那就切分成n个任务，存入带锁任务队列

2.预处理reduce任务：

切分成nReduce个任务（这个参数由原代码提供），存入带锁任务队列

3.提供接口：给work提供任务

从任务队列里取任务，将相关参数下发给worker（如任务类型，任务下标等）

4.提供接口：接收work任务完成上报

打上任务完成的标记，如果map任务都完成，才开始发送reduce任务。如果在发送任务后没接收到work上报，将任务重新加入任务队列

### worker.go：

1.通过rpc向coordinator请求任务

2.处理map任务：

假设任务编号为x（x范围0到n-1），生成mr-x-y个中间文件(如mr-x-0,mr-x-1...)，y范围0到nReduce-1

其中y的生成法则是根据ihash(key)%nReduce而定的

3.处理reduce任务

假设任务编号是y，将所有的mr-x-y（x范围0到n-1）中间文件（如mr-0-y,mr-1-y...）整合成mr-out-y文件里

4.通过rpc通知coordinator任务完成

## 学习参考

lab1任务书（必看，根据这个来写的）：https://pdos.csail.mit.edu/6.824/labs/lab-mr.html

提供一份任务书的翻译（但是推荐自己看原文）：https://blog.csdn.net/hh1986170901/article/details/120262227

有图片的教程，帮助理解（没详细看过）：https://blog.csdn.net/qq_34872231/article/details/130243535

视频理解，可能有一定帮助：https://www.bilibili.com/video/BV1rS4y1n7PC

能提供一定帮助：https://huaweicloud.csdn.net/63356224d3efff3090b54e4e.html

## 流程图

```mermaid
graph LR
    F1 --读取文件--> C
    F2 --> C
    F3 --> C

    C --map阶段--> W1
    C --> W2
    C --> W3

    W1--生成中间产物-->M1-1
    W1---->M1-2
    W2---->M2-1
    W2---->M2-2
    W3---->M3-1
    W3---->M3-2

    M1-1--reduce阶段-->Wa
    M1-2-->Wb
    M2-1-->Wa
    M2-2-->Wb
    M3-1-->Wa
    M3-2-->Wb

    Wa--->O1
    Wb--->O2

    F1[file1]
    F2[file2]
    F3[file3]

    W1[worker1]
    W2[worker2]
    W3[worker3]

    Wa[workera]
    Wb[workerb]

    M1-1[1-1]
    M1-2[1-2]
    M2-1[2-1]
    M2-2[2-2]
    M3-1[3-1]
    M3-2[3-2]

    C[coordinator]

    O1[output1]
    O2[output2]
```

注：所有的map阶段完成后，才执行reduce阶段

注：worker1和worker2和workera可以是同一个work进程，但是请求到了不同的任务（不同时），也是允许的

注：生成中间文件mr-x-y（严格来说应该从0-0开始），x的范围是0到文件数-1，y的范围是0到nReduce-1

### coordinator流程图

```mermaid
graph TD
    coordinator开始-->预处理map任务
    预处理map任务-->预处理reduce任务
    预处理reduce任务-->等待work请求map任务
    等待work请求map任务-->发送map任务
    发送map任务--有map任务-->等待work请求map任务
    发送map任务--接收到post且无map任务-->等待work请求reduce任务
    等待work请求reduce任务-->发送reduce任务
    发送reduce任务--有reduce任务-->等待work请求reduce任务
    发送reduce任务--接收到post且无reduce任务-->coordinator结束
```

注：每次在发送任务的时候，同时会起一个协程，确认是否有post返回

### worker流程图

```mermaid
graph TD
    worker开始-->向coordinator请求任务
    向coordinator请求任务--map任务-->调用map处理生成中间文件
    向coordinator请求任务--reduce任务-->调用reduce生成最终文件
    调用map处理生成中间文件-->通知coordinator任务完成
    调用reduce生成最终文件-->通知coordinator任务完成
    通知coordinator任务完成-->worker结束
```

## 可能的坑

极度推荐看任务书的Hints

十次测试都能通过一般就比较稳定了，一次大概要三分钟

## 源码

https://gitee.com/jiangnan1634605411/mit6.824

# lab2A Raft-Election

2A要完成的实验主要是raft的基础架构，核心就是三大状态（follower，candidate，leader）的切换


## 任务说明

搭建raft集群，实现基础的超时-选举功能

一共有三个测试：

1.是否能选出一个leader，并保证任期稳定

2.是否能选出leader后，在leader挂掉后，还能选出新leader

3.更多的节点测试，更多的轮数测试集群是否正常运行

go test通过rf.GetState()来调用检查当前节点状态是否为leader，通过Make来创建节点

### 文件解释

下列给出本实验重要的相关文件：

```
.
├── config.go
├── persister.go
├── raft.go             本次实验需要修改的文件
├── test_test.go        测试用例：只用到2A部分
└── util.go
```

### 指令解释

下面解释本实验出现的部分指令：

```
go test -run 2A
```
只需要用这个指令就可以进行测试了，推荐测试一百组（约一小时）。可以写脚本测试，也可以在这条指令的基础上加参数-count 100测试。据说2A测试失败率不超过3%就可以下一步实验了。

## 任务目标

补全raft.go的代码，需要补全的部分基本由注释提供

1.补全raft结构体信息

2.补全拉票参数/心跳包参数信息，即RequestVoteArgs，RequestVoteReply等

3.补全Make()函数，提供测试函数初始化接口

4.补全GetState()函数，提供给测试函数接口

5.补全ticker()函数，控制状态（如follower）对应的行为

6.补全拉票函数RequestVote及对应rpc接口

7.提供心跳函数AppendEntries及对应rpc接口

## 学习参考

可视化动图：http://thesecretlivesofdata.com/raft/#overview

任务书：http://nil.csail.mit.edu/6.824/2020/labs/lab-raft.html

论文：http://nil.csail.mit.edu/6.824/2020/papers/raft-extended.pdf

### 其他参考

视频：https://www.bilibili.com/video/BV1uF411G7vc/

论文翻译：https://blog.csdn.net/erlib/article/details/53671783

任务书翻译：https://zhuanlan.zhihu.com/p/248686289

部分教程（无好坏之分，只是额外提供一个学习的方向，适合自己才是最好的）：

https://blog.csdn.net/MarksSa/article/details/127429742

https://blog.csdn.net/qq_52245648/article/details/129485643

https://blog.csdn.net/Z_Stand/article/details/116565983

https://lizhaolong.blog.csdn.net/article/details/105981661

https://blog.csdn.net/lin819747263/article/details/106316163

https://blog.csdn.net/z1213035927/article/details/122867095

https://zhuanlan.zhihu.com/p/476644274

## 流程图

### follower

最初始的状态就是follower，由于超时（没有收到心跳包）从而转变为candidate

### candidate

变为candidate后，开始向其他节点通过rpc拉票，如果拉票超过半数就变为leader，否则就继续成为candidate拉票

在期间如果发现有其他leader任期更大，就变为对方（不严谨）的follower

### leader

作为leader要一直向其他节点发出心跳包，维护自己的leader地位

如果遇到了其他leader任期更大，也要变为对方的follower

## 可能的坑

首先是测试不稳定，推荐多组测试后才能确定代码没问题（但是测一百组就要一个多小时）

注意：休眠时间的设置可能和论文不同，需要自己分析调整

如果发现任期频繁更换，可能是超时时间的设置问题

如果是发现任期没leader，可能是休眠时间设置的问题

如果发现一个任期多leader，可能是拉票出现问题

另：似乎如果先产生leader，再崩了超过一半节点，集群还会运行。但是测试似乎没有这个方面的样例

## 源码

https://gitee.com/jiangnan1634605411/mit6.824