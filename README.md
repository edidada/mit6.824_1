### 学习笔记

## lab1 MapReduce

coordinator:

预处理模块：处理文件信息，初始化任务队列

任务：第几个任务，哪一个文件，nReduce参数

任务队列：队列存任务，锁->发给worker没完成的还要重新加入队列

任务完成标记

worker:

处理map任务

处理reduce任务

处理完任务发送一个消息代表任务完成（不发送代表失败）

注：测试脚本wait -n运行不了，修改成wait

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

### 完成日志

完成rpc通信，传输基础数据

预处理分配map/reduce任务，初始化任务队列

提供get/post代表请求/完成任务接口

worker完成map/reduce处理

todo：直接加了一把大锁，在RPC层面就控制线程安全（实际上可以更细化）

### 可能的坑

待完善……