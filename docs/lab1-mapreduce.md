# 实验报告：Lab1 MapReduce

## 1. 实验目的

基于 MIT 6.5840（原 6.824）Lab1，使用 Go 实现一个分布式的 MapReduce 框架，包含：

- **Coordinator（协调者）**：负责任务的分配、进度跟踪与故障恢复；
- **Worker（工作节点）**：向 Coordinator 领取 Map / Reduce 任务并本地执行；
- **插件式 Map/Reduce 函数**：通过 `mrapps` 以独立 `main` 包形式提供（如 `wc`、`indexer`、`jobcount` 等）。

目标是熟悉 Go 的并发模型（`goroutine` / `channel`）、RPC 通信（`labrpc`）以及容错设计。

## 2. 实验环境

- 语言：Go（module 名 `6.5840`）
- 通信层：`src/labrpc`（课程自带的简化 RPC 框架）
- 序列化：`src/labgob`（基于 `encoding/gob`）
- 测试脚本：`main/test-mr.sh`、`main/test-mr-many.sh`

## 3. 实现方案

### 3.1 整体架构

- `mr/coordinator.go`：维护 Map 阶段与 Reduce 阶段的任务状态机，记录每个任务的状态（`idle` / `in-progress` / `completed`）以及分配给哪个 Worker、分配时间戳。
- `mr/worker.go`：轮询 Coordinator 领取任务；执行对应的 Map 或 Reduce 函数；通过中间文件（`mr-X-Y`）落盘。
- `mr/rpc.go`：定义任务请求/响应、任务完成上报等 RPC 消息结构。
- `main/mrcoordinator.go` / `main/mrworker.go`：分别是 Coordinator 与 Worker 的进程入口；`mrapps/*` 是被 worker 动态加载的 Map/Reduce 实现。

### 3.2 关键设计点

1. **任务分配与状态机**：Coordinator 持有所有任务，Worker 通过 RPC 拉取，完成后上报；
2. **超时与容错**：Coordinator 定期检查 `in-progress` 任务是否超时（例如 10s 未上报），超时则重新放回可分配队列，保证某个 Worker 崩溃后任务仍能被其他 Worker 完成；
3. **原子落盘**：Map 输出先写临时文件再 `rename`，避免 Reduce 读到半成品；
4. **退出条件**：所有 Map 与 Reduce 任务均 `completed` 后，Coordinator 通知 Worker 退出。

## 4. 测试结果

执行 `main/test-mr.sh` 与 `main/test-mr-many.sh`，全部用例通过（详见仓库根目录 `lab1_res.txt`）：

| 测试用例 | 结果 |
| --- | --- |
| wc test | PASS |
| indexer test | PASS |
| map parallelism test | PASS |
| reduce parallelism test | PASS |
| job count test | PASS |
| early exit test | PASS |
| crash test | PASS |

脚本最终输出 `PASSED ALL TESTS`。

> 注：日志中 `rpc.Register: method "Done" has 1 input parameters; needs exactly three` 是 `labrpc` 框架对不符合其约定的方法的告警，不影响测试正确性；`cannot load 6.5840/diskv` 来自仓库中旧的 `main/diskvd.go` 等文件（属于早期 6.824 实验遗留，与 6.5840 的 MapReduce 无关，已用 `//go:build ignore` 标记排除，不参与编译）。

## 5. 遇到的问题与解决

- **RPC 方法签名告警**：`labrpc` 要求被注册方法必须严格符合 `(args, reply, done chan *Reply)` 三参数形式，否则仅打印告警。检查并修正注册的方法签名即可。
- **崩溃恢复**：在 crash test 中需要保证 Worker 中途退出后任务能被重新分配，通过 Coordinator 端的超时回收机制解决。

## 6. 小结

Lab1 打通了 Go 并发与 RPC 的基础，并实践了“任务分解 + 心跳/超时恢复”的经典分布式协调思路，为后续 Raft 实验奠定工程基础。
