# 实验报告：Lab2A Raft 选主（Leader Election）

## 1. 实验目的

基于 MIT 6.5840（原 6.824）Lab2A，实现 Raft 共识算法的**领导者选举**部分：

- 节点在 `Follower` / `Candidate` / `Leader` 三种状态间转换；
- 通过随机选举超时（election timeout）触发选举；
- 使用 RequestVote RPC 完成投票与任期（Term）递增；
- 保证在同一任期内至多只有一个 Leader。

## 2. 实验环境

- 语言：Go（module 名 `6.5840`）
- 通信层：`src/labrpc`
- 持久化/测试脚手架：`src/raft`（含 `config.go`、`persister.go`、`util.go`、`test_test.go`）

## 3. 实现方案

### 3.1 状态机

- `raft/raft.go`：核心结构体 `Raft` 维护 `currentTerm`、`votedFor`、`state`、`log[]`、以及选举超时定时器与心跳定时器。
- 节点启动时处于 `Follower`，并设置一个随机的选举超时（如 300–600ms 区间）。

### 3.2 选举流程

1. **Follower → Candidate**：选举超时未收到合法 Leader 的心跳/附加日志，则自增 `currentTerm`，置 `state=Candidate`，给自己投一票，并向其他节点发送 `RequestVote`；
2. **投票规则**：每个任期内只能投出一票（`votedFor`），且候选人的日志“至少和自己一样新”才投票；
3. **Candidate → Leader**：获得多数票后成为 Leader，立即发送心跳（空 AppendEntries）阻止其他节点发起选举；
4. **任期回退**：收到更高 `Term` 的消息时，节点降级为 Follower 并更新 `currentTerm`。

### 3.3 关键设计点

- **随机选举超时**：避免多个节点同时超时导致选票分裂（split vote）；
- **心跳维持权威**：Leader 定期发送空 AppendEntries，Follower 收到后重置选举定时器；
- **网络分区恢复**：分区期间旧 Leader 仍在低任期发心跳，恢复连通后高任期节点会让其降级。

## 4. 测试结果

执行 `go test 6.5840/raft -run 2A`，三个官方用例全部通过（详见仓库根目录 `lab2a_res.txt`）：

| 测试用例 | 结果 |
| --- | --- |
| Test (2A): initial election | Passed |
| Test (2A): election after network failure | Passed |
| Test (2A): multiple elections | Passed |

日志中的 `labgob warning: Decoding into a non-default variable/field Term may not work` 是 `labgob` 在反序列化时对非零默认字段的告警，不影响选主正确性。

## 5. 遇到的问题与解决

- **选票分裂**：若所有节点使用相同超时，容易反复分裂。解决方式为在每个节点使用彼此独立的随机选举超时区间。
- **Term 同步**：在 RequestVote 与 AppendEntries 的处理中统一处理 `Term` 的比较与降级逻辑，确保任何更高任期的消息都能让节点退回 Follower。

## 6. 小结

Lab2A 完成了 Raft 最基础的选主能力，是整个共识算法的基石。后续 Lab2B/2C 将在本基础上扩展日志复制与持久化。
