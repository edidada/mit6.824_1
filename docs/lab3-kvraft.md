# 实验报告：Lab3 — 基于 Raft 的容错 KV 服务 (kvraft)

## 目标

在 Lab2 的 Raft 之上实现一个线性一致的键值服务，支持 `Get` / `Put` / `Append`，
能容忍少数派服务器宕机、重启、网络不可靠，以及在日志过大时通过快照裁剪。

## 设计与实现

- **所有操作走 Raft**：`Get` / `PutAppend` 的 handler 都调用 `rf.Start(op)`，
  通过 `applyCh` 等待日志被提交并应用到状态机后才回复客户端，保证线性一致性。
- **等待机制**：每个请求按 Raft 返回的 `index` 注册一个带缓冲的 channel，
  `applier` 应用到该 index 时把结果送回；同时用定时器 + 周期性的
  `GetState()` 任期检查，在失联/失去 leadership 时快速返回 `ErrWrongLeader`，
  避免分区中的旧 leader 让请求永久挂起。
- **去重**：按 `(ClientId, Seq)` 记录每个客户端已应用的最大序号，避免
  `Append` 在重试时被重复追加。`Get` 幂等，不做去重。
- **快照 (3B)**：当 `persister.RaftStateSize() > maxraftstate` 时，
  把 `data`、`dupe`、`lastApplied` 编码后调用 `rf.Snapshot(index, ...)`；
  重启时从 `persister.ReadSnapshot()` 恢复，并处理 `applyCh` 上的
  `SnapshotValid` 消息。

## 关键问题与修复

1. **`Get` 对不存在的键返回 `ErrNoKey` 导致客户端死循环**：改为返回 `OK` + 空串
   （clerk 也兼容 `ErrNoKey`）。
2. **速度测试 `TestSpeed3A` 超慢（~100ms/op）**：根因是 Raft leader 只在
   100ms 心跳 tick 上复制日志。给 Raft 增加 `replicateCh` 唤醒通道，
   `Start()` 追加日志后立即触发一次复制，使单条操作在一个 RTT 内提交。
3. **分区测试中请求永久挂起**：`submit` 增加超时与任期检查，旧 leader
   无法提交时及时返回 `ErrWrongLeader`，让 clerk 换 leader 重试。

## 测试结果

`go test ./kvraft -count=1`：3A 全部 14 个用例 + 3B 全部 9 个用例
（含 `TestSnapshotUnreliableRecoverConcurrentPartitionLinearizable3B`）全部通过。

Lab2 Raft 在加入 `replicateCh` 后回归通过全部 2A–2D 用例。
