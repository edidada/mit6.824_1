# 实验报告：Lab4 — 分片 KV 服务 (shardctrler / shardkv)

## 目标

- **4A shardctrler**：用 Raft 实现分片配置控制器，支持 `Join` / `Leave` /
  `Move` / `Query`，按配置号维护分片到复制组的映射，并保证重分配尽量均衡、
  迁移量最小。
- **4B shardkv**：多个复制组，各组独立跑 Raft；客户端先向 shardctrler 查配置，
  再把请求发到负责该分片的组；配置变更时分片数据在组间迁移，配合快照裁剪日志。

## 设计与实现

### shardctrler（4A，已完成并全部通过）

- `Join` / `Leave` / `Move` / `Query` 都作为 Op 走 Raft，顺序应用；
  `configs []Config` 按配置号索引。
- 重均衡：统计各 gid 持有的分片数，把无主分片优先分给负载最小的组，
  再反复把分片从负载最大组移到最小组，直到 `max-min <= 1`；
  `Move` 直接指定分片归属。
- 用 `(ClientId, Seq)` 去重，避免 clerk 重试导致配置号重复递增；
  深拷贝 `Config`（尤其是 `Groups` map）避免历史配置被后续修改污染。

### shardkv（4B，部分完成）

- **状态机（全部随 Raft 复制并进快照）**：当前配置 `cfg`、上一份配置、
  每个分片的 `data` / `dupe` / `status`，以及 `pullFrom`（待拉取分片的数据源）、
  `deleteTo`（迁移完成后通知旧属主删除）。
- **分片状态**：`stServing`（可服务）/ `stPulling`（刚获得、待拉数据）/
  `stPushing`（已移交、保留数据供新属主拉取）。
- **配置轮询**：leader 逐个安装配置；仅当没有 `stPulling` 分片时才推进到下一
  配置（避免数据在迁移完成前被覆盖）。
- **迁移**：采用拉取式。`migrator` 对 `stPulling` 分片向 `pullFrom` 记录的服务器
  发 `GetShard`，把拉到的数据经 Raft 以 `opShard` 提交；成功后向旧属主发
  `DeleteShard` 触发垃圾回收。拉取源/删除目标都存进复制状态，因此不会因为
  配置变化而「孤儿化」。
- **旧属主配合**：`GetShard` 只在 `stPushing` 且已安装请求方配置、且不再持有该
  分片时提供数据，避免把空的占位 map 交给新属主造成数据丢失。

## 已修复的问题

1. **配置变更死锁**：原实现要求所有分片都 `stServing` 才能推进配置，而
   `stPushing` 分片依赖新属主的删除通知；通知一旦因配置推进而丢失，旧属主永远
   卡住，反过又使依赖它的拉取全部失败。修复：`ready` 只阻塞 `stPulling`；
   迁移源与删除目标改为按分片持久化；`GetShard` 放宽对旧属主配置号的要求；
   分片「回迁」时若仍持有 `stPushing` 数据则直接复用，不再重新拉取。
2. **快照**：`pullFrom` / `deleteTo` 一并编入快照，重启后迁移状态可恢复。

## 当前状态与已知局限

- **稳定通过**：`TestStaticShards`、`TestJoinLeave`、`TestConcurrent1`、
  `TestChallenge2Unaffected`、`TestChallenge2Partial`（`TestSnapshot` 多数通过）。
- **尚未稳定**：`TestMissChange`、`TestConcurrent2/3`、`TestUnreliable2/3`、
  `TestChallenge1Delete`。在「配置快速连续变更 / 分片来回迁移」时，客户端缓存的
  旧配置与服务端迁移时序之间存在竞态，个别 `Get` 可能读到缺少最近一次 `Append`
  的旧值；`Challenge1Delete` 的垃圾回收还不够彻底，状态大小超出阈值。
- 这部分是 6.5840 中最难的实验，需要更严格的「按配置号串行化迁移 + 线性一致的
  分片所有权判定」才能覆盖全部挑战用例。CI 目前只对 shardkv 做编译校验并运行
  上述稳定子集，详见 `.github/workflows/ci.yml`。

## 其它说明

`shardctrler.Query` 保持走 Raft（一次提交），以保证读到的是已提交配置；
曾尝试改为本地读以减轻 shardctrler 日志压力，但会破坏
`TestMulti` 的「Check Same config on servers」一致性检查，故已回退。
