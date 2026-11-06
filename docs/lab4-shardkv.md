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
  每个分片的 `data` / `dupe` / `status` / `frozen`，以及 `pullFrom`（待拉取分片的
  数据源）、`deleteTo`（迁移完成后通知旧属主删除）。
- **分片状态**：`stServing`（可服务）/ `stPulling`（刚获得或回迁后待刷新）/
  `stPushing`（已移交、保留数据供新属主拉取）。`frozen[s]` 标记该副本是完整的
  冻结数据（而非空占位）。
- **配置轮询**：leader 逐个安装配置；仅当没有 `stPulling` 分片时才推进到下一
  配置（避免数据在迁移完成前被覆盖）。`stPushing` 不阻塞推进。
- **迁移**：采用拉取式。`migrator` 对 `stPulling` 分片向 `pullFrom` 记录的服务器
  发 `GetShard`，把拉到的数据经 Raft 以 `opShard` 提交；成功后向旧属主发
  `DeleteShard` 触发垃圾回收。拉取源/删除目标都存进复制状态，因此不会因为
  配置变化而「孤儿化」。
- **旧属主配合**：`GetShard` 只交出 `frozen=true` 且当前不在 `stServing` 的副本
  （`stPushing` 或 `stPulling`）；不比较跨组配置号，避免等待环。
- **回迁处理**：分片回到本组时置 `stPulling` 重新拉取，但保留原冻结副本，
  使较早配置下仍在拉取的组仍能从本组取得数据。

## 已修复的问题

1. **配置变更死锁**：原实现要求所有分片都 `stServing` 才能推进配置，而
   `stPushing` 分片依赖新属主的删除通知；通知一旦因配置推进而丢失，旧属主永远
   卡住，反过又使依赖它的拉取全部失败。修复：`ready` 只阻塞 `stPulling`；
   迁移源与删除目标改为按分片持久化；`GetShard`/`DeleteShard` 去掉跨组配置号比较。
2. **快照**：`pullFrom` / `deleteTo` / `frozen` 一并编入快照，重启后迁移状态可恢复。
3. **shardctrler 日志无界增长**：加入快照裁剪，`Query` 不再因 O(n²) `persist()`
   而超时（详见下文「本轮已合入的修复」）。

## 当前状态与已知局限

- **稳定通过（11/13）**：`TestStaticShards`、`TestJoinLeave`、`TestSnapshot`、
  `TestMissChange`、`TestConcurrent1`、`TestConcurrent2`、`TestUnreliable1`、
  `TestUnreliable2`、`TestUnreliable3`、`TestChallenge2Unaffected`、
  `TestChallenge2Partial`。
- **仍未通过（2/13）**：`TestConcurrent3`（极端连续重配置 + 全组重启下偶发挂起）、
  `TestChallenge1Delete`（状态大小约 204 KB，阈值 117 KB）。
- 结果仍有轻微随机性：不同随机种子下个别用例会互相消长，但 11 个通过是常态。

### 主要根因（已用调试日志定位到具体操作，现已修复）

此前失败表现统一为 `check()` 中 `Get(k)` 读到一个「前缀」值：初始 `Put` 的 20 字节在，
但紧接着的那次 `Append` 丢了。以 `TestMissChange` 的 key="2"（shard 0）为例的完整轨迹：

1. `Put(key2, seq=3)` 在 gid=100 / config 1 落库，值 `uPz3...HQm`。
2. shard 0 依次迁移到 gid=101(config 2)、gid=102(config 3)，值不变。
3. `Append(key2, seq=26)` 在 **gid=102 / config 5** 正确应用，
   值变为 `uPz3...HQmltGn2XOM0rZwk2WiSkDG`（日志确认 `dupe[0]=3`，`26>3` 已应用）。
4. **config 6 把 shard 0 又划回 gid=101**。gid=101 的 `stPushing` 副本是它在更早
   配置让出 shard 0 时冻结的旧值 `uPz3...HQm`（不含 Append）。`applyConfig` 的
   「回迁复用」分支直接把它置为 `stServing` 并对外服务，于是 `Get(2)` 读到旧值。

即：**分片在多个配置间来回迁移（bounce）时，`stPushing` 冻结副本会过期**。
修复方式是回迁后一律重新拉取，同时保留旧副本供较旧配置的拉取者使用
（`frozen` 标记），既不复用过期值、也不形成循环拉取。

### 本轮已合入的修复（6/13 → 11/13）

1. **`frozen` 按分片标记（复制 + 进快照）**：区分「完整冻结副本」与「空占位」。
   分片回迁时不再复用可能过期的 `stPushing` 副本，而是置 `stPulling` 重新拉取；
   但**保留旧副本且 `frozen` 仍为 true**，因此较早配置下仍在拉取该分片的组仍能从
   我们这里取到数据 —— 这一条同时消除了 stale read 与 A↔B 循环拉取死锁。
   空占位（`frozen=false`）永远不会被 `GetShard` 交出去。
2. **让出分片时不再强制置 `frozen=true`**：若 GC 已清空该副本，必须保持 false，
   否则会把空 map 当作完整分片交出去（这是 `TestConcurrent2` 丢 Append 的原因）。
3. **`GetShard` / `DeleteShard` 去掉跨组配置号比较**：各组推进速度不同，比较配置号
   会形成跨组等待环；改为只用分片状态（`frozen` / 是否 `stServing`）判定。
4. **客户端 Op 携带 `ConfigNum`**：applier 仅在「客户端配置比自己旧」时拒绝
   （`op.ConfigNum < kv.cfg.Num`）。只拒绝旧客户端可避免把写操作应用到即将让出的
   副本上；若服务端落后则仍可服务，避免 clerk 拿着新配置无限重试而挂起。
5. **shardctrler 增加快照**：`Query` 也走 Raft 并写入日志，长时间连续重配置会让
   日志无界增长、`persist()` 退化为 O(n²)，最终把 `Query` 拖到超时（`TestConcurrent3`
   里 clerk 全部卡在 `Query`）。加入 `ctrlerSnapshotThreshold`（4 KB）后在 applier
   中裁剪日志，`Query` 保持线性一致；shardctrler 两个用例仍全部通过。

### 仍被回退的尝试

| 尝试 | 结果 | 原因 |
|---|---|---|
| 去掉 `ready` 门（配置推进与拉取解耦） | livelock | 配置推进远快于拉取，分片被反复重分配 |
| `opShard` 增加严格配置号校验 | 拉取饥饿 | 拉取期间配置已推进，合法拉取被误丢弃 |
| 关闭 GC | stale read 增多 | 过期的冻结副本被长期保留并交出去 |
| `GetShard` 允许交出 `stServing` 副本 | 9/13（回归） | 把仍在被 clerk 写入的活动数据交出去，丢失写操作 |
| clerk 每次操作强制刷新配置 | 无改善 | 瓶颈不在 clerk 缓存配置 |

### 剩余两个用例的方向

- `TestConcurrent3`：12 秒内产生 15+ 次配置变更并反复全组重启，分片在两个组之间
  来回迁移的速度超过拉取速度。需要给每个分片记录「数据有效的截止配置号」
  `shardVer[s]`，回迁时按版本号判断是否可直接复用，从根源上避免反复拉取。
- `TestChallenge1Delete`：GC 目前只在单个新属主确认后删除，且 `pullFrom`/`deleteTo`
  仍占空间。需要按配置号串行化 GC，确认所有引用旧版本的拉取都完成后再删除。

CI 目前对 shardkv 做编译校验并运行 5 个最稳定用例作为门禁，另有两个
`continue-on-error` 的诊断步骤收集其余用例日志（含挂起用例，故设了较短超时）。
详见 `.github/workflows/ci.yml`。

## 其它说明

`shardctrler.Query` 保持走 Raft（一次提交），以保证读到的是已提交配置；
曾尝试改为本地读以减轻 shardctrler 日志压力，但会破坏
`TestMulti` 的「Check Same config on servers」一致性检查，故已回退。
