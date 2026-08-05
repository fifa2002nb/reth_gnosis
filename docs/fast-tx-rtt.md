# fast_tx peer-echo RTT 监控

在 `arb_sendRawTransactionFast` 直发交易后，观测每个 peer 从收到 tx 到把它 announce 回来的**应用层往返延迟**（下文简称 echo RTT）。基础问题：devp2p 的 `Transactions` 消息**协议层没有 ACK**——没有「这笔 tx 到达对端的确凿时间」。本模块用一个可靠的代理：peer 收到后大概率会把 hash 再 gossip 回来，第一条 echo 到达的时刻就是「这个 peer 已收到并处理完」的下界。

3 分钟版：读 §1、§3、§5。写监控脚本前读 §6。

---

## 1. TL;DR — 出什么、看什么

启动 reth 时加 `--metrics 0.0.0.0:9001`。相关指标全部落到该端点，前缀 `arb_fast_tx_`。

| 你想回答的问题 | 看哪个 |
|---|---|
| 每个 peer 有多快 | `arb_fast_tx_echo_rtt_us` histogram（§3.1） |
| 哪些 peer 是「死 peer / 长期不回声」 | `arb_fast_tx_echo_total{outcome="timeout"}` counter（§3.2） |
| 我发出去的 (tx × peer) 总量 | `arb_fast_tx_echo_total` 全部相加（§3.3） |
| 单笔 tx 的完整链路 | `RUST_LOG=fast_tx::rtt=debug` 日志（§3.4） |

**没有值 = 没订阅者 / 没 fast_tx 广播**。指标只在 `arb_sendRawTransactionFast` 至少被调过一次、且节点连上至少一个 peer 时才会有数据。

---

## 2. 数据流总览

```
              ┌────────────────────────────────────────────────────────────┐
              │ fifa2002nb/reth @ v2.2.0-arb-fast-tx-rtt (fork of v2.2.0)  │
              │                                                            │
              │  TransactionsManager                                       │
              │    ├── on_network_tx_event(IncomingTransactions/…Hashes)   │
              │    │     └─► notify_incoming(IncomingTxEvent)              │
              │    └── incoming_listeners: Vec<UnboundedSender<_>>         │
              │              ▲                                             │
              │              │ subscribe_incoming() 注入                    │
              └──────────────┼─────────────────────────────────────────────┘
                             │
              ┌──────────────┴─────────────────────────────────────────────┐
              │ reth_gnosis::fast_tx                                       │
              │                                                            │
              │  send_raw_transaction_fast(bytes)                          │
              │    └─► for peer in active_peers:                           │
              │           PENDING.insert((hash, peer), Instant::now())     │
              │           network.send_transactions(peer, ...)             │
              │                                                            │
              │  spawn_rtt_tasks (启动时装一次)                             │
              │    ├── subscriber:  rx.recv() ─► on_incoming_event() ─┐    │
              │    │                                                  │    │
              │    └── GC (100 ms tick): 扫过期条目 ─► timeout 出账    │    │
              │                                                       ▼    │
              │  metrics::histogram!/counter!  ─► reth --metrics :9001     │
              └────────────────────────────────────────────────────────────┘
```

关键约束：subscriber 只把「原始 wire 事件」当观测点，**不改动 pool 导入路径**——fast_tx「不入池」的语义被完整保留。

代码地图：

| 位置 | 干什么 |
|---|---|
| `crates/net/network/src/transactions/mod.rs`（在 fork 里） | `IncomingTxEvent` 枚举 + `TransactionsHandle::subscribe_incoming()` + manager 端 fan-out |
| `src/fast_tx.rs::PENDING` | 已发未回声条目 `(hash, peer) → Instant` |
| `src/fast_tx.rs::spawn_rtt_tasks` | subscriber + GC，在 `set_fast_tx_handles` 时启动 |
| `src/fast_tx.rs::on_incoming_event` | 命中 PENDING 就出账 echo、记 metrics + 日志 |
| `src/fast_tx.rs::gc_expired` | 100 ms 扫一次，超过 `RTT_TIMEOUT` 的记 timeout |
| `Cargo.toml` 的 `reth-* = { git = …fifa2002nb/reth, rev = "…" }` | 通过 rev pin 拉 fork 分支的对应 commit，本仓 build 时自动 fetch |

---

## 3. 指标目录

`peer` 标签 = peer_id 的前 12 位 hex（B512 全串 128 字符做 label 会把 Prometheus 撑爆；前 12 位在同一节点的活跃 peer 集合内足以唯一）。`kind` 标签 = `"full"`（peer 用 `Transactions` 消息回声）或 `"hashes"`（peer 用 `NewPooledTransactionHashes` 回声，套利场景绝大多数是这种）。

### 3.1 `arb_fast_tx_echo_rtt_us` — Histogram

**含义**：每个 peer 第一条 echo 相对发送时刻的延迟，单位微秒。

自动展开为 3 个 Prometheus 序列：

| 序列 | 用途 |
|---|---|
| `arb_fast_tx_echo_rtt_us_bucket` | 分桶累积；配 `histogram_quantile()` 出分位数 |
| `arb_fast_tx_echo_rtt_us_sum` | RTT 累积和（画平均） |
| `arb_fast_tx_echo_rtt_us_count` | echo 到的次数（= `_total{outcome="echoed"}` 求和） |

**标签**：`peer`, `kind`。

> Peer 端优化：`Transactions` 只在 peer 刻意重复广播时才会出现（罕见），生产上关注 `kind="hashes"`。

### 3.2 `arb_fast_tx_echo_total` — Counter

**含义**：每一次 (tx, peer) 广播的最终结局计数。

**标签**：
- `peer`：同上；
- `outcome`：
  - `"echoed"`：`RTT_TIMEOUT` (800 ms) 内看到了 echo。此时**额外带** `kind` 标签；
  - `"timeout"`：超期没等到 echo，被 GC 出账。**不带** `kind`（GC 时无法知道 kind，可以理解为「本来预期任一种 kind 的回声都可以，都没等到」）。

### 3.3 派生量（Prometheus 无独立序列，靠查询组合）

- **每秒发出多少 (tx × peer) 对** = `sum(rate(arb_fast_tx_echo_total[1m]))`——所有 outcome 加总；
- **每笔 tx 的 fanout** = 上式 ÷ 每秒 `arb_sendRawTransactionFast` 调用数（后者需你在 caller 端另打点或从 jsonrpsee 层拿）。

### 3.4 结构化日志（Loki / grep）

Target：`fast_tx::rtt`；开关：`RUST_LOG=fast_tx::rtt=debug`。逐笔 echo 出一条：

```text
peer=a1b2c3d4e5f6 hash=0xdeadbeef… kind=hashes rtt_us=32567 msg="peer echo"
```

用于**归因单笔 tx**：已知 hash 想复盘哪些 peer 回、多久回。**超时 peer 不打这条日志**——超时的记账只出现在 Prometheus counter 里。想要日志级别的 timeout 也能看到，可以自己在 `gc_expired()` 里加一条 `tracing::debug!`（见 §9）。

---

## 4. 启动方式

reth_gnosis 沿用 reth 原生的 metrics 开关：

```bash
./reth node \
  --chain gnosis \
  ...其他常规参数 \
  --metrics 0.0.0.0:9001                       # ← 打开这个
```

冒烟测试：

```bash
# 1. 端点有响应
curl -s http://localhost:9001/metrics | head -5

# 2. arb 指标是否出现（发过一笔 fast tx 之后）
curl -s http://localhost:9001/metrics | grep '^arb_fast_tx'

# 3. 看单个 peer 的桶累积
curl -s http://localhost:9001/metrics \
  | grep 'arb_fast_tx_echo_rtt_us_bucket' | head -20
```

如果 `arb_fast_tx_*` 一条都没有，见 §8「无数据」。

---

## 5. PromQL 手册

拷贝可用，参数可改。默认时窗 1 min，追踪长期看板改成 5 min / 15 min。

```promql
### p99 echo RTT（微秒）by peer
histogram_quantile(0.99,
  sum by (le, peer) (rate(arb_fast_tx_echo_rtt_us_bucket{kind="hashes"}[1m])))

### 中位数 echo RTT by peer
histogram_quantile(0.50,
  sum by (le, peer) (rate(arb_fast_tx_echo_rtt_us_bucket{kind="hashes"}[1m])))

### 全网 p99（不分 peer，反映端到端下限）
histogram_quantile(0.99,
  sum by (le) (rate(arb_fast_tx_echo_rtt_us_bucket{kind="hashes"}[1m])))

### 每个 peer 的 timeout 比例（识别死 peer 的核心指标）
sum by (peer) (rate(arb_fast_tx_echo_total{outcome="timeout"}[5m]))
/ ignoring(outcome) group_left
  sum by (peer) (rate(arb_fast_tx_echo_total[5m]))

### 全网 timeout 率（异常时 spike）
sum(rate(arb_fast_tx_echo_total{outcome="timeout"}[1m]))
/ sum(rate(arb_fast_tx_echo_total[1m]))

### 每个 peer 的活跃度（每分钟 echo 次数）
sum by (peer) (rate(arb_fast_tx_echo_total{outcome="echoed"}[1m])) * 60

### Peer 按 p50 从低到高排序（找最快 peer）
sort(histogram_quantile(0.50,
  sum by (le, peer) (rate(arb_fast_tx_echo_rtt_us_bucket{kind="hashes"}[5m]))))

### 当前活跃 peer 数（过去 1 min 有过 echo 的 peer 计数）
count(count by (peer) (rate(arb_fast_tx_echo_total{outcome="echoed"}[1m]) > 0))
```

---

## 6. 监控脚本编写规范 ⭐

> **这一节是你写任何后续脚本前应该先读的。**

三种消费模式，选一个匹配的场景。

### 6.1 场景一：直接抓 `/metrics` 端点（一次性 / 冒烟 / CI）

Prometheus text format 语义稳定，无外部依赖，脚本一次性拿快照。

```bash
#!/usr/bin/env bash
# check_fast_tx_rtt.sh — 快速看当前状态
set -euo pipefail
ENDPOINT="${METRICS_URL:-http://localhost:9001/metrics}"

echo "== echo counts =="
curl -sf "$ENDPOINT" | grep '^arb_fast_tx_echo_total{' | sort

echo
echo "== RTT sum / count (µs, sample average by peer, all kinds) =="
paste \
  <(curl -sf "$ENDPOINT" | grep '^arb_fast_tx_echo_rtt_us_sum{'   | sort) \
  <(curl -sf "$ENDPOINT" | grep '^arb_fast_tx_echo_rtt_us_count{' | sort)
```

约定：
- 脚本必须走环境变量 `METRICS_URL`，允许远程节点复用；
- 不要写死 `localhost` 或端口；
- 单次拉取，不要在 shell 循环里反复 `curl` ——用 Prometheus 抓取。

### 6.2 场景二：Prometheus + Alertmanager（生产告警）

**放到 Prometheus rules，不要在应用侧做告警**——避免每个消费者各自实现阈值判断。示例 rules（`fast_tx_alerts.yaml`）：

```yaml
groups:
- name: fast_tx_rtt
  interval: 30s
  rules:
  # 单个 peer 长时间不回声：连续 5 min timeout > 50%
  - alert: FastTxPeerSilent
    expr: |
      sum by (peer) (rate(arb_fast_tx_echo_total{outcome="timeout"}[5m]))
      / ignoring(outcome) group_left
        sum by (peer) (rate(arb_fast_tx_echo_total[5m]))
      > 0.5
    for: 5m
    labels: { severity: warning }
    annotations:
      summary: "peer {{ $labels.peer }} echoes < 50% of fast tx broadcasts"
      description: "考虑在 caller 端剔除该 peer；持续 5 分钟"

  # 全网 timeout 率飙升：网络出问题或大批 peer 断连
  - alert: FastTxNetworkDegraded
    expr: |
      sum(rate(arb_fast_tx_echo_total{outcome="timeout"}[1m]))
      / sum(rate(arb_fast_tx_echo_total[1m]))
      > 0.3
    for: 3m
    labels: { severity: critical }
    annotations:
      summary: "fast tx 全网 timeout > 30%"

  # p99 突增
  - alert: FastTxLatencyRegression
    expr: |
      histogram_quantile(0.99,
        sum by (le) (rate(arb_fast_tx_echo_rtt_us_bucket{kind="hashes"}[5m])))
      > 200000
    for: 10m
    labels: { severity: warning }
    annotations:
      summary: "fast tx 全网 p99 echo RTT > 200 ms 持续 10 min"
```

### 6.3 场景三：caller 端实时 peer 排序（Python 拉 Prometheus HTTP API）

套利 caller 想据 RTT 排序、剔除慢 peer，用 Prometheus HTTP API 定期拉聚合值。**不要**直接消费 `/metrics` raw text——Prometheus 已经帮你做过时间窗聚合。

```python
# rank_peers.py — 拉 p50 排名，打印可用于 caller 端 peer 白名单
import requests, json, time, os, sys

PROM = os.environ.get("PROM_URL", "http://prom.internal:9090")

QUERY = (
    'histogram_quantile(0.50, sum by (le, peer) ('
    'rate(arb_fast_tx_echo_rtt_us_bucket{kind="hashes"}[5m])))'
)

def snapshot() -> dict[str, float]:
    r = requests.get(f"{PROM}/api/v1/query", params={"query": QUERY}, timeout=5)
    r.raise_for_status()
    return {
        item["metric"]["peer"]: float(item["value"][1])
        for item in r.json()["data"]["result"]
        if float(item["value"][1]) > 0
    }

if __name__ == "__main__":
    rttp50 = snapshot()
    # 只保留 p50 < 100 ms 的 peer 作为「优质 peer 白名单」
    keep = {p: us for p, us in rttp50.items() if us < 100_000}
    print(json.dumps(dict(sorted(keep.items(), key=lambda kv: kv[1])), indent=2))
```

编写约定：
- `PROM_URL` 走环境变量；
- 查询用 `[5m]` 时窗聚合，别用 `[10s]`——`arb_sendRawTransactionFast` 调用频率有限，短窗会剧烈抖动；
- 结果集里 peer 是**前 12 位 hex**（不是完整 enode）；caller 端要匹配的话保存完整 peer_id → 前缀的映射，或者用 reth 的 `admin_peers` RPC 逐条对照；
- 每次拉快照后先过阈值再排序，避免长尾 peer 排在最前但样本 <10；
- 建议**定时拉 + 缓存**，不要每笔套利决策都同步查一次。

### 6.4 不推荐的做法（写下以免踩坑）

- ❌ **不要 tail reth 日志 grep `fast_tx::rtt`**：日志开销大、结构容易变、timeout 事件没有对应日志行；
- ❌ **不要用 `arb_fast_tx_echo_rtt_us_count` 减去上一秒值算 QPS**：Prometheus 客户端库允许 counter 重置（进程重启后归零），用 `rate()`；
- ❌ **不要把 peer_id 前 12 位当稳定 ID 存长时间**：peer 断连重连后 peer_id 完全不变，但前 12 位是 hex，**极小概率**碰撞。想长期归因就存完整 peer_id（`admin_peers` 里拿）；
- ❌ **不要在 alertmanager 之外多处做同一份阈值判断**：告警去重是 Prometheus 的事，caller 端只做数据消费。

---

## 7. Grafana 面板建议

一个 dashboard，5 个 panel 就够看。放到 `docs/dashboards/` 或 Grafana 侧的 JSON 存档。

| Panel | 类型 | Query（关键片段） |
|---|---|---|
| **RTT p99 by peer** | Time series | `histogram_quantile(0.99, sum by (le, peer) (rate(arb_fast_tx_echo_rtt_us_bucket{kind="hashes"}[1m])))`，legend `{{peer}}`，Y 轴 log µs |
| **Timeout rate by peer** | Time series | §5 里的 timeout 比例查询 |
| **活跃 peer 数** | Stat | §5 里的「当前活跃 peer 数」 |
| **Peer 排行榜** | Table，按 p50 升序 | §5 里的 sort 查询，Value 显示 µs |
| **全网 timeout 率** | Gauge，阈值 30%/50% | §5 里的「全网 timeout 率」 |

---

## 8. 排查

**症状：`/metrics` 里根本看不到 `arb_fast_tx_*` 前缀**
- 节点还没被调用过 `arb_sendRawTransactionFast`？发一笔看看。
- 启动日志有 `peer-echo RTT subscriber started` 吗？没有→fork 版没生效，检查 `cargo tree | grep reth-network` 应该显示 `github.com/fifa2002nb/reth` 而不是 `paradigmxyz/reth`。
- 有 `subscribe_incoming failed` warn？说明 TransactionsManager 已经关掉，节点自身状态有问题。

**症状：所有 peer 都 `outcome="timeout"`，没有 echoed**
- 检查 `tx_gossip_disabled` 是否被开了（`fast_tx` 里有 warn）；
- 检查 peer 数：`get_active_peers` 是否返回空？可能是 discv4/discv5 没起来；
- Gnosis chain 特殊：如果发的 tx 用了不合规 nonce / gasPrice，peer 收到就丢，永远不会 re-announce——这不是「网络问题」而是 tx 本身对端不接受。

**症状：p99 长期 > 500 ms**
- 检查节点入口带宽 / CPU；
- 用 `sort by p50` 查询定位是不是少数长尾 peer 拖高全网；
- 视情况把 `RTT_TIMEOUT` 从 800 ms 调高（见 §9），否则慢 peer 会被误记 timeout。

**症状：`_bucket` 总数远小于 `_total` echoed 计数**
- Prometheus 桶配置错误或版本不一致；`metrics` crate v0.24 用默认桶。如果 caller 想要更细的桶，得在 metrics-exporter 层重配（reth 侧 `reth-metrics` 的 recorder 决定）。

**症状：`cargo build` 报 `failed to authenticate` 拉不到 reth**
- `fifa2002nb/reth` fork 是 private 且当前机器没有 GitHub 凭据。把 fork 改成 public（推荐）或在 `~/.cargo/config.toml` 里设 `[net] git-fetch-with-cli = true` 让 cargo 用系统 git（前提是 git 已经能访问 private repo）。

---

## 9. 调参 / 二次开发

**常量位置**：`src/fast_tx.rs`

```rust
const RTT_TIMEOUT: Duration = Duration::from_millis(800);
const RTT_GC_INTERVAL: Duration = Duration::from_millis(100);
```

调整原则：
- 生产上 `RTT_TIMEOUT` 覆盖 p99 + 若干 σ 即可；把它调到 2 s 会让 timeout counter 变懒，掩盖真实的死 peer；
- `RTT_GC_INTERVAL` 决定 timeout 出账的最大延迟，减小会加压 CPU（100 条 entry 内可忽略）；

**想加 timeout 日志**：在 `gc_expired()` 里 `map.remove(&key)` 命中分支加一条 `tracing::debug!(target: "fast_tx::rtt", peer=..., hash=..., "timeout")`。

**想暴露 caller-side RPC**：定义 `arb_getPeerRttStats` 返回 `HashMap<PeerId, PeerRttStat>`，在 `on_incoming_event` / `gc_expired` 里同时更新一个进程内的 EWMA 结构。Caller 就不用绕 Prometheus。

**想改 peer label 长度**：`format_peer` 里的 `s[..12]` 常量。**注意向下兼容**——改了以后老 Grafana query / alertmanager rule 会因 label 值不同而丢历史序列（Prometheus 视其为新序列）。

**上游 reth 版本升级**：见 [README#Upgrading reth](../README.md#upgrading-reth) —— 把 fork 的 `v2.2.0-arb-fast-tx-rtt` 分支 rebase 到新 tag、push、更新本仓 Cargo.toml 里的 `rev`。

---

## 10. 关联

- 交易发送路径：`src/fast_tx.rs`（本模块）
- 网络初始化：`src/network.rs::build_network` 里调用 `set_fast_tx_handles`
- 上游 fork：[fifa2002nb/reth @ v2.2.0-arb-fast-tx-rtt](https://github.com/fifa2002nb/reth/tree/v2.2.0-arb-fast-tx-rtt)（Cargo.toml 里 pin 到具体 rev）
