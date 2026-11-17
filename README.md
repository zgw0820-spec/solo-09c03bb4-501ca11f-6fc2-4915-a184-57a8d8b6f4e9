# 机房设备故障日志只读查询 API

值班分析员共用的**只读 SQL 入口**：固定两条 PostgreSQL 连接，提交单条
`SELECT` 与**总截止时间**，排队等待也计入预算；超时立即取消数据库语句，
确认事务结束才复用连接，无法确认则弃置连接并补建；迟到结果不会改写超时
回执。**会话级设置不会跨请求泄漏**：每次归还连接前都会比对连接建立时的
会话快照，发现 `search_path` 等被改动就复位，复位无法确认则弃置补建。
后续查询始终拿到属于自己的结果。

## 技术栈

- FastAPI（Swagger UI：`/docs`，ReDoc：`/redoc`）
- psycopg 3（异步连接 + libpq `PQcancel`）
- PostgreSQL 16
- Docker Compose 编排，预置 7 条故障日志与只读角色 `fault_ro`

## 快速开始（Docker Compose）

```bash
docker compose up --build
# API:    http://localhost:8000
# Swagger: http://localhost:8000/docs
```

自测（另开终端，容器内或本机均可）：

```bash
docker compose exec api python -m tests.selftest
```

## 接口

### `POST /query`

```json
{
  "sql": "SELECT id, device_id, severity FROM fault_logs LIMIT 5",
  "timeout_ms": 2000
}
```

`timeout_ms` 是**包含排队时间**的总预算（单调时钟），从请求进入即计时。

| 场景 | HTTP | 回执要点 |
| --- | --- | --- |
| 成功 | 200 | `status=ok`，`rows` 为本次查询自身结果，附 `wait_ms`/`execution_ms` |
| 执行超时 | 504 | `error.code=query_timeout`，`status=timeout`，`cancelled`、`connection_discarded`、`late_result_ignored` |
| 排队期间预算耗尽 | 503 | `error.code=acquire_timeout`，语句**从未发送**到数据库 |
| SQL 不合法 | 400 | `error.code=invalid_sql` |

### `GET /health`

返回连接池容量、空闲/占用数、累计创建与弃置数。会话清理成功不会增加
`total_discarded`；只有连接被弃置补建时该计数才增长。

## 关键设计

### 1. 容量恒为 2，排队计入总预算

`QueryConnectionPool` 用容量为 2 的信号量限制**物理后端数**，第三个请求
排队等待而非新开连接。`acquire(deadline)` 接收绝对单调时钟截止时间，
等待、会话校验与执行共用同一预算；排队期间过期、或在取得/校验连接期间
过期，都直接返回 503，不发送任何语句，也不泄漏借出槽位。

### 2. 单条只读 SELECT 的三层防护

1. `app/validation.py` 仅允许单条 `SELECT`/`WITH`/`TABLE`/`VALUES`，拒绝
   多语句（额外分号）、写关键字、`SELECT ... INTO` 等。
2. 每个请求在显式 `BEGIN READ ONLY` 事务中执行，会话默认
   `default_transaction_read_only=on`，并设置局部 `statement_timeout` 兜底。
3. 数据库角色 `fault_ro` 仅有 `SELECT` 权限，且角色默认只读；直连也无法
   写入（`InsufficientPrivilege`）。

### 3. 超时处理协议（`app/executor.py`）

1. 语句作为 **shielded task** 运行，超时回执一旦确定就不会被迟到结果覆盖。
2. 截止时间到，调用 libpq `PQcancel`（经独立取消连接）请求服务器中止语句。
3. `await` 该 shielded 任务直至真正结束（收到 `QueryCanceled` 或拿到迟到
   结果），杜绝协程残留。
4. 执行 `ROLLBACK` 并核对 `transaction_status == IDLE`；再执行会话快照
   校验/复位（见第 5 节），两项都通过才把连接放回池；无法确认则关闭弃置
   并补建，保持容量为 2。
5. 迟到完成的结果只做审计日志，回执固定为 504 且不携带 `rows`。

### 4. 连接级故障的弃置补建与无感重试

后端被带外终止（如 `pg_terminate_backend`）时，连接级错误触发弃置并由池
补建；只读语句在同一总预算内安全重试一次，客户端无感知。若补建暂时失败，
池临时缩水并在下一次获取时懒建补回，不会死锁。

### 5. 会话状态隔离（修复跨请求污染）

`SELECT` 本身是只读事务，但 `set_config(name, value, false)` 修改的是
**会话级** GUC，`COMMIT` 之后依然生效，事务空闲也不会清除。因此一条看似
无害的 `SELECT set_config('search_path','pg_catalog',false)` 会让下一位
分析员的 `fault_logs` 报 `UndefinedTable`。处理方式：

1. 连接建立时记录会话基线快照：当前角色、`pg_settings.source='session'`
   的全部覆盖项、以及会话级 advisory 锁数量。
2. 任何归还路径（正常成功、语句错误回滚、超时确认 IDLE）都先执行
   `sanitize()`：处于 IDLE 且快照完全一致则直接复用；有漂移则执行
   `DISCARD ALL` 并重新应用池默认值（只读、read committed），再复验
   快照与 IDLE。
3. 探测或复位失败（例如连探测都被残留的 `statement_timeout=1ms` 打断，
   或 `DISCARD ALL` 报错）时**不把连接交出去**，而是关闭弃置并补建，
   池容量保持 2。
4. 借出时再做一次防护性校验；`prepare_threshold=None` 避免
   `DISCARD ALL` 释放预编译语句后出现 `InvalidSqlStatementName`。

普通查询不会被误伤：无漂移时只在借出与归还时各多一次轻量快照查询，
连接照常复用，不产生弃置。

## 无 Docker 的本地运行与自测

本机若无容器运行时，可用 `pgserver`（PyPI 上的免 root PostgreSQL 16）：

```bash
pip install -r requirements.txt pgserver
python tests/local_run.py        # 拉起 PG16 + uvicorn
python -m tests.selftest         # 端到端自测（56 项）
python tests/test_protocol.py    # 超时/迟到结果/排队/会话隔离协议直测（34 项）
```

自测覆盖：普通查询与接续结果、只读约束、执行超时与取消、排队过期不执行、
容量 2、带外终止后的弃置补建、以及**两条连接被 `set_config` 污染后仍返回
自身结果且不牺牲健康连接**（以及复位失败时弃置补建）的回归用例。

## 目录结构

```
app/            FastAPI 应用（config/pool/executor/validation/schemas/main）
db/01_init.sql  建表、预置日志、只读角色（Compose 首次初始化执行）
tests/          selftest.py（HTTP 端到端）、test_protocol.py（协议直测）、
                local_run.py（免 Docker 启动器）
Dockerfile / docker-compose.yml / requirements.txt
```
