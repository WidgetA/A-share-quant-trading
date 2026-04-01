# 沧海数据 (tsanghi) API 参考

> 官方文档: https://tsanghi.com/doc
> Base URL: `https://tsanghi.com/api/fin/stock`
> 认证: 所有请求需携带 `token` 参数
> Token 存储: `data/tsanghi_token.txt`

## 通用约定

- 交易所代码: `XSHG`(上交所), `XSHE`(深交所)
- 股票代码: 6位数字, 如 `600519`
- 日期格式: `yyyy-mm-dd`
- **Volume 单位: 手 (1手=100股)**，系统内统一用股，需在读取时 ×100
- 响应格式: `{"code": 200, "msg": "success", "data": [...]}`
- code≠200 表示错误, data=null 时表示无数据

---

## 日线行情 `/daily`

### 单只股票历史日线

**端点**: `GET /stock/{exchange}/daily`

| 参数 | 类型 | 必选 | 说明 |
|------|------|------|------|
| token | string | 必选 | API Token |
| ticker | string | 必选 | 股票代码 |
| start_date | date | 可选 | 起始日期 |
| end_date | date | 可选 | 终止日期 |
| order | int | 可选 | 0=不排序, 1=升序, 2=降序 |

**Response**: `ticker, date, open, high, low, close, volume`

### 全市场某日日线（批量）

**端点**: `GET /stock/{exchange}/daily/latest`

| 参数 | 类型 | 必选 | 说明 |
|------|------|------|------|
| token | string | 必选 | API Token |
| date | date | 可选 | 指定日期，默认最新交易日 |

**Response**: 同上，返回该交易日全部股票

**停牌股**: open/close 为 null，需调用方自行处理

---

## 历史5分钟行情 `/5min`

**端点**: `GET /stock/{exchange}/5min`

### Request 参数

| 参数 | 类型 | 必选 | 说明 |
|------|------|------|------|
| token | string | 必选 | API Token |
| ticker | string | 必选 | 股票代码, e.g. 600519 |
| start_date | date | 可选 | "yyyy-mm-dd", 默认最近一个月 |
| end_date | date | 可选 | "yyyy-mm-dd", 默认最新日期 |
| limit | int | 可选 | 最大 100000, **默认 1000** |
| order | int | 可选 | 0=不排序, 1=升序, 2=降序, 默认 0 |
| columns | string | 可选 | 自定义输出字段, 逗号分隔 |
| fmt | string | 可选 | json (默认) 或 csv |

**关键限制**:
- `start_date` 到 `end_date` 最大跨度 **1年**
- 默认 `limit=1000`，一只股票一天 48 根 bar，**必须设 `limit=100000`**
- 更新时间: 收盘后 3~4 小时，每天更新

### Response 字段

| 字段 | 类型 | 说明 |
|------|------|------|
| ticker | string | 股票代码 |
| date | datetime | **"yyyy-mm-dd hh:mm:ss"**（日期+时间合并） |
| open | float | 开盘价 |
| high | float | 最高价 |
| low | float | 最低价 |
| close | float | 收盘价 |
| volume | float | 成交量（**手**, 需 ×100 转股） |
| amount | float | 成交额（默认不输出, 需 `columns` 指定） |
| pre_close | float | 昨收价（默认不输出, 需 `columns` 指定） |

### 5min bar 时间标签约定

A股交易时段 5min bar 对应:
```
09:35 → 09:30~09:35 (第1根)
09:40 → 09:35~09:40 (第2根)
...
11:30 → 11:25~11:30 (上午最后)
13:05 → 13:00~13:05 (下午第1根)
...
15:00 → 14:55~15:00 (收盘bar)
```

### 系统使用方式

V15 策略只需 **9:40 快照**（前 2 根 bar: 09:35 + 09:40）:
- `close_940` = 09:40 bar 的 close
- `cum_volume` = (09:35.volume + 09:40.volume) × 100
- `max_high` = max(09:35.high, 09:40.high)
- `min_low` = min(09:35.low, 09:40.low)

聚合后存入 GreptimeDB `backtest_minute` 表。

---

## 其他端点

### 股本信息 `/shares`

**端点**: `GET /stock/{exchange}/shares?ticker=xxx`

### 公司信息 `/company/info`

**端点**: `GET /stock/{exchange}/company/info?ticker=xxx`
