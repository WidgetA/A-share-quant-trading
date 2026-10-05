# A 股二级行业与美股、韩股板块对应

基础文件为 [`src/data/reference/cross_market/industry_boards.json`](../src/data/reference/cross_market/industry_boards.json)。
包含申万 2021 版全部 **134 个二级行业**，每行分别记录美股与韩股的对应板块、业务范围差异及来源。
快照核对日期为 2026-10-05。开发基于已部署重构 v15 的 `refactor/cleanup-v15-only` / `921b273b971383ce98776119f5af5fd716bacca4`，
工作分支为 `feat/v15-cross-market-industries`。

## 分类来源

| 市场 | 基准及实际审阅目录 | 原始层级 |
| --- | --- | --- |
| A 股 | [Tushare 公开申万 2021 分类表](https://tushare.pro/document/2?doc_id=181)，保留其 134 个二级代码和原名 | L2 |
| 美股 | [Stock Analysis 行业目录](https://stockanalysis.com/stocks/industry/)，145 个业务行业 | industry |
| 韩股 | [NAVER 业种目录](https://stock.naver.com/api/domestic/market/upjong/list?startIdx=0&pageSize=100&sortType=changeRate)，79 项中的 78 个业务业种；排除混入基金产品的 `25 기타` | 업종 (upjong) |

海外保留供应商自身分类层级，未改称申万二级或 GICS 二级。美股表示美国上市市场，实际成员可含海外 ADR。
公开成员用于核对业务，不作为完整、实时成分股数据。目录、已见成员归属和公司经营资料是来源事实；跨市场关系是据这些事实作出的判断。

## 读取及字段

```python
import json
from pathlib import Path

reference = json.loads(
    Path("src/data/reference/cross_market/industry_boards.json").read_text(encoding="utf-8")
)
industries = {row["sw_code"]: row for row in reference["industries"]}
boards = {board["id"]: board for board in reference["boards"]}
semiconductors = industries["270100"]
```

`sw_code` 是申万二级分类代码。`board_id` 是本文件的命名空间标识：`US:SA:<真实目录 slug>` 或 `KR:NAVER:<真实业种编号>`，
用它关联 `boards` 中的来源原名、URL 与原始层级。它不是证券代码或行情代码。

每个市场结果包含 `status`、`matches`、`gap_note`、`exclusions` 和 `reviewed_catalog_source_id`。
`matches` 可以有多个板块，不强行一对一；每项的 `source_ids` 关联可复核来源，`scope_note` 说明实际重合和额外范围。

| `relation` | 相对申万二级业务范围的含义 |
| --- | --- |
| `same_business` | 主要行业业务范围同类；不承诺企业集合、制度或逐个业务纯粹等价 |
| `broader` | 海外类别包含目标业务，同时含其他范围 |
| `narrower` | 海外类别覆盖目标中的一部分业务 |
| `partial_overlap` | 只有部分业务相交，双方范围还有实质差异 |

`mapped` 表示已建立至少一个有范围依据的对应；`no_match_in_reviewed_catalog` 表示在所列目录及取得的经营证据中尚未确立对应，
不等于该市场没有相关企业。`unverified` 用于仍未核实的结果。无对应时保留空 `matches` 和具体原因，不以数据请求失败证明业务不存在。

`sources` 保存原始链接、核对时间、事实定位和支持的事实。`http` 来源保存实际取得原文字节的 SHA256；
`web_processed` 表示读取到公开网页处理文本，`raw_sha256` 为 `null`，不冒充取得了原始 HTTP 字节。
`provenance` 保存生产代码基线、真实 Kimi 模型及四批原稿摘要；最终数据已据原始证据复核并修正原稿。

## 已核对的边界

半导体设备类别实际含太阳能硅料企业，因此不能整体当成申万半导体的纯子集。
白酒与普通烈酒、城商行和农商行与海外区域银行，都保留业务或制度差异。
缺少同名专类的种子、动物疫苗、家电零部件等，只在取得实际经营与板块归属证据后建立局部对应。
材料按实际申万归属核对，碳纤维原丝与碳/碳复合材料不能仅凭“碳”字合并。

基础板块文件保持原始快照；后续指数取数使用
[`industry_indices.json`](../src/data/reference/cross_market/industry_indices.json)。
它保留全部 134 个二级行业及 US、KR 两个结果，记录 170 个去重行业指数代码
（美国 131 个、韩国 39 个）和 457 条有范围依据的关联。未建立已核指数对应的结果也保留，
美国 3 个、韩国 8 个；这表示本次已审来源中的缺口，不表示市场不存在相关企业。
新的指数证据可以补充基础板块文件中的空缺，两个文件通过基础文件 SHA256 关联。

`indices` 保存真实 Yahoo `symbol`、提供方名称、已见 Yahoo 名称别名、币种、交易所时区、
身份及数据能力证据。各行业的 `markets.US/KR.matches` 用 `index_id` 关联这些记录，
保留原板块标识、范围差异、提供方定义定位、实际成员业务证据及来源摘要。
定义、当前成员与继承的基础业务记录分别标明；取得的官方原文与网页处理文本不混称。
例如金融指数中的渔业控股公司仅提供该公司已证实业务的部分交集，
不把金融指数视为纯渔业指数，也不推导成分权重或价格相关性。

Yahoo 的可回补日线与当前快照分别记录为 `daily_history`、`snapshot_only`。
本次已采用 7 个有日线历史的美国指数，其余 163 个只取得当前快照；
试取的 `1mo`、`1y` 不是正式历史上限。首次正式取数用显式起止时间请求源端全部日线，
并检查实际返回粒度；`range=max&interval=1d` 曾实际返回月线，不能据请求参数认定日线。
韩国快照里的零开高低和成交量占位保持空值，未生成历史日线。

采集入口为 [`scripts/collect_cross_market_indices.py`](../scripts/collect_cross_market_indices.py)，
将真实行情存入 Greptime `cross_market_index_prices`，完整对应关系存入
`cross_market_industry_indices`。每个参考版本有美国、韩国各 134 行。
可先按二级行业读出 `mapping_json` 中的真实指数，再查询其行情：

```sql
SELECT mapping_json FROM cross_market_industry_indices
WHERE provider = 'yahoo' AND market = 'US' AND sw_code = '270100'
ORDER BY reference_at DESC LIMIT 1;

SELECT * FROM cross_market_index_prices
WHERE provider = 'yahoo' AND market = 'US' AND symbol = '^SOX'
ORDER BY ts DESC LIMIT 5;
```

`ts` 保存源时间，`fetched_at` 保存取回时间；`trade_date` 使用交易所本地日期。
快照、历史日线及来源缺口按实际内容保留，最新日线可能尚未最终确认。
生产运行、失败批次重放和部署版本核对见 [采集运行说明](cross-market-index-operations.md)。
没有增加 ETF、个股取数或其他分类层级的产品功能。
