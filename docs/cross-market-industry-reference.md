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

数据只交付二级行业与行业板块对应，不包含 ETF、指数、价格序列或其他层级的完整目录。
对应关系本身不证明价格相关性。此次工作未发布生产服务。
