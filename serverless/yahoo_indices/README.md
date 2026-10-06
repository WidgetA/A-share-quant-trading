# 美国 FC 行业指数取数端

`ashare_yahoo_indices_v15` 是美国 `us-west-1` 的独立原生 Python 3.12 函数，入口为 `handler.handler`。每次同步调用只请求一个行业指数及其时间窗口，直接使用 `YahooIndexClient(proxy=None)` 访问 Yahoo。它返回完整原始 JSON，国内采集器负责解析、pending、映射、Greptime 写入和回读。部署不使用新 Docker 镜像，也不启动或修改现有训练函数。

[`s.yaml`](s.yaml) 定义 `timeout: 360`、`instanceConcurrency: 1`、`internetAccess: true`、`memorySize: 512`、`diskSize: 512` 和 `cpu: 0.35`。原生事件调用不创建公开 HTTP 触发器。国内运行方式见 [`docs/cross-market-index-operations.md`](../../docs/cross-market-index-operations.md)。

## ZIP 内容

ZIP 根目录必须直接包含 `handler.py` 和依赖包，不能多包一层目录：

```text
handler.py                         # 本目录 handler.py
src/__init__.py                    # 空文件
src/data/__init__.py               # 空文件
src/data/yahoo_indices.py
src/data/fc_yahoo_worker.py
httpx/                            # requirements.txt 的依赖安装结果
httpcore/
tzdata/
...                               # 其余依赖及 metadata
```

仅打包上述取数模块，不复制项目完整 `src/__init__.py` 或安装整个项目；项目整体 Python 版本要求与这个独立 Python 3.12 函数包不同。函数依赖固定在 [`requirements.txt`](requirements.txt)：`httpx==0.28.1`、`tzdata==2026.2`。使用 Linux Python 3.12 的依赖；不带入 Windows 虚拟环境、`.pyd`、`.dll`、凭证、训练模块、数据库配置或本地采集状态。

在已有 Linux Python 3.12 构建环境、工作树根目录，可按下列方式生成独立 ZIP。Windows 侧构建产物使用 `D:/CodexBuild/cross-market-fc-v15`；工作树读取与编辑使用迁移后的 `Z:/Project/Python/A-share-quant-trading-v15-industry-map`。

```sh
build_root=$(mktemp -d)
package="$build_root/package"
mkdir -p "$package/src/data"
cp serverless/yahoo_indices/handler.py "$package/handler.py"
cp src/data/yahoo_indices.py src/data/fc_yahoo_worker.py "$package/src/data/"
: > "$package/src/__init__.py"
: > "$package/src/data/__init__.py"
python3.12 -m pip install --only-binary=:all: --target "$package" -r serverless/yahoo_indices/requirements.txt
PACKAGE_DIR="$package" ZIP_PATH="$build_root/yahoo_indices.zip" python3.12 - <<'PY'
import os
from pathlib import Path
from zipfile import ZIP_DEFLATED, ZipFile

package = Path(os.environ["PACKAGE_DIR"])
with ZipFile(os.environ["ZIP_PATH"], "w", ZIP_DEFLATED) as archive:
    for path in sorted(package.rglob("*")):
        if path.is_file() and "__pycache__" not in path.parts and path.suffix != ".pyc":
            archive.write(path, path.relative_to(package).as_posix())
PY
sha256sum "$build_root/yahoo_indices.zip"
```

保留 ZIP SHA256 与其中源文件摘要。部署后从 FC 下载代码包核对实际 ZIP 字节；成功上传或函数配置受理不等于实际运行包已一致。

## 部署目标与凭证

Serverless Devs 使用本目录 `s.yaml`。部署环境变量为：

```dotenv
CROSS_MARKET_FC_ACCESS=<既有训练AccessKey配置对应的Serverless Devs凭证别名>
CROSS_MARKET_FC_REGION=us-west-1
CROSS_MARKET_FC_FUNCTION_NAME=ashare_yahoo_indices_v15
CROSS_MARKET_FC_CODE_ZIP=<构建ZIP的绝对路径>
```

在已有授权的部署环境中执行 `s deploy -t serverless/yahoo_indices/s.yaml`，或者使用官方 FC3 SDK 将同一 ZIP 设置为该独立函数的 `code.zipFile`。两种方式均使用既有训练 AccessKey ID/Secret，仅目标函数不同，不触发训练，不更改训练的区域、镜像、资源配置或调度。若通过 API 创建函数，应显式设置 `diskSize: 512`，同时保留上述原生运行时与入口参数。

`FC_*` 是平台保留前缀，不能写入函数自定义 `environmentVariables`；worker 读取平台内置 `os.environ["FC_REGION"]`，不由部署传入该变量。`CROSS_MARKET_FC_REGION` 是部署端变量，二者用途不同。[FC 官方环境变量说明](https://www.alibabacloud.com/help/en/functioncompute/environment-variables)

签名凭证留在部署端和国内采集器，函数代码包中不含 AccessKey。国内使用 [`deploy/cross-market/requirements-fc.txt`](../../deploy/cross-market/requirements-fc.txt) 的官方 FC SDK `4.8.2`，其 Linux CPython 3.13 vendor 包与这个 Python 3.12 取数 ZIP 分开构建。

## 同步事件契约

原生 Python handler 接收 JSON 的事件 bytes，返回 UTF-8 JSON 字符串；不存在 HTTP 触发器的 `body/statusCode` 包装。[FC 官方事件 handler 说明](https://www.alibabacloud.com/help/en/functioncompute/event-handlers-1-1)

请求六个字段必须齐全：

```json
{
  "schema_version": 1,
  "request_id": "collection-request-id",
  "symbol": "^SOX",
  "market": "US",
  "start": null,
  "capability": "daily_history"
}
```

`market` 为 `US` 或 `KR`；`capability` 为 `daily_history` 或 `snapshot_only`；`start` 为 `null` 或非负整数秒。`null` 对应源端完整可用历史起点，不设固定一个月业务范围。Yahoo 请求使用显式 `period1/period2` 和 `interval=1d`，并验证实际返回粒度；`range=max` 可能被源端转为月线，不能只按请求参数声称拿到日线。

响应完整回显请求，并追加下列字段：

```json
{
  "schema_version": 1,
  "request_id": "collection-request-id",
  "symbol": "^SOX",
  "market": "US",
  "start": null,
  "capability": "daily_history",
  "fetched_at": 1791294600000,
  "raw_json": "<完整原始Yahoo JSON文本>",
  "payload_sha256": "<原始UTF-8响应字节SHA256>",
  "request_url": "<实际Yahoo请求URL>",
  "runtime": {
    "region": "us-west-1",
    "fc_request_id": "<平台context.request_id>"
  }
}
```

上例为字段示意，不是实测行情。响应不携带展开的 `points` 数组，不截断历史或原始 JSON，不把 ETF、个股或宽基指数替换成行业指数。worker 验证请求、原始 SHA 与 INDEX 身份，现有 Yahoo 客户端同时验证市场、时区和观测。快照保留源端实际值和源时钟，不能把单个快照写成历史日线。

源错误、身份错误、SHA 不一致和坏事件均抛异常，由原生 FC 运行时报告函数失败，不返回带错误字段的成功行情包。国内适配器先检查实际 SDK HTTP 状态和 `x-fc-error`/`x-fc-error-type`，完整读取 BinaryIO 响应流，再核对契约、运行区域、源 SHA 和本地解析。HTTP 200 本身不能建立成功。

同一函数实例保留专属事件循环与 Yahoo 客户端，避免连续调用重建连接或丢失该实例的 429 冷却；`instanceConcurrency: 1` 对应一个实例一个同步事件。冷却不跨实例共享、不持久化。国内当前并发为 2，有限重试使用同次请求 ID；函数内没有持续采集循环，也不依赖本地 checkpoint。

## 已有实测与后续验收

2026-10-06 [真实部署与调用记录](../../dev-tools/cross-market-yahoo/production_probe/fc_worker_deployment.json) 已确认：

| 请求 | 源观测 | 原始响应字节 | FC 响应字节 | 源 SHA256 |
| --- | ---: | ---: | ---: | --- |
| `^SOX`，US，完整日线 | 8161 | 817629 | 818245 | 一致 |
| `^BKX`，US，完整日线 | 8464 | 868605 | 869221 | 一致 |
| `KOSPI-10.KS`，KR，快照 | 1 | 1262 | 1879 | 一致 |

三项均为同步调用 HTTP 200、无函数错误、`instrumentType=INDEX`，运行区域为 `us-west-1`。日线实际 `dataGranularity=1d`，请求 `start:null`。[云端代码包读回](../../dev-tools/cross-market-yahoo/production_probe/fc_cloud_artifact_readback.json) 确认 ZIP 字节与上传包一致，SHA256 为 `7102f6a499e447a6aa23acc2d3c00d4ee19bc161a064f225ac3b1967012a8866`，原训练函数未变。记录中的响应长度证明这些实际完整响应可回传，不据此推断未验证的统一响应大小上限。

行为检查可运行 `python -m pytest tests/unit/test_fc_yahoo_worker.py tests/unit/test_fc_yahoo_indices.py -q`；新构建包的实际云端运行、身份、源时钟、完整响应与摘要仍需核查。上述三项出口实测不代表国内 FC 路线全部 170 指数、268 映射行、Greptime 逐字段读回、连续轮次和重启验收已经完成。
