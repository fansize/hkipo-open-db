# HK IPO Backend

一个用于抓取香港 IPO 数据并生成 SQLite 数据库的轻量项目。

项目支持两种运行模式：

- 默认模式：本地抓取并更新仓库内的 `db/ipo_data.db`
- `--daily` 模式：为 GitHub Actions 准备，抓取后将数据库写入临时目录，供 Release 上传使用

## 环境要求

- Python 3.13+
- [uv](https://docs.astral.sh/uv/)
- 可选：Jina API Key


## 用法命令

安装依赖：

```bash
uv sync
```

本地更新数据库：

```bash
uv run main.py
```

模拟 GitHub Actions 模式：

```bash
uv run main.py --daily
```
