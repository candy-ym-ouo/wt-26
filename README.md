# TSDB

一个仅使用 Go 标准库实现的轻量级时序数据库，支持批量指标写入、UTC 时间分片、WAL 崩溃恢复、不可变段文件、区间查询、step 聚合、标签过滤和嵌入式 Web 控制台。

## 运行

```bash
go run ./cmd/server -data-dir ./data -listen :8080
```

浏览器访问 `http://localhost:8080/`。服务参数：

- `-shard-duration`：分片跨度，默认 `2h`
- `-retention`：数据保留时间，默认 `168h`
- `-flush-bytes`：内存表刷盘阈值，默认 `8388608`
- `-maintenance-interval`：后台维护周期，默认 `1m`
- `-wal-sync`：每次 WAL 写入是否 fsync，默认 `true`

## API 示例

```bash
now=$(($(date +%s) * 1000))
curl -X POST http://localhost:8080/api/v1/write \
  -H 'Content-Type: application/json' \
  -d "{\"metric\":\"cpu.usage\",\"tags\":{\"host\":\"web-01\"},\"points\":[{\"ts\":$now,\"value\":42.5}]}"

curl "http://localhost:8080/api/v1/query_range?metric=cpu.usage&start=$((now-60000))&end=$now"
```

## 验证和打包

```bash
make check
make package
```

`make package` 会在 `dist/` 中生成当前操作系统和架构对应的压缩发布包。
