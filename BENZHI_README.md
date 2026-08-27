基于 Go 实现的时序数据库 Web 项目，一款后端服务，处理指标批量写入、持久化恢复与范围查询。

# TSDB 评测打包说明

## 构建

```bash
go build ./...
./build_benzhi_docker.sh tsdb linux/arm64
./build_benzhi_docker.sh tsdb linux/amd64
```

## 运行

```bash
go run ./cmd/server -data-dir ./data -listen :8080
```

服务启动后访问 `http://localhost:8080/`。可使用 `POST /api/v1/write` 写入指标，通过查询接口读取时序数据。

## 测试

```bash
go test ./...
go test -race ./...
go vet ./...
```

## 容器内验证

```bash
docker run -it tsdb:latest
go build ./...
go test ./...
```

镜像保留完整 Go 工具链，并在构建阶段下载模块和预编译项目，供容器内离线编译与测试使用。
