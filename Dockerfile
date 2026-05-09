ARG GO_IMAGE=golang:1.26.1-alpine3.22
FROM --platform=$BUILDPLATFORM ${GO_IMAGE} AS compile

ARG TARGETOS=linux
ARG TARGETARCH
ARG SERVICE
ARG VERSION=dev
ARG GOPROXY=https://goproxy.cn,direct
ARG CGOENABLED=0

WORKDIR /build

# 挂载依赖缓存
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    go mod download -x

COPY . .

RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    GOOS=$TARGETOS GOARCH=$TARGETARCH CGO_ENABLED=$CGOENABLED \
    go build -ldflags="-s -w -X main.Version=$VERSION" \
    -o /app/service ./cmd/server/main.go

# 最终镜像部分
FROM alpine:3.22 AS final

# 安装必要的依赖
RUN apk add --no-cache libc6-compat

# 创建非root用户
RUN addgroup -g 1000 appuser && adduser -u 1000 -G appuser -D appuser
WORKDIR /app

# 复制可执行文件
COPY --from=compile --chown=appuser:appuser /app/service /app/

# 切换到非root用户
USER appuser

# 设置环境变量
ENV CONFIG_PATH=/app/configs/config.yaml

# 运行微服务的命令
ENTRYPOINT ["/app/service"]
