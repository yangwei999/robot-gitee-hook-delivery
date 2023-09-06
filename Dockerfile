FROM openeuler/openeuler:23.03 as BUILDER
RUN dnf update -y && \
    dnf install -y golang && \
    go env -w GOPROXY=https://goproxy.cn,direct

MAINTAINER zengchen1024<chenzeng765@gmail.com>

# build binary
WORKDIR /go/src/github.com/opensourceways/robot-gitee-hook-delivery
COPY . .
RUN GO111MODULE=on CGO_ENABLED=0 go build -a -o robot-gitee-hook-delivery .

# copy binary config and utils
FROM openeuler/openeuler:23.03
RUN dnf -y update && \
    dnf in -y shadow && \
    groupadd -g 1000 delivery && \
    useradd -u 1000 -g delivery -s /bin/bash -m delivery

USER delivery

COPY  --chown=delivery --from=BUILDER /go/src/github.com/opensourceways/robot-gitee-hook-delivery/robot-gitee-hook-delivery /opt/app/robot-gitee-hook-delivery

ENTRYPOINT ["/opt/app/robot-gitee-hook-delivery"]
