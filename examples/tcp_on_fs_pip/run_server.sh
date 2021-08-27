#!/bin/sh

python kamui/tcp_on_fs/server.py \
    --workspace '/mnt/192_168_1_101/test/' \
    --proxy-address test_pip \
    --target-address mirrors.aliyun.com,80
