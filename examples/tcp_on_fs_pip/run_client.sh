#!/bin/sh

python kamui/tcp_on_fs/client.py \
    --listen-address 127.0.0.1,8080 \
    --workspace '\\192.168.1.101\test\' \
    --proxy-address test_pip
