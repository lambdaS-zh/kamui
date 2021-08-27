# tcp_on_fs

## 说明
在共享文件夹（文件系统）中代理TCP。

## 命令用法
使用命令前，需要先确保此项目源码目录已被安装到python包目录下，然后通过`python -m kamui.tcp_on_fs.client`的形式来调用；或者在此项目根目录下以`python kamui/tcp_on_fs/client.py`的形式来直接调用。

<br/>客户端
`client.py <ARGS>`
* `--iops IOPS数`                  可省略，默认10，限制对IO层的读写频繁度，数字越高，代理性能越好，但对共享目录的压力也越高
* `--listen-address IP,端口`       客户代理端的TCP监听地址，客户端通过访问此地址来代理数据
* `--proxy-address 代理地址`        两个代理端通过使用共同的地址来识别构建成同一个代理通道，是一个抽象名字地址
* `--time-slice-interval 毫秒间隔`  可省略，默认10，工作进程的响应时间片间隔
* `--workspace 工作目录路径`        两个代理端在共享文件夹中的共同工作目录路径，两端需要使用相同的此路径，否则无法构建代理

<br/>服务端
`server.py <ARGS>`
* `--iops IOPS数`                  可省略，默认10，限制对IO层的读写频繁度，数字越高，代理性能越好，但对共享目录的压力也越高
* `--proxy-address 代理地址`        两个代理端通过使用共同的地址来识别构建成同一个代理通道，是一个抽象名字地址
* `--target-address 域名,端口`       目标服务端的TCP地址，其中域名可以直接使用IP
* `--time-slice-interval 毫秒间隔`  可省略，默认10，工作进程的响应时间片间隔
* `--workspace 工作目录路径`        两个代理端在共享文件夹中的共同工作目录路径，两端需要使用相同的此路径，否则无法构建代理

可参照examples/tcp_on_fs_pip示例
