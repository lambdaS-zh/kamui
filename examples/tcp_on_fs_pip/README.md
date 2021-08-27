# tcp_on_fs_pip示例

## 说明
此示例演示通过tcp_on_fs代理进行pip安装eventlet的方法

```
              http                    |                     http
[pip install] ---> [client_proxy] "test_pip" [server_proxy] ---> [aliyun-pypi-repo]
                   127.0.0.1:8080     |
```

* 由于此TCP代理不包含处理DNS，且http hosts头部会影响服务器是否接收请求，所以需要先在客户端机器上配置hosts，使得mirrors.aliyun.com指向127.0.0.1，并在pip参数地址中使用此域名而非127.0.0.1
* 请勿在同一台机器上通过本地文件系统来同时构建客户端和服务端代理，由于上一步中配置了hosts，这样做会导致代理成环发起请求
* 由于使用一般OS文件API实现，所以需要确保共享目录能直接访问读写。即，对于需要权限登录的共享文件夹，在windows上需要先通过资源管理器进行登录，在linux上需要直接登录挂载到一般目录
