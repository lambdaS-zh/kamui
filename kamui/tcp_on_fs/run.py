import sys

from kamui.base_stream import (
    BaseClient,
    BaseServer,
)
from kamui.tcp_on_fs.io import FsTcpTunnelIO
from kamui.proxy_runner import (
    ClientWorkspaceProcess,
    ServerWorkspaceProcess,
)


class FsTcpTunnelClient(BaseClient):
    IO = FsTcpTunnelIO


class FsTcpTunnelServer(BaseServer):
    IO = FsTcpTunnelIO


class FsTcpTunnelClientWorkspaceProcess(ClientWorkspaceProcess):
    PROXY_CLIENT = FsTcpTunnelClient


class FsTcpTunnelServerWorkspaceProcess(ServerWorkspaceProcess):
    PROXY_SERVER = FsTcpTunnelServer


def run_server():
    pr = FsTcpTunnelServerWorkspaceProcess({
        'iops':                     10,
        'proxy_address':            'test.com',
        'target_address':           ('127.0.0.1', 8090),
        'time_slice_interval':      10,
        'workspace':                './_workspace',
    })
    pr.run()


def run_client():
    pr = FsTcpTunnelClientWorkspaceProcess({
        'iops':                     10,
        'listen_address':           ('127.0.0.1', 8088),
        'proxy_address':            'test.com',
        'time_slice_interval':      10,
        'workspace':                './_workspace',
    })
    pr.run()


if __name__ == '__main__':
    run_type = sys.argv[1]
    assert run_type in ('server', 'client')
    if run_type == 'server':
        run_server()
    else:
        run_client()
