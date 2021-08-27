import sys
from getopt import getopt

from kamui.base_stream import BaseClient
from kamui.tcp_on_fs.io import FsTcpTunnelIO
from kamui.proxy_runner import ClientWorkspaceProcess


DEFAULT_IOPS = 10
DEFAULT_TIME_SLICE_INTERVAL = 10
DEFAULT_WORKSPACE = './_workspace'


class FsTcpTunnelClient(BaseClient):
    IO = FsTcpTunnelIO


class FsTcpTunnelClientWorkspaceProcess(ClientWorkspaceProcess):
    PROXY_CLIENT = FsTcpTunnelClient


def run(iops,
        listen_address, proxy_address,
        time_slice_interval, workspace):
    pr = FsTcpTunnelClientWorkspaceProcess({
        'iops':                     int(iops),
        'listen_address':           tuple(listen_address),
        'proxy_address':            proxy_address,
        'time_slice_interval':      int(time_slice_interval),
        'workspace':                workspace,
    })
    pr.run()


def usage():
    sys.stderr.writelines([
        'Usage: %s <ARGS> \n' % sys.argv[0],
        '  <ARGS> can be: \n',
        '  --iops <NUMBER>                  (OPTIONAL,DEFAULT=%s) max iops for disk reading and writing.\n' % DEFAULT_IOPS,
        '  --listen-address <IP>,<PORT>     local TCP address.\n',
        '  --proxy-address <ADDR>           abstract proxy address. This should be the same at both proxy sides.\n',
        '  --time-slice-interval <NUMBER>   (OPTIONAL,DEFAULT=%s) msecs between each process working frames.\n' % DEFAULT_TIME_SLICE_INTERVAL,
        '  --workspace <PATH>               (OPTIONAL,DEFAULT=%s) workspace dir. This should be the same at both proxy sides.\n' % DEFAULT_WORKSPACE,
        '\n',
        ])


def parse_and_run():
    pairs, _ = getopt(sys.argv[1:], 'h', [
        'iops=',
        'listen-address=',
        'proxy-address=',
        'time-slice-interval=',
        'workspace=',
        'help',
    ])

    la, pa, query = None, None, False
    iops, ti, ws = DEFAULT_IOPS, DEFAULT_TIME_SLICE_INTERVAL, DEFAULT_WORKSPACE
    for arg, value in pairs:
        if '--iops' == arg:
            iops = int(value)
        elif '--listen-address' == arg:
            la = value.split(',')
            la[1] = int(la[1])
        elif '--proxy-address' == arg:
            pa = value
        elif '--time-slice-interval' == arg:
            ti = int(value)
        elif '--workspace' == arg:
            ws = value
        elif arg in ('-h', '--help'):
            query = True

    if query or None in (la, pa):
        usage()
        sys.exit(1)

    run(iops, la, pa, ti, ws)


if __name__ == '__main__':
    parse_and_run()
