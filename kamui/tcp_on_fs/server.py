import sys
from getopt import getopt

from kamui.base_stream import BaseServer
from kamui.tcp_on_fs.io import FsTcpTunnelIO
from kamui.proxy_runner import ServerWorkspaceProcess


DEFAULT_IOPS = 10
DEFAULT_TIME_SLICE_INTERVAL = 10
DEFAULT_WORKSPACE = './_workspace'


class FsTcpTunnelServer(BaseServer):
    IO = FsTcpTunnelIO


class FsTcpTunnelServerWorkspaceProcess(ServerWorkspaceProcess):
    PROXY_SERVER = FsTcpTunnelServer


def run(iops,
        proxy_address, target_address,
        time_slice_interval, workspace):
    pr = FsTcpTunnelServerWorkspaceProcess({
        'iops':                     int(iops),
        'proxy_address':            proxy_address,
        'target_address':           tuple(target_address),
        'time_slice_interval':      int(time_slice_interval),
        'workspace':                workspace,
    })
    pr.run()


def usage():
    sys.stderr.writelines([
        'Usage: %s <ARGS> \n' % sys.argv[0],
        '  <ARGS> can be: \n',
        '  --iops <NUMBER>                  (OPTIONAL,DEFAULT=%s) max iops for disk reading and writing.\n' % DEFAULT_IOPS,
        '  --proxy-address <ADDR>           abstract proxy address. This should be the same at both proxy sides.\n',
        '  --target-address <DOMAIN>,<PORT> target TCP address. DOMAIN can be IP.\n',
        '  --time-slice-interval <NUMBER>   (OPTIONAL,DEFAULT=%s) msecs between each process working frames.\n' % DEFAULT_TIME_SLICE_INTERVAL,
        '  --workspace <PATH>               (OPTIONAL,DEFAULT=%s) workspace dir. This should be the same at both proxy sides.\n' % DEFAULT_WORKSPACE,
        '\n',
    ])


def parse_and_run():
    pairs, _ = getopt(sys.argv[1:], 'h', [
        'iops=',
        'proxy-address=',
        'target-address=',
        'time-slice-interval=',
        'workspace=',
        'help',
    ])

    pa, ta, query = None, None, False
    iops, ti, ws = DEFAULT_IOPS, DEFAULT_TIME_SLICE_INTERVAL, DEFAULT_WORKSPACE
    for arg, value in pairs:
        if '--iops' == arg:
            iops = int(value)
        elif '--proxy-address' == arg:
            pa = value
        elif '--target-address' == arg:
            ta = value.split(',')
            ta[1] = int(ta[1])
        elif '--time-slice-interval' == arg:
            ti = int(value)
        elif '--workspace' == arg:
            ws = value
        elif arg in ('-h', '--help'):
            query = True

    if query or None in (pa, ta):
        usage()
        sys.exit(1)

    run(iops, pa, ta, ti, ws)


if __name__ == '__main__':
    parse_and_run()
