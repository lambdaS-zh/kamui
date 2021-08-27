from collections import namedtuple
from multiprocessing import (
    Process,
)
from os.path import realpath
from queue import Queue
from socket import (
    create_connection,
    socket,
    AF_INET,
    SHUT_WR,
    SOCK_STREAM,
)
from threading import (
    Thread,
)
from time import (
    monotonic,
    sleep,
)

from kamui.base_stream import (
    BaseClient,
    BaseServer,
    BlockingOperation,
    EAgain,
)
from kamui.logging import get_logger


LOG = get_logger(__name__)


ClientWorkspaceConfig = namedtuple('ClientWorkspaceConfig', (
    'iops',
    'listen_address',
    'proxy_address',
    'time_slice_interval',
    'workspace',
))


ServerWorkspaceConfig = namedtuple('ServerWorkspaceConfig', (
    'iops',
    'proxy_address',
    'target_address',
    'time_slice_interval',
    'workspace',
))


def _check_tcp_address(address):
    if not isinstance(address, (list, tuple)) or len(address) != 2:
        raise ValueError('Invalid tcp-address: ' + str(address))


def _check_proxy_address(address):
    if not isinstance(address, str):
        raise ValueError('Invalid proxy-address: ' + str(address))


class _SimplexChannelThread(Thread):

    def __init__(self, q_, addr_in, conn_in, addr_out, conn_out, *args, **kwargs):
        super(_SimplexChannelThread, self).__init__(*args, **kwargs)
        self._q = q_  # type: Queue
        self._addr_in = addr_in
        self._conn_in = conn_in
        self._addr_out = addr_out
        self._conn_out = conn_out

    @staticmethod
    def _long_op(func, *args, **kwargs):
        try:
            return func(*args, **kwargs)
        except BlockingOperation as ex:
            retring = ex.retrying
            while True:
                try:
                    return retring()
                except EAgain:
                    # TODO: set timeout and customized interval
                    sleep(0.01)
                    continue

    def run(self):
        try:
            while True:
                data = self._long_op(self._conn_in.recv, 4096)
                if not data:
                    # read EOF
                    LOG.debug('%s -> %s: EOF' % (self._addr_in, self._addr_out))
                    self._long_op(self._conn_out.shutdown, SHUT_WR)
                    break
                self._long_op(self._conn_out.sendall, data)
        except ConnectionAbortedError:
            LOG.error('connection aborted by accident.')
        finally:
            self._q.put(1, block=False)
            if self._q.full():
                LOG.debug('channel [%s -- %s] closed' % (self._addr_in, self._addr_out))
                self._long_op(self._conn_out.close)
                self._long_op(self._conn_in.close)


class ClientWorkspaceProcess(Process):

    PROXY_CLIENT = BaseClient

    @staticmethod
    def parse_config(dict_data):
        iops = dict_data.get('iops', 3)

        la = dict_data.get('listen_address')
        _check_tcp_address(la)

        pa = dict_data.get('proxy_address')
        _check_proxy_address(pa)

        ts = dict_data.get('time_slice_interval', 1)
        if ts < 1:
            ts = 1
        ts /= 1000  # ms -> s

        ws = dict_data.get('workspace')
        if not ws:
            ws = './_workspace'

        return ClientWorkspaceConfig(
            iops=iops,
            listen_address=la,
            proxy_address=pa,
            time_slice_interval=ts,
            workspace=realpath(ws)
        )

    def __init__(self, client_proxy_config, *args, **kwargs):
        super(ClientWorkspaceProcess, self).__init__(*args, **kwargs)
        self._config = self.parse_config(client_proxy_config)

    def _proxy_connect(self, timeout_msec=30000):
        proxy_client = self.PROXY_CLIENT(self._config.workspace)
        try:
            conn = proxy_client.connect(self._config.proxy_address)
        except BlockingOperation as ex:
            retrying = ex.retrying
            interval = self._config.time_slice_interval
            start_time = monotonic() * 1000
            while True:
                try:
                    return retrying()
                except EAgain:
                    if monotonic() * 1000 - start_time > timeout_msec:
                        raise TimeoutError()
                    sleep(interval)
                    continue
        else:
            return conn

    def run(self):
        LOG.info('--- client running ---')
        LOG.info('config: ' + str(self._config))
        self.PROXY_CLIENT.IO.set_iops(self._config.iops)

        la = self._config.listen_address
        pa = self._config.proxy_address

        in_tcp = socket(AF_INET, SOCK_STREAM)
        in_tcp.bind(la)
        in_tcp.listen(128)
        try:
            while True:
                conn_tcp, addr_tcp = in_tcp.accept()
                LOG.info('received a tcp connection on %s, forwarding to proxy %s' % (la, pa))
                conn_proxy = self._proxy_connect()
                q_ = Queue(2)
                c2s_thr = _SimplexChannelThread(q_, la, conn_tcp, pa, conn_proxy)
                s2c_thr = _SimplexChannelThread(q_, pa, conn_proxy, la, conn_tcp)
                c2s_thr.setDaemon(True)
                s2c_thr.setDaemon(True)
                c2s_thr.start()
                s2c_thr.start()
        finally:
            in_tcp.close()


class ServerWorkspaceProcess(Process):

    PROXY_SERVER = BaseServer

    @staticmethod
    def parse_config(dict_data):
        iops = dict_data.get('iops', 3)

        pa = dict_data.get('proxy_address')
        _check_proxy_address(pa)

        ta = dict_data.get('target_address')
        _check_tcp_address(ta)

        ts = dict_data.get('time_slice_interval', 1)
        if ts < 1:
            ts = 1
        ts /= 1000  # ms -> s

        ws = dict_data.get('workspace')
        if not ws:
            ws = './_workspace'

        return ServerWorkspaceConfig(
            iops=iops,
            proxy_address=pa,
            target_address=ta,
            time_slice_interval=ts,
            workspace=realpath(ws)
        )

    def __init__(self, server_proxy_config, *args, **kwargs):
        super(ServerWorkspaceProcess, self).__init__(*args, **kwargs)
        self._config = self.parse_config(server_proxy_config)

    def _proxy_accept(self, proxy_server):
        interval = self._config.time_slice_interval
        while True:
            try:
                return proxy_server.accept()
            except EAgain:
                sleep(interval)
                continue

    def run(self):
        LOG.info('--- server running ---')
        LOG.info('config: ' + str(self._config))
        self.PROXY_SERVER.IO.set_iops(self._config.iops)

        pa = self._config.proxy_address
        ta = self._config.target_address

        proxy_server = self.PROXY_SERVER(self._config.workspace)
        proxy_server.listen(pa)

        while True:
            conn_proxy = self._proxy_accept(proxy_server)
            LOG.info('received a proxy connection on %s, forwarding to tcp %s' % (pa, ta))
            conn_tcp = create_connection(ta)
            q_ = Queue(2)
            c2s_thr = _SimplexChannelThread(q_, pa, conn_proxy, ta, conn_tcp)
            s2c_thr = _SimplexChannelThread(q_, ta, conn_tcp, pa, conn_proxy)
            c2s_thr.setDaemon(True)
            s2c_thr.setDaemon(True)
            c2s_thr.start()
            s2c_thr.start()
