from collections import namedtuple
from multiprocessing import (
    Process,
)
from threading import (
    Thread,
)
from socket import (
    create_connection,
    create_server,
    SHUT_WR,
)
from time import sleep

from kamui.base_stream import (
    BaseClient,
    BaseServer,
    BlockingOperation,
    EAgain,
)


ClientWorkshopConfig = namedtuple('ClientWorkshopConfig', (
    'listen_address',
    'proxy_address',
    'time_slice_interval',
    'workspace',
))


ServerWorkshopConfig = namedtuple('ServerWorkshopConfig', (
    'proxy_address',
    'target_address',
    'time_slice_interval',
    'workspace',
))


class ProxyThread(Thread):

    def __init__(self, conn_in, conn_out, *args, **kwargs):
        super(ProxyThread, self).__init__(*args, **kwargs)
        self._conn_in = conn_in
        self._conn_out = conn_out

    def run(self):
        # TODO: close the fd
        while True:
            data = self._conn_in.recv()
            if not data:
                # read EOF
                self._conn_out.shutdown(SHUT_WR)
                break
            self._conn_out.sendall(data)


class ClientWorkshopProcess(Process):

    PROXY_CLIENT = BaseClient

    @staticmethod
    def parse_config(raw) -> ClientWorkshopConfig:
        # TODO
        pass

    def __init__(self, client_proxy_config, *args, **kwargs):
        super(ClientWorkshopProcess, self).__init__(*args, **kwargs)
        self._config = self.parse_config(client_proxy_config)

    def _proxy_connect(self):
        # TODO: set timeout
        proxy_client = self.PROXY_CLIENT(self._config.workspace)
        try:
            conn = proxy_client.connect(self._config.proxy_address)
        except BlockingOperation as ex:
            retrying = ex.retrying
            interval = self._config.time_slice_interval
            while True:
                try:
                    return retrying()
                except EAgain:
                    sleep(interval)
                    continue
        else:
            return conn

    def run(self):
        with create_server(self._config.listen_address) as in_tcp:
            while True:
                conn_tcp, addr_tcp = in_tcp.accept()
                conn_proxy = self._proxy_connect()
                c2s_thr = ProxyThread(conn_tcp, conn_proxy)
                s2c_thr = ProxyThread(conn_proxy, conn_tcp)
                c2s_thr.setDaemon(True)
                s2c_thr.setDaemon(True)
                c2s_thr.start()
                s2c_thr.start()


class ServerWorkshopProcess(Process):

    PROXY_SERVER = BaseServer

    @staticmethod
    def parse_config(raw) -> ServerWorkshopConfig:
        # TODO
        pass

    def __init__(self, server_proxy_config, *args, **kwargs):
        super(ServerWorkshopProcess, self).__init__(*args, **kwargs)
        self._config = self.parse_config(server_proxy_config)

    def _proxy_accept(self):
        # TODO: set timeout
        proxy_server = self.PROXY_SERVER(self._config.workspace)
        interval = self._config.time_slice_interval
        while True:
            try:
                return proxy_server.accept()
            except EAgain:
                sleep(interval)
                continue

    def run(self):
        while True:
            conn_proxy = self._proxy_accept()
            conn_tcp = create_connection(self._config.target_address)
            c2s_thr = ProxyThread(conn_proxy, conn_tcp)
            s2c_thr = ProxyThread(conn_tcp, conn_proxy)
            c2s_thr.setDaemon(True)
            s2c_thr.setDaemon(True)
            c2s_thr.start()
            s2c_thr.start()
