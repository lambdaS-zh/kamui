from collections import deque
from uuid import uuid4


ID_SERVER = 'id_server'
ID_SERVER_LISTEN = 'id_server_listen'
ID_CLIENT = 'id_client'
ID_CONNECTION = 'id_connection'


def id_join(*args):
    return '/'.join(args)


class EAgain(Exception):
    pass


class EBlockingOperation(Exception):

    def __init__(self, retrying):
        super(EBlockingOperation, self).__init__()
        self.retrying = retrying


class ERefused(Exception):
    pass


class BaseIO(object):

    @classmethod
    def delete(cls, workspace, zone_id):
        raise NotImplementedError()

    def __init__(self, workspace, zone_id):
        self._workspace = workspace
        self._zone_id = zone_id

    def write(self, data):
        raise NotImplementedError()

    def read(self, create=False):
        raise NotImplementedError()


class BaseConnection(object):

    def __init__(self, magic, on_close):
        self._magic = magic
        self._on_close = on_close

    def shutdown(self):
        pass

    def close(self):
        self._on_close(self)


class BaseClient(object):

    IO = BaseIO

    def __init__(self, workspace):
        self._workspace = workspace

    def connect(self, address):
        # TODO: random zone_id
        zone_id = id_join(ID_SERVER_LISTEN, address)
        l_io = self.IO(self._workspace, zone_id)
        l_data = l_io.read()
        if l_data is None:
            raise ERefused()

        if l_data.get('F_CONN') and not l_data.get('F_CONN_ACK'):
            raise EAgain('some client else connecting')

        l_data['CLIENT_ID'] = 'todo'
        l_io.write(l_data)

        raise EBlockingOperation(self._on_retrying)

    def _on_retrying(self):
        pass


class BaseServer(object):

    IO = BaseIO

    def __init__(self, workspace):
        self._workspace = workspace
        self._address = None
        self._backlog = None
        self._connections = None
        self._recv_q = deque()
        self._conn_nums = set()

    def _pick_conn_num(self):
        for num in range(1000):
            if num not in self._conn_nums:
                self._conn_nums.add(num)
                return num
        raise EAgain('connection numbers full')

    def listen(self, address, backlog=5):
        assert isinstance(backlog, int)
        assert self._connections is None
        self._address = address
        self._backlog = backlog
        self._connections = list()

    def accept(self):
        assert self._address is not None

        if len(self._connections) >= self._backlog:
            raise EAgain('backlog busy')

        # TODO: random zone_id
        zone_id = id_join(ID_SERVER_LISTEN, self._address)
        l_io = self.IO(self._workspace, zone_id)
        l_data = l_io.read(create=True)

        if not l_data.get('F_CONN'):
            raise EAgain('F_CONN false')

        server_magic = uuid4().hex
        client_magic = uuid4().hex

        conn_num = self._pick_conn_num()
        zone_id = id_join(ID_CONNECTION, self._address, conn_num)
        c_io = self.IO(self._workspace, zone_id)
        c_data = c_io.read(create=True)
        c_data['SERVER_MAGIC'] = server_magic
        c_data['CLIENT_MAGIC'] = client_magic
        c_io.write(c_data)

        l_data['F_CONN_ACK'] = True
        l_data['CONN_NUM'] = conn_num
        l_io.write(l_data)

        conn = BaseConnection(server_magic, self._close_cb)
        self._connections.append(conn)
        return conn

    def _close_cb(self, conn):
        self._connections.remove(conn)
