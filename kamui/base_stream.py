ID_SERVER = 'id_server'
ID_CLIENT = 'id_client'


class EAgain(Exception):
    pass


class BaseIO(object):

    def __init__(self, workspace, zone_id):
        self._workspace = workspace
        self._zone_id = zone_id

    def write(self, data):
        raise NotImplementedError()

    def read(self):
        raise NotImplementedError()


class BaseConnection(object):

    def __init__(self, on_close):
        self._on_close = on_close

    def close(self):
        self._on_close(self)


class BaseClient(object):

    def connect(self, address):
        pass


class BaseServer(object):

    IO = BaseIO

    def __init__(self, workspace):
        self._workspace = workspace
        self._address = None
        self._backlog = None
        self._connections = None

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

        c_io = self.IO(self._workspace, ID_CLIENT)
        c_data = c_io.read()

        if not c_data.get('F_CONN'):
            raise EAgain('F_CONN false')

        address = c_data.get('CONN_ADDR')
        if address != self._address:
            raise EAgain('strange client')

        c_data['F_CONN_ACK'] = True
        c_io.write(c_data)

        conn = BaseConnection(self._close_cb)
        self._connections.append(conn)
        return conn

    def _close_cb(self, conn):
        self._connections.remove(conn)
