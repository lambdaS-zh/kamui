import json
from os import (
    listdir,
    makedirs,
    remove as remove_file,
)
from os.path import (
    isdir,
    isfile,
    join as path_join,
)
from threading import Semaphore
from shutil import rmtree
from time import (
    monotonic,
    sleep as os_sleep,
)

from kamui.base_stream import (
    BaseIO,
    EAgain,
    id_head,
    id_segments,
    id_split,
    is_request_token,
    ID_SERVER_LISTEN_BACKLOG,
    ID_CONNECTION,
    ID_CONN_C2S_CTRL,
    ID_CONN_C2S_DATA,
    ID_CONN_S2C_CTRL,
    ID_CONN_S2C_DATA,
)


class _ServerListenBacklogIO(object):

    @staticmethod
    def target_dir(workspace, zone_id):
        # e.g.
        # id= $ID_SERVER_LISTEN_BACKLOG/foo.com
        # path= $WORKSPACE/addresses/foo.com/requests/
        address = id_split(zone_id)[1]
        return path_join(workspace, 'addresses', address, 'requests')

    @staticmethod
    def target_request_file(workspace, zone_id):
        # e.g.
        # id= $ID_SERVER_LISTEN_BACKLOG/foo.com/0123456789abcdef
        # path= $WORKSPACE/addresses/foo.com/requests/0123456789abcdef
        address = id_split(zone_id)[1]
        token = id_split(zone_id)[2]
        return path_join(workspace, 'addresses', address, 'requests', token)

    @staticmethod
    def is_list(zone_id):
        return id_segments(zone_id) == 2

    @classmethod
    def delete(cls, workspace, zone_id):
        if cls.is_list(zone_id):
            t_dir = cls.target_dir(workspace, zone_id)
            rmtree(t_dir)
        else:
            t_file = cls.target_request_file(workspace, zone_id)
            remove_file(t_file)

    def __init__(self, workspace, zone_id):
        self._workspace = workspace
        self._zone_id = zone_id
        self._is_list = self.is_list(zone_id)

    def write(self, data):
        t_dir = self.target_dir(self._workspace, self._zone_id)
        if not isdir(t_dir):
            makedirs(t_dir)

        assert not self._is_list

        t_file = self.target_request_file(self._workspace, self._zone_id)
        with open(t_file, 'w') as fd:
            content = json.dumps(data)
            fd.write(content)

    def read(self, create):
        t_dir = self.target_dir(self._workspace, self._zone_id)
        if not isdir(t_dir) and create:
            makedirs(t_dir)

        if self._is_list:
            items = listdir(t_dir)
            items = [item for item in items if is_request_token(item)]
            return {
                'PENDING':          len(items),
                'REQUEST_TOKENS':   items,
            }

        # --- is not list ---
        t_file = self.target_request_file(self._workspace, self._zone_id)
        if not isfile(t_file) and create:
            with open(t_file, 'w') as fd:
                fd.write('{}')
        try:
            with open(t_file, 'r') as fd:
                return json.load(fd)
        except OSError:
            return None
        except ValueError:
            raise EAgain('someone may be writing this zone.')


class _ConnectionIO(object):

    FILENAME_MAP = {
        ID_CONN_C2S_CTRL:   'c2s_ctrl',
        ID_CONN_C2S_DATA:   'c2s_data',
        ID_CONN_S2C_CTRL:   's2c_ctrl',
        ID_CONN_S2C_DATA:   's2c_data',
    }

    @staticmethod
    def target_dir(workspace, zone_id):
        # e.g.
        # id= $ID_CONNECTION/foo.com/1/$ID_CONN_C2S_CTRL
        # path= $WORKSPACE/addresses/foo.com/connections/00001/
        address = id_split(zone_id)[1]
        conn_num = '%05d' % int(id_split(zone_id)[2])
        return path_join(workspace, 'addresses', address, 'connections', conn_num)

    @classmethod
    def target_file(cls, workspace, zone_id):
        # e.g.
        # id= $ID_CONNECTION/foo.com/1/$ID_CONN_C2S_CTRL
        # path= $WORKSPACE/addresses/foo.com/connections/00001/c2s_ctrl
        address = id_split(zone_id)[1]
        conn_num = '%05d' % int(id_split(zone_id)[2])
        suffix = cls.FILENAME_MAP[id_split(zone_id)[3]]
        return path_join(workspace, 'addresses', address, 'connections', conn_num, suffix)

    @classmethod
    def delete(cls, workspace, zone_id):
        raise NotImplementedError()

    def __init__(self, workspace, zone_id):
        self._workspace = workspace
        self._zone_id = zone_id

    def write(self, data):
        # TODO: open with binary mode when file is type of data
        t_dir = self.target_dir(self._workspace, self._zone_id)
        if not isdir(t_dir):
            makedirs(t_dir)

        t_file = self.target_file(self._workspace, self._zone_id)
        with open(t_file, 'w') as fd:
            content = json.dumps(data)
            fd.write(content)

    def read(self, create):
        # TODO: open with binary mode when file is type of data
        t_dir = self.target_dir(self._workspace, self._zone_id)
        if not isdir(t_dir) and create:
            makedirs(t_dir)

        t_file = self.target_file(self._workspace, self._zone_id)
        if not isfile(t_file) and create:
            with open(t_file, 'w') as fd:
                fd.write('{}')

        try:
            with open(t_file, 'r') as fd:
                return json.load(fd)
        except OSError:
            return None
        except ValueError:
            raise EAgain('someone may be writing this zone.')


class FsTcpTunnelIO(BaseIO):

    _ROUTES = {
        ID_SERVER_LISTEN_BACKLOG:   _ServerListenBacklogIO,
        ID_CONNECTION:              _ConnectionIO,
    }

    _IO_INTERVAL_MSEC = 333  # INTERVAL * IOPS = 1000ms
    _LAST_IO_TIMESTAMP = 0
    _SEMA = Semaphore(1)

    @classmethod
    def set_iops(cls, iops):
        cls._IO_INTERVAL_MSEC = 1000 / iops

    @classmethod
    def route(cls, zone_id):
        real_type = cls._ROUTES.get(id_head(zone_id))
        if real_type is None:
            raise ValueError('Unknown zone_id: %s' % zone_id)
        return real_type

    @classmethod
    def delete(cls, workspace, zone_id):
        return cls.route(zone_id).delete(workspace, zone_id)

    def __init__(self, workspace, zone_id):
        super(FsTcpTunnelIO, self).__init__(workspace, zone_id)
        real_type = self.route(zone_id)
        self._real_io = real_type(workspace, zone_id)

    def _atomic(self, func, *args, **kwargs):
        self._SEMA.acquire()
        try:
            now = self.now()
            age = now - self._LAST_IO_TIMESTAMP
            if age < self._IO_INTERVAL_MSEC:
                self.sleep(self._IO_INTERVAL_MSEC - age)
            try:
                return func(*args, **kwargs)
            finally:
                self._LAST_IO_TIMESTAMP = now
        finally:
            self._SEMA.release()

    def write(self, data):
        return self._atomic(self._real_io.write, data)

    def read(self, create=False):
        return self._atomic(self._real_io.read, create)

    @staticmethod
    def now():
        return monotonic() * 1000

    @staticmethod
    def sleep(interval_msec):
        os_sleep(interval_msec / 1000)
