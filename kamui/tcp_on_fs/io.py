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
            if isdir(t_dir):
                rmtree(t_dir)
        else:
            t_file = cls.target_request_file(workspace, zone_id)
            if isfile(t_file):
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
        t_file = cls.target_file(workspace, zone_id)
        if isfile(t_file):
            remove_file(t_file)

    def __init__(self, workspace, zone_id):
        self._workspace = workspace
        self._zone_id = zone_id
        if ID_CONN_C2S_DATA in zone_id or ID_CONN_S2C_DATA in zone_id:
            self._bin_data = True
        else:
            self._bin_data = False

    def write_ctrl(self, data):
        t_dir = self.target_dir(self._workspace, self._zone_id)
        if not isdir(t_dir):
            makedirs(t_dir)

        t_file = self.target_file(self._workspace, self._zone_id)
        with open(t_file, 'w') as fd:
            content = json.dumps(data)
            fd.write(content)

    def read_ctrl(self, create):
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

    def write_data(self, data):
        assert isinstance(data, bytes)
        t_dir = self.target_dir(self._workspace, self._zone_id)
        if not isdir(t_dir):
            makedirs(t_dir)

        t_file = self.target_file(self._workspace, self._zone_id)
        with open(t_file, 'wb') as fd:
            fd.write(data)

    def read_data(self, create):
        t_dir = self.target_dir(self._workspace, self._zone_id)
        if not isdir(t_dir) and create:
            makedirs(t_dir)

        t_file = self.target_file(self._workspace, self._zone_id)
        if not isfile(t_file) and create:
            with open(t_file, 'wb') as fd:
                fd.write(b'')

        try:
            with open(t_file, 'rb') as fd:
                return fd.read()
        except OSError:
            return None
        except ValueError:
            raise EAgain('someone may be writing this zone.')

    def write(self, data):
        if self._bin_data:
            return self.write_data(data)
        else:
            return self.write_ctrl(data)

    def read(self, create):
        if self._bin_data:
            return self.read_data(create)
        else:
            return self.read_ctrl(create)


class FsTcpTunnelIO(BaseIO):

    _ROUTES = {
        ID_SERVER_LISTEN_BACKLOG:   _ServerListenBacklogIO,
        ID_CONNECTION:              _ConnectionIO,
    }

    _io_interval_msec = 100  # INTERVAL * IOPS = 1000ms
    _last_io_timestamp = 0
    _sema = Semaphore(1)

    @classmethod
    def set_iops(cls, iops):
        cls._io_interval_msec = 1000 / iops

    @classmethod
    def route(cls, zone_id):
        real_type = cls._ROUTES.get(id_head(zone_id))
        if real_type is None:
            raise ValueError('Unknown zone_id: %s' % zone_id)
        return real_type

    @classmethod
    def delete(cls, workspace, zone_id):
        # return cls.route(zone_id).delete(workspace, zone_id)
        return cls._atomic(cls.route(zone_id).delete, workspace, zone_id)

    def __init__(self, workspace, zone_id):
        super(FsTcpTunnelIO, self).__init__(workspace, zone_id)
        real_type = self.route(zone_id)
        self._real_io = real_type(workspace, zone_id)

    @classmethod
    def _atomic(cls, func, *args, **kwargs):
        cls._sema.acquire(timeout=10)  # FIXME
        try:
            now = cls.now()
            age = now - cls._last_io_timestamp
            if age < cls._io_interval_msec:
                cls.sleep(cls._io_interval_msec - age)
            try:
                return func(*args, **kwargs)
            finally:
                cls._last_io_timestamp = now
        except OSError:
            # On windows, it may cause PermissionError if
            # someone else opened the same file.
            raise EAgain('resource temporarily unavailable.')
        finally:
            cls._sema.release()

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
