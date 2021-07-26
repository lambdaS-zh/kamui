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
from shutil import rmtree

from kamui.base_stream import (
    BaseIO,
    BaseClient,
    BaseServer,
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
        address = id_split(zone_id)[1]
        return path_join(workspace, 'addresses', address)

    @staticmethod
    def target_request_file(workspace, zone_id):
        address = id_split(zone_id)[1]
        token = id_split(zone_id)[2]
        return path_join(workspace, 'addresses', address, token)

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

    @classmethod
    def delete(cls, workspace, zone_id):
        raise NotImplementedError()

    def __init__(self, workspace, zone_id):
        self._workspace = workspace
        self._zone_id = zone_id

    def write(self, data):
        raise NotImplementedError()

    def read(self, create):
        raise NotImplementedError()


class FsTcpTunnelIO(BaseIO):

    _ROUTES = {
        ID_SERVER_LISTEN_BACKLOG:   _ServerListenBacklogIO,
        ID_CONNECTION:              _ConnectionIO,
    }

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

    def write(self, data):
        return self._real_io.write(data)

    def read(self, create=False):
        return self._real_io.read(create)


class FsTcpTunnelClient(BaseClient):
    IO = FsTcpTunnelIO


class FsTcpTunnelServer(BaseServer):
    IO = FsTcpTunnelIO
