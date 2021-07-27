from functools import partial
from socket import (
    SHUT_RD,
    SHUT_RDWR,
    SHUT_WR,
)
from uuid import uuid4
from zlib import crc32


ID_SERVER_LISTEN_BACKLOG = 'id_server_listen_backlog'
ID_CONNECTION = 'id_connection'
ID_CONN_C2S_CTRL = 'id_conn_c2s_ctrl'
ID_CONN_C2S_DATA = 'id_conn_c2s_data'
ID_CONN_S2C_CTRL = 'id_conn_s2c_ctrl'
ID_CONN_S2C_DATA = 'id_conn_s2c_data'


def id_join(*args):
    args = (str(arg) for arg in args)
    return '/'.join(args)


def id_head(zone_id):
    return zone_id.split('/')[0]


def id_split(zone_id):
    return zone_id.split('/')


def id_segments(zone_id):
    return len(id_split(zone_id))


def make_request_token():
    return 'req-' + uuid4().hex


def is_request_token(raw):
    return raw.startswith('req-')


class EAgain(Exception):
    pass


class BlockingOperation(Exception):

    def __init__(self, retrying):
        super(BlockingOperation, self).__init__()
        self.retrying = retrying


class ConnectionRefused(Exception):
    pass


class BaseIO(object):

    # TODO: move to an individual base_io.py

    @classmethod
    def set_iops(cls, iops):
        raise NotImplementedError()

    @classmethod
    def delete(cls, workspace, zone_id):
        raise NotImplementedError()

    @classmethod
    def checksum(cls, io_data):
        return hex(crc32(io_data)).replace('0x', '')

    def __init__(self, workspace, zone_id):
        self._workspace = workspace
        self._zone_id = zone_id

    def write(self, data):
        raise NotImplementedError()

    def read(self, create=False):
        raise NotImplementedError()

    def delete_self(self):
        return self.delete(self._workspace, self._zone_id)


class BaseConnection(object):

    IO = BaseIO

    STAGE_IDLE = 'idle'
    STAGE_REQUESTING = 'requesting'
    STAGE_REPLYING = 'replying'

    @classmethod
    def get_stage(cls, ctrl_data):
        snd = ctrl_data.get('F_SND')
        snd_ack = ctrl_data.get('F_SND_ACK')
        if snd and not snd_ack:
            return cls.STAGE_REQUESTING
        elif snd and snd_ack:
            return cls.STAGE_REPLYING
        else:
            return cls.STAGE_IDLE

    @staticmethod
    def finishing(ctrl_data):
        return ctrl_data.get('F_FIN') and not ctrl_data.get('F_FIN_ACK')

    def __init__(self, workspace, side, zone_id, on_close=None):
        assert side in ('client', 'server')
        self._workspace = workspace
        self._side = side
        self._zone_id = zone_id
        self._on_close = on_close
        self._recv_buffer = b''
        self._recv_eof = False
        self._send_eof = False
        self._recv_seq = 0
        self._send_seq = 0

        if side == 'client':
            self._recv_ctrl_id = id_join(zone_id, ID_CONN_S2C_CTRL)
            self._recv_data_id = id_join(zone_id, ID_CONN_S2C_DATA)
            self._send_ctrl_id = id_join(zone_id, ID_CONN_C2S_CTRL)
            self._send_data_id = id_join(zone_id, ID_CONN_C2S_DATA)
        else:
            self._recv_ctrl_id = id_join(zone_id, ID_CONN_C2S_CTRL)
            self._recv_data_id = id_join(zone_id, ID_CONN_C2S_DATA)
            self._send_ctrl_id = id_join(zone_id, ID_CONN_S2C_CTRL)
            self._send_data_id = id_join(zone_id, ID_CONN_S2C_DATA)

    def recv(self, data_len=0):
        raise BlockingOperation(partial(
            self._on_retrying_receiving, data_len))

    def _cut_buffer(self, data_len):
        if 0 == data_len:
            tmp = self._recv_buffer
            self._recv_buffer = b''
            return tmp
        tmp = self._recv_buffer[:data_len]
        self._recv_buffer = self._recv_buffer[data_len:]
        return tmp

    def _on_retrying_receiving(self, data_len):
        ctrl_io = self.IO(self._workspace, self._recv_ctrl_id)
        data_io = self.IO(self._workspace, self._recv_data_id)

        ctrl_data = ctrl_io.read()
        if ctrl_data is None:
            raise EAgain('request not found')

        stage = self.get_stage(ctrl_data)

        if not self._recv_eof and stage == self.STAGE_REQUESTING:
            if ctrl_data.get('SEQ', -7) != self._recv_seq + 1:
                raise BrokenPipeError('bad request seq')
            in_data = data_io.read()
            if self.IO.checksum(in_data) != ctrl_data.get('CHECKSUM'):
                raise BrokenPipeError('bad request checksum')

            self._recv_buffer += in_data
            self._recv_seq += 1
            ctrl_data['F_SND_ACK'] = True
            ctrl_data['SEQ_ACK'] = self._recv_seq
            ctrl_io.write(ctrl_data)
            # REQUESTING -> REPLYING
            raise EAgain('data received')

        if self.finishing(ctrl_data):
            self._recv_eof = True

        buf_len = len(self._recv_buffer)
        if buf_len >= data_len:
            return self._cut_buffer(data_len)
        elif self._recv_eof:
            return self._cut_buffer(buf_len)
        else:
            raise EAgain('not enough')

    def sendall(self, data):
        assert isinstance(data, bytes)
        raise BlockingOperation(partial(
            self._on_retrying_sending_all, data))

    def _on_retrying_sending_all(self, data):
        ctrl_io = self.IO(self._workspace, self._send_ctrl_id)
        data_io = self.IO(self._workspace, self._send_data_id)

        ctrl_data = ctrl_io.read(create=True)
        stage = self.get_stage(ctrl_data)

        if self.finishing(ctrl_data):
            raise BrokenPipeError('sending-pipe closed')

        if stage == self.STAGE_IDLE:
            data_io.write(data)
            self._send_seq += 1
            ctrl_data['F_SND'] = True
            ctrl_data['SEQ'] = self._send_seq
            ctrl_data['CHECKSUM'] = self.IO.checksum(data)
            ctrl_io.write(ctrl_data)
            # IDLE -> REQUESTING
            raise EAgain('data sent')

        if stage == self.STAGE_REPLYING:
            if ctrl_data.get('SEQ_ACK', -7) != self._send_seq:
                raise BrokenPipeError('bad reply ack')
            ctrl_data['F_SND'] = False
            ctrl_data['F_SND_ACK'] = False
            ctrl_data['SEQ'] = -1
            ctrl_data['SEQ_ACK'] = -1
            ctrl_io.write(ctrl_data)
            # REPLYING -> IDLE
            raise EAgain('ack got')

        raise EAgain('waiting for rely')

    def shutdown(self, flag):
        assert flag in (SHUT_RD, SHUT_WR, SHUT_RDWR)
        if flag == SHUT_RD:
            # Do nothing.
            pass
        else:
            ctrl_io = self.IO(self._workspace, self._send_ctrl_id)
            ctrl_data = ctrl_io.read(create=True)
            ctrl_data['F_FIN'] = True
            ctrl_io.write(ctrl_data)
            self._send_eof = True

    def close(self):
        # TODO: mark connection file as reusable
        if not self._send_eof:
            self.shutdown(SHUT_RDWR)
        if self._on_close is not None:
            self._on_close(self)
            self._on_close = None


class BaseClient(object):

    IO = BaseIO

    def __init__(self, workspace):
        self._workspace = workspace

    def connect(self, address):
        request_token = make_request_token()
        zone_id = id_join(ID_SERVER_LISTEN_BACKLOG, address, request_token)
        r_io = self.IO(self._workspace, zone_id)
        r_data = r_io.read(create=True)
        if r_data is None:
            raise ConnectionRefused()

        r_data['CLIENT_ADDRESS'] = 'reserved'
        r_io.write(r_data)

        raise BlockingOperation(partial(
            self._on_retrying_connecting, r_io, address))

    def _on_retrying_connecting(self, r_io, address):
        r_data = r_io.read()
        if r_data.get('F_CONN') and r_data.get('F_CONN_ACK'):
            conn_num = r_data['CONN_NUM']
            zone_id = id_join(ID_CONNECTION, address, conn_num)
            r_io.delete_self()
            return BaseConnection(self._workspace, 'client', zone_id)
        else:
            raise EAgain('waiting for server accepting')


class BaseServer(object):

    IO = BaseIO

    def __init__(self, workspace):
        self._workspace = workspace
        self._address = None
        self._backlog = None
        self._connections = None
        self._conn_nums = [False] * 1000

    def _pick_conn_num(self):
        for num in range(len(self._conn_nums)):
            if not self._conn_nums[num]:
                self._conn_nums[num] = True
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

        zone_id = id_join(ID_SERVER_LISTEN_BACKLOG, self._address)
        b_io = self.IO(self._workspace, zone_id)
        b_data = b_io.read(create=True)
        if b_data.get('PENDING', 0) >= self._backlog:
            raise EAgain('backlog busy')

        request_tokens = b_data.get('REQUEST_TOKENS', [])
        for token in request_tokens:
            try:
                conn = self._accept_one(token)
            except EAgain:
                continue
            else:
                return conn

        raise EAgain('no requests at present.')

    def _accept_one(self, request_token):
        zone_id = id_join(ID_SERVER_LISTEN_BACKLOG, self._address, request_token)
        l_io = self.IO(self._workspace, zone_id)
        l_data = l_io.read()
        if l_data is None or not l_data.get('F_CONN'):
            self.IO.delete(self._workspace, zone_id)
            raise EAgain('F_CONN false')

        conn_num = self._pick_conn_num()
        zone_id = id_join(ID_CONNECTION, self._address, conn_num)
        c_io = self.IO(self._workspace, zone_id)
        c_data = c_io.read(create=True)
        c_data['SERVER_ADDRESS'] = self._address
        c_io.write(c_data)

        l_data['F_CONN_ACK'] = True
        l_data['CONN_NUM'] = conn_num
        l_io.write(l_data)

        conn = BaseConnection(self._workspace, 'server', zone_id,
                              partial(self._close_cb, conn_num))
        self._connections.append(conn)
        return conn

    def _close_cb(self, conn_num, conn):
        self._conn_nums[conn_num] = False
        self._connections.remove(conn)
