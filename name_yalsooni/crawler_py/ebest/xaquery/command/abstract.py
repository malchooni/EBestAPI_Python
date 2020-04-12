class CM:
    TR_NAME = "TR_NAME"
    # REQUEST = "REQUEST"
    RES_TYPE = "RES_TYPE"
    SHUTDOWN = "SHUTDOWN"

    RETRY_REQUEST = "RETRY_REQUEST"

    RES_TYPE_DB = "DB"
    RES_TYPE_SOCKET = "SOCKET"


# 명령어
class Command:

    def __init__(self, tr_name):
        self.command = dict()
        self.command[CM.TR_NAME] = tr_name
        self.command[CM.RES_TYPE] = CM.RES_TYPE_DB
        self.command[CM.RETRY_REQUEST] = 0
        self.request_dict = None
        self.response_dict = None
        self.uuid = None
        self.outbound = None

    def get_command(self):
        return self.command

    def get_request(self):
        return self.request_dict

    def get_tr_name(self):
        return self.command[CM.TR_NAME]

    def get_res_type(self):
        return self.command[CM.RES_TYPE]

    def get_retry_request(self):
        return self.command[CM.RETRY_REQUEST]

    def get_response(self):
        return self.response_dict

    def get_outbound(self):
        return self.outbound

    def get_uuid(self):
        return self.uuid

    def set_res_type(self, res_type):
        self.command[CM.RES_TYPE] = res_type

    def set_response(self, response_dict):
        self.response_dict = response_dict

    def set_uuid(self, uuid):
        self.uuid = uuid

    def set_outbound(self, outbound):
        self.outbound = outbound
