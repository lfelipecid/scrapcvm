from browsermobproxy import Server


class ProxyManager:
    __BMP = r'/home/felipecid/documents/browsermob-proxy-2.1.4/bin/browsermob-proxy'

    def __init__(self):
        self.__server = Server(ProxyManager.__BMP)
        self.__client = None

    def start_server(self):
        self.__server.start()
        return self.__server

    def start_client(self):
        self.__client = self.__server.create_proxy(params={'trustAllServers': 'true'})
        return self.__client

    @property
    def client(self):
        return self.__client

    @property
    def server(self):
        return self.__server
