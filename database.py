import re
from tornado import gen
from tornado.httpclient import AsyncHTTPClient, HTTPError, HTTPRequest
from tornado.httputil import url_concat, HTTPHeaders
from tornado.escape import json_decode, json_encode
from tornado.concurrent import Future


config = {
    'url'      : 'http://localhost:7474/db/data/',
    'username' : 'neo4j',
    'password' : '123456'
}


class DatabaseError(HTTPError):

    def __init__(self, code, message=None, response=None):
        super(DatabaseError, self).__init__(code, message, response)


class Request(object):

    header = {
        'Accept': 'application/json; charset=UTF-8',
        'Connection': 'keep-alive',
        'X-Stream': 'true'
    }

    def __init__(self, io_loop=None):
        self.client = AsyncHTTPClient(io_loop)

    def fetch(self, url, method='GET', body=None, callback=None):
        header = HTTPHeaders(self.header)
        if body is not None:
            header['Content-Type'] = 'application/json'
            body = json_encode(body)
        request = HTTPRequest(url, method, header, body, 
            auth_username = config.get('username', None),
            auth_password = config.get('password', None)
        )
        return self.client.fetch(request, callback=callback)


class Database(object):

    def __init__(self, request, service_root):
        self.request = request
        self.service_root = service_root

    def __call__(self):
        return Connection(self.request, self.service_root)

    @classmethod
    def init(cls, io_loop=None, callback=None):
        request = Request(io_loop)
        def _response(response):
            if response.body:
                service_root = json_decode(response.body)
                if callback:
                    callback(cls(request, service_root))
            else:
                raise DatabaseError(response.code, 'Unable to get service root.', response)
        request.fetch(config.get('url'), callback=_response)


class Connection(Future):

    def __init__(self, request, service_root):
        super(Connection, self).__init__()
        self.request = request
        self.service_root = service_root
        self.entered = 0

    def __enter__(self):
        super(Connection, self).__init__()
        self.entered += 1
        return self

    def __exit__(self, type, value, traceback):
        if type and isinstance(type, Exception):
            raise type
        if self.entered == 1:
            self.commit()
        self.entered -= 1

    def query(self, query, params=None):
        statement = {
            'statement': query
        }
        if params:
            statement['parameters'] = params
        @gen.coroutine
        def _asynchronous():
            response = yield self.queries(statement)
            if response:
                return response[0]
            else:
                return None
        return _asynchronous()

    def queries(self, *statements):
        url = self.service_root['transaction']
        if self.entered == 0:
            if hasattr(self, '_commit'):
                url = self._commit
            else:
                url = '%s/commit' % url
        statements = {'statements':list(statements)}
        @gen.coroutine
        def _asynchronous():
            response = yield self.request.fetch(url, 'POST', statements)
            content = json_decode(response.body)
            if content.get('errors'):
                messages = ''
                for error in content['errors']:
                    messages += error['message']
                raise DatabaseError(response.code, messages, response)
            else:
                if 'Location' in response.headers:
                    self._commit = response.headers['Location']
                results = []
                for result in content['results']:
                    rows = []
                    columns = result['columns']
                    for row in result['data']:
                        data = {}
                        for index, column in enumerate(row['row']):
                            data[columns[index]] = column
                        rows.append(data)
                    results.append(rows)
                return results
        return _asynchronous()

    def commit(self):
        if hasattr(self, '_commit'):
            def _response(response):
                content = json_decode(response.body)
                if content.get('errors'):
                    messages = ''
                    for error in content['errors']:
                        messages += error['message']
                    self.set_exception(DatabaseError(response.code, messages, response))
                else:
                    self.set_result(response)
            url = '%s/commit' % self._commit
            self.request.fetch(url, 'POST', {'statements':[]}, callback=_response)
        else:
            raise Exception('No statement exist.')