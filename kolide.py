import requests
import json
import websocket
import ssl
import urllib3
from datetime import datetime

config = {
    "host": "",
    "username": "",
    "password": "",
}


class Kolide:
    def __init__(self):
        """Class to provide Kolide Fleet API"""
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        self.__cfg = config
        self.__token = ''
        self.__get_token()

    def __get_token(self):
        """Get token from Kolide Fleet"""
        data = json.dumps({'username': self.__cfg['username'], 'password': self.__cfg['password']})
        try:
            response = requests.post('https://%s/api/v1/kolide/login' % self.__cfg['host'], data=data, verify=False)
        except Exception as e:
            print('%s\n%s' % (datetime.now(), e))
            return
        self.__token = response.json().get('token', '')
        return

    def __find_host_id_by_host_name(self, response, host):
        """Find host id by host name

        :param response: JSON object with server response
        :param host: host name
        :return: host id or error message"""
        for i in range(len(response.get('hosts', []))):
            hostname = str(response['hosts'][i].get('hostname', '')).lower()
            if host != hostname:
                continue
            if response['hosts'][i].get('status', '') == 'offline':
                return '%s is offline' % host
            return response['hosts'][i].get('id', None)
        return 'not found: %s' % host

    def __get_host_id(self, host):
        """Retrieve host id from Kolide Fleet

        :param host: host name
        :return: host id or error message"""
        try:
            session = requests.session()
            session.verify = False
            session.headers = {'Content-Type': 'application/json', 'Authorization': 'Bearer %s' % self.__token}
            response = session.get('https://%s/api/v1/kolide/hosts' % self.__cfg['host'])
            session.close()
        except Exception as e:
            print('%s\n%s' % (datetime.now(), e))
            return 'cannot connect to Kolide Fleet'
        if response.status_code != requests.codes.ok:
            return 'cannot get data from Kolide Fleet with provided token: status: %s' % response.status_code
        return self.__find_host_id_by_host_name(response.json(), host)

    def __send_query(self, host_id, query):
        """Send query via Kolide Fleet

        :param host_id: host id
        :param query: SQL query
        :return: query id or error message"""
        try:
            session = requests.session()
            session.verify = False
            session.headers = {'Content-Type': 'application/json', 'Authorization': 'Bearer %s' % self.__token}
            response = session.post('https://%s/api/v1/kolide/queries/run' % self.__cfg['host'],
                                    data=json.dumps({"query": query, "selected": {"hosts": [host_id], "labels": []}})
                                    ).json()
            session.close()
            return response.get('campaign', {}).get('id', None)
        except Exception as e:
            print('%s\n%s' % (datetime.now(), e))
            return 'cannot send query to Kolide Fleet'

    def __get_response(self, identifier):
        """Retrieve response from Kolide Fleet

        :param identifier: query id
        :return: (True|False, query response)"""
        if not isinstance(identifier, int):
            return False, identifier
        try:
            ws = websocket.create_connection("wss://%s/api/v1/kolide/results/websocket" % self.__cfg['host'],
                                             sslopt={"cert_reqs": ssl.CERT_NONE})
            ws.send(json.dumps({"type": "auth", "data": {"token": self.__token}}))
            ws.send(json.dumps({"type": "select_campaign", "data": {"campaign_id": identifier}}))
            # the query results are in the 3rd response from Kolide Fleet websocket
            ws.recv()
            ws.recv()
            full_message = json.loads(ws.recv())
            ws.close()
        except Exception as e:
            print('%s\n%s' % (datetime.now(), e))
            return False, 'cannot get response from Kolide Fleet'
        message = []
        for i in range(len(full_message.get("data", {}).get("rows", []))):
            message.append(full_message["data"]["rows"][i])
        return True if message else False, message

    def query(self, host: str, query_name: str, query_args=''):
        """Produce query to Kolide Fleet

        :param host: host name
        :param query_name: name of query
        :param query_args: query arguments
        :return: (True|False, query response)"""
        host_id = self.__get_host_id(host.lower())
        if not isinstance(host_id, int):
            return False, host_id
        if query_name == 'file_hash':
            query = 'SELECT path, md5, sha1, sha256 FROM hash where path = "%s"' % query_args
            success, data = self.__get_response(self.__send_query(host_id, query))
            return (success, 'file not found or hash cannot be calculated') if not success else (success, data[0])
        else:
            return False, 'Unknown query name: %s' % query_name


if __name__ == '__main__':
    status, info = Kolide().query('<some_host_name>', 'file_hash', r'c:\windows\notepad.exe')
    print(status)
    print(json.dumps(info, indent=2))
