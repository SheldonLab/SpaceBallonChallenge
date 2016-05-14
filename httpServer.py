import cgi
import json
import sys
import urlparse
from BaseHTTPServer import BaseHTTPRequestHandler, HTTPServer
from SocketServer import ThreadingMixIn
import traceback
from dataupload import DataUpload
host = "localhost"
user = "root"
password = "moxie100"
database = (host, user, password)


class RequestHandler(BaseHTTPRequestHandler):

    def do_POST(self):
        parsed_url = urlparse.urlparse(self.path)
        path = parsed_url.path
        ctype, pdict = cgi.parse_header(self.headers['Content-Type'])
        try:
            data_string = self.rfile.read(int(self.headers['Content-Length']))
            print data_string
            data = json.loads(data_string)
            DataUpload(data)
            self.send_response(200)
            self.end_headers()
            return
        except:
            print traceback.print_exc()
            self.send_error(500)
            return



class ThreadedHTTPServer(ThreadingMixIn, HTTPServer):
    """Handle requests in a separate thread."""


def httpServer(server_ip, port):
    server_address = (server_ip, port)
    httpd = ThreadedHTTPServer(server_address, RequestHandler)

    sa = httpd.socket.getsockname()
    print "Serving HTTP on", sa[0], "port", sa[1], "..."
    print 'use <Ctrl-C> to stop'
    httpd.serve_forever()
