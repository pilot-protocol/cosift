#!/usr/bin/env python3
"""Run the candidate Caddy routing locally with disposable HTTP upstreams.
Requires a Caddy binary; never contacts or configures production.
"""
import argparse, http.client, json, os, socket, subprocess, tempfile, threading, time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--caddy', default='caddy')
    parser.add_argument('--config', default=str(Path(__file__).resolve().parents[1] / 'deploy/Caddyfile.community'))
    args = parser.parse_args()
    received = []
    class Upstream(BaseHTTPRequestHandler):
        def log_message(self, *args): pass
        def do_GET(self):
            received.append((self.path, dict(self.headers)))
            body = b'{"status":"ok"}'
            self.send_response(200)
            self.send_header('Content-Type','application/json')
            self.send_header('Content-Length',str(len(body)))
            self.end_headers(); self.wfile.write(body)
        do_POST = do_GET
    upstream = ThreadingHTTPServer(('127.0.0.1',0),Upstream)
    threading.Thread(target=upstream.serve_forever,daemon=True).start()
    engine_received = []
    class Engine(BaseHTTPRequestHandler):
        # Stands in for the :7779 listener: its JSON 404 for any unknown path.
        def log_message(self, *args): pass
        def do_POST(self):
            length = int(self.headers.get('Content-Length') or 0)
            if length: self.rfile.read(length)
            engine_received.append((self.path, self.headers.get_all('X-Forwarded-For') or [], dict(self.headers)))
            status, body = (200, b'{"verdict":"none"}') if self.path == '/v1/articles/match' else (404, b'{"error":"Not Found","status":404,"code":"not_found","detail":"not found"}')
            try:
                self.send_response(status)
                self.send_header('Content-Type','application/json')
                self.send_header('Content-Length',str(len(body)))
                self.end_headers(); self.wfile.write(body)
            except BrokenPipeError: pass
        do_GET = do_PUT = do_DELETE = do_POST
    engine = ThreadingHTTPServer(('127.0.0.1',0),Engine)
    threading.Thread(target=engine.serve_forever,daemon=True).start()
    try:
        adapted = subprocess.run([args.caddy,'adapt','--adapter','caddyfile','--config',args.config],capture_output=True,text=True,check=True)
        config = json.loads(adapted.stdout)
        servers = config['apps']['http']['servers']
        server = next(v for v in servers.values() if ':443' in v['listen'])
        # Keep the real route matchers and handlers; substitute transport endpoints
        # and disable TLS/ACME/admin for this isolated routing test.
        server = json.loads(json.dumps(server).replace('127.0.0.1:7780',f'127.0.0.1:{upstream.server_port}').replace('127.0.0.1:7777',f'127.0.0.1:{upstream.server_port}').replace('127.0.0.1:7779',f'127.0.0.1:{engine.server_port}'))
        with socket.socket() as sock:
            sock.bind(('127.0.0.1',0)); port=sock.getsockname()[1]
        server['listen']=[f'127.0.0.1:{port}']
        server['automatic_https']={'disable':True}
        config={'admin':{'disabled':True},'apps':{'http':{'servers':{'test':server}}}}
        with tempfile.TemporaryDirectory(prefix='cosift-edge-') as temp:
            path=Path(temp)/'caddy.json';path.write_text(json.dumps(config))
            with open(Path(temp)/'caddy.log','w+') as log:
                env=dict(os.environ,XDG_DATA_HOME=temp,XDG_CONFIG_HOME=temp)
                process=subprocess.Popen([args.caddy,'run','--config',str(path)],stdout=log,stderr=subprocess.STDOUT,env=env)
                try:
                    def call(route,method='GET',host='cosift.pilotprotocol.network'):
                        return send(route,method,host)[:2]
                    def send(route,method='GET',host='cosift.pilotprotocol.network',headers=None,body=None):
                        conn=http.client.HTTPConnection('127.0.0.1',port,timeout=5)
                        try:
                            conn.putrequest(method,route,skip_host=True,skip_accept_encoding=True)
                            conn.putheader('Host',host)
                            for k,v in (headers or []): conn.putheader(k,v)
                            if body is not None: conn.putheader('Content-Length',str(len(body)))
                            conn.endheaders(body)
                            response=conn.getresponse();data=response.read()
                            return response.status, response.getheader('Location'), response.getheader('Content-Type'), data
                        finally:conn.close()
                    deadline=time.monotonic()+10
                    while True:
                        try: call('/healthz');break
                        except OSError:
                            if process.poll() is not None or time.monotonic()>deadline:
                                log.seek(0);raise RuntimeError(log.read())
                            time.sleep(.05)
                    for host in ('cosift.pilotprotocol.network','origin.cosift.pilotprotocol.network'):
                        for route in ('/','/signup','/login','/app.js','/style.css','/sample.csv','/healthz','/api/limits','/api/credits','/search?q=test','/answer?q=test','/research?q=test'):
                            assert call(route,host=host)[0]==200,(host,route)
                        assert call('/api/payments/webhook','POST',host)[0]==200
                        for route in ('/query','/find','/find_similar','/contents','/stats','/metrics','/queue','/domains','/verify','/sla','/admin','/admin/community-enqueue','/debug/pprof/','/unknown','/search/','/api','/openapi.json'):
                            before=len(received)
                            status,_=call(route,host=host)
                            assert status==404,(host,route,status,'native route bypass')
                            assert len(received)==before,(host,route,'reached an upstream')
                        status,location=call('/chat',host=host)
                        assert status==302 and location=='/login',(host,'/chat',status,location)
                        oidc='Bearer eyJhbGciOiJSUzI1NiIsImtpZCI6ImsifQ.eyJpc3MiOiJodHRwczovL2FjY291bnRzLmdvb2dsZS5jb20ifQ.c2ln'
                        key='csk_0123456789abcdef_'+'A'*52
                        for auth in ('Bearer '+key,'bearer '+key,'  BEARER   '+key,'Bearer\t'+key):
                            before=len(engine_received)
                            status,_,_,data=send('/v1/articles/match','POST',host,[('Authorization',auth),('Content-Type','application/json')],b'{}')
                            assert status==403 and data==b'forbidden',(host,auth[:12],status,data)
                            assert len(engine_received)==before,(host,'a csk_ key reached the engine')
                        for spoof,want in (([('X-Forwarded-For','1.2.3.4, 5.6.7.8'),('CF-Connecting-IP','9.9.9.9')],'9.9.9.9'),([('X-Forwarded-For','1.2.3.4, 5.6.7.8')],'5.6.7.8'),([],'127.0.0.1')):
                            before=len(engine_received)
                            status,_,_,data=send('/v1/articles/match','POST',host,[('Authorization',oidc),('Content-Type','application/json')]+spoof,b'{"q":"x"}')
                            assert status==200 and data==b'{"verdict":"none"}',(host,status,data)
                            assert len(engine_received)==before+1,(host,'the OIDC request was not proxied')
                            path,xff,headers=engine_received[-1]
                            assert path=='/v1/articles/match' and xff==[want],(host,spoof,xff)
                            assert not any(k.lower()=='cf-connecting-ip' for k in headers),(host,'CF-Connecting-IP reached the engine')
                        before=len(engine_received)
                        status,location,ctype,data=send('/v1/nope','GET',host,[('Authorization',oidc)])
                        assert status==404 and location is None and ctype=='application/json' and json.loads(data)['code']=='not_found',(host,status,ctype,data)
                        assert len(engine_received)==before+1,(host,'the unknown /v1 path did not reach the engine')
                        before=len(engine_received)
                        assert call('/v1',host=host)[0]==404 and len(engine_received)==before,(host,'/v1 without a path reached the engine')
                        status,_,_,_=send('/v1/articles/match','POST',host,[('Authorization',oidc),('Content-Type','application/json')],b'{"q":"'+b'a'*(1<<20)+b'"}')
                        assert status==413,(host,'an oversized /v1 body',status)
                        time.sleep(.2)
                    print('PASS: both public hosts route app/API/health to community; native, operational, admin and unknown routes cannot bypass it.')
                    print('PASS: /v1 refuses csk_ keys at the edge, proxies OIDC bearers with exactly one X-Forwarded-For, and passes the engine\'s JSON 404 through.')
                finally:
                    process.terminate()
                    try: process.wait(timeout=5)
                    except subprocess.TimeoutExpired:process.kill();process.wait()
    finally:
        upstream.shutdown();upstream.server_close()
        engine.shutdown();engine.server_close()
if __name__=='__main__': main()
