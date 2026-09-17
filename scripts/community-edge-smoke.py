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
    try:
        adapted = subprocess.run([args.caddy,'adapt','--adapter','caddyfile','--config',args.config],capture_output=True,text=True,check=True)
        config = json.loads(adapted.stdout)
        servers = config['apps']['http']['servers']
        server = next(v for v in servers.values() if ':443' in v['listen'])
        # Keep the real route matchers and handlers; substitute transport endpoints
        # and disable TLS/ACME/admin for this isolated routing test.
        server = json.loads(json.dumps(server).replace('127.0.0.1:7780',f'127.0.0.1:{upstream.server_port}').replace('127.0.0.1:7777',f'127.0.0.1:{upstream.server_port}'))
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
                        conn=http.client.HTTPConnection('127.0.0.1',port,timeout=5)
                        try:
                            conn.request(method,route,headers={'Host':host})
                            response=conn.getresponse();response.read()
                            return response.status, response.getheader('Location')
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
                    print('PASS: both public hosts route app/API/health to community; native, operational, admin and unknown routes cannot bypass it.')
                finally:
                    process.terminate()
                    try: process.wait(timeout=5)
                    except subprocess.TimeoutExpired:process.kill();process.wait()
    finally:upstream.shutdown();upstream.server_close()
if __name__=='__main__': main()
