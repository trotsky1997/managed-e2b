"""Test expose_local: forward local service into sandbox via SSH reverse tunnel."""
import os, time, threading, http.server
os.environ["E2B_API_KEY"] = "***REMOVED***"

from managed_e2b import SandboxLifecycle

lc = SandboxLifecycle(db_path="me2b.db", max_concurrent=1)
with lc.acquire(template="base", timeout=300) as h:
    sid = h.sandbox.sandbox_id
    print(f"sid: {sid}")

    # Start a local test HTTP server on :18080
    class Handler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"hello from local adapter via expose_local!")
        def log_message(self, *a): pass
    srv = http.server.HTTPServer(("127.0.0.1", 18080), Handler)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    print("local test server on :18080")

    # Use the new expose_local method
    print("calling h.expose_local(18080)...")
    pf = h.expose_local(18080)
    print(f"PortForward: port={pf.port} host={pf.host} url={pf.url}")
    time.sleep(2)

    # Test: from sandbox, curl localhost:18080 should reach our local server
    r = h.run("curl -s http://127.0.0.1:18080/ 2>&1 || echo CURL_FAIL; echo END")
    print(f"sandbox -> localhost:18080: {r['stdout'] if r else 'err'}")

    if "hello from local" in (r["stdout"] if r else ""):
        print("\n✅ expose_local WORKS!")
    else:
        print("\n❌ expose_local failed")

    srv.shutdown()
    print("done")
