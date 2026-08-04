"""SSH reverse tunnel: forward local service INTO sandbox.

local adapter (localhost:18080) → SSH -R → sandbox localhost:18080
so claude inside sandbox can hit http://localhost:18080
"""
import os, time, threading, http.server
import paramiko
from managed_e2b import SandboxLifecycle

os.environ["E2B_API_KEY"] = "***REMOVED***"

lc = SandboxLifecycle(db_path="me2b.db", max_concurrent=1)
with lc.acquire(template="base", timeout=300) as h:
    sid = h.sandbox.sandbox_id
    print(f"sid: {sid}")

    # 1. Install + configure sshd in sandbox
    h.run("sudo apt-get update -qq 2>&1 | tail -1", timeout=60)
    h.run("sudo apt-get install -y -qq openssh-server 2>&1 | tail -1", timeout=60)
    h.run('sudo sed -i "s/#PermitRootLogin.*/PermitRootLogin yes/" /etc/ssh/sshd_config')
    h.run('sudo sed -i "s/#PasswordAuthentication.*/PasswordAuthentication yes/" /etc/ssh/sshd_config')
    h.run('echo "root:e2bpass" | sudo chpasswd')
    h.run("sudo ssh-keygen -A 2>&1 | tail -1")
    h.run("sudo /usr/sbin/sshd")
    time.sleep(1)
    r = h.run("ss -tlnp | grep :22 || echo NO_LISTEN")
    print("sshd:", r["stdout"].strip() if r else "err")

    # 2. Expose port 22 publicly
    pf = h.expose_port(22)
    ssh_host = pf.host
    print(f"ssh host: {ssh_host}")

    # 3. Start a local test HTTP server on :18080 (simulates anyharness adapter)
    class TestHandler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"hello from local adapter")
        def log_message(self, *a): pass

    srv = http.server.HTTPServer(("127.0.0.1", 18080), TestHandler)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    print("local test server on localhost:18080")

    # 4. SSH reverse tunnel via paramiko
    #    ssh -R 18080:127.0.0.1:18080 root@<sandbox_host>
    #    This makes sandbox:localhost:18080 forward to our local:18080
    print(f"connecting SSH to {ssh_host}:22 ...")
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    try:
        client.connect(ssh_host, port=22, username="root", password="e2bpass",
                       timeout=15, allow_agent=False, look_for_keys=False)
        print("SSH connected!")

        # Request reverse port forward: sandbox port 18080 → our local 18080
        transport = client.get_transport()
        success = transport.request_port_forward("", 18080)
        print(f"reverse tunnel: sandbox:18080 -> local:18080 (success={success})")

        # 5. Test: from inside sandbox, curl localhost:18080 should reach our local server
        time.sleep(2)
        r2 = h.run("curl -s http://localhost:18080/ 2>&1 || echo CURL_FAILED; echo END")
        print(f"sandbox → localhost:18080: {r2['stdout'] if r2 else 'err'}")

        if "hello from local" in (r2["stdout"] if r2 else ""):
            print("✅ REVERSE TUNNEL WORKS! Sandbox can reach local service via localhost:18080")
        else:
            print("❌ tunnel not working yet")

    except Exception as e:
        print(f"SSH error: {e}")
    finally:
        client.close()

    srv.shutdown()
    print("done")
