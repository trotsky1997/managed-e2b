"""Test SSH port forwarding: sandbox sshd → local adapter."""
import os, time, subprocess
from managed_e2b import SandboxLifecycle

os.environ["E2B_API_KEY"] = "***REMOVED***"

lc = SandboxLifecycle(db_path="me2b.db", max_concurrent=1)
with lc.acquire(template="base", timeout=300) as h:
    sid = h.sandbox.sandbox_id
    print(f"sid: {sid}")

    # Install openssh-server + socat
    h.run("sudo apt-get update -qq 2>&1 | tail -1", timeout=60)
    h.run("sudo apt-get install -y -qq openssh-server socat 2>&1 | tail -1", timeout=60)
    print("installed sshd + socat")

    # Configure sshd for password auth
    h.run('sudo sed -i "s/#PermitRootLogin.*/PermitRootLogin yes/" /etc/ssh/sshd_config')
    h.run('sudo sed -i "s/#PasswordAuthentication.*/PasswordAuthentication yes/" /etc/ssh/sshd_config')
    h.run('echo "root:e2bpass" | sudo chpasswd')
    h.run("sudo ssh-keygen -A 2>&1 | tail -1")
    h.run("sudo /usr/sbin/sshd")
    time.sleep(1)

    # Verify sshd
    r = h.run("ss -tlnp | grep :22 || echo NO_LISTEN")
    print("sshd listen:", r["stdout"].strip() if r else "err")

    # Expose port 22
    pf = h.expose_port(22)
    ssh_host = pf.host
    print(f"ssh host: {ssh_host}")

    # Test SSH from local: ssh -R to forward local:18080 → sandbox:18080
    # First, start a simple HTTP server on local:18080 to test
    import threading, http.server

    class TestHandler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"hello from local")
        def log_message(self, *a): pass

    server = http.server.HTTPServer(("127.0.0.1", 18080), TestHandler)
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()
    print("local test server on :18080")

    # SSH reverse tunnel: local → sandbox, forward sandbox:18080 → local:18080
    # sshpass for password auth
    sshpass_check = subprocess.run(["which", "sshpass"], capture_output=True, text=True)
    if not sshpass_check.stdout.strip():
        print("installing sshpass...")
        subprocess.run(["choco", "install", "sshpass", "-y"], capture_output=True)

    # Try SSH with password
    ssh_cmd = [
        "ssh", "-o", "StrictHostKeyChecking=no",
        "-o", "UserKnownHostsFile=/dev/null",
        "-o", "PreferredAuthentications=password",
        "-o", "PubkeyAuthentication=no",
        "-o", "ConnectTimeout=15",
        "-R", "18080:127.0.0.1:18080",  # reverse tunnel
        f"root@{ssh_host}",
        "echo SSH_CONNECTED && curl -s http://localhost:18080/healthz || curl -s http://localhost:18080/ && echo END"
    ]

    print(f"running: ssh -R 18080:localhost:18080 root@{ssh_host}")
    result = subprocess.run(ssh_cmd, capture_output=True, text=True, timeout=30,
                          input="e2bpass\n")
    print(f"ssh rc: {result.returncode}")
    print(f"ssh stdout: {result.stdout}")
    print(f"ssh stderr: {result.stderr[:300]}")

    server.shutdown()
    print("done")
