"""Test SSH to E2B sandbox via websocat WebSocket proxy."""
import os, time, subprocess
os.environ["E2B_API_KEY"] = "***REMOVED***"

from managed_e2b import SandboxLifecycle

lc = SandboxLifecycle(db_path="me2b.db", max_concurrent=1)
with lc.acquire(template="base", timeout=600) as h:
    sid = h.sandbox.sandbox_id
    print(f"sid: {sid}")

    # 1. Install openssh-server + websocat
    h.run("sudo apt-get update -qq 2>&1 | tail -1", timeout=60)
    h.run("sudo apt-get install -y -qq openssh-server 2>&1 | tail -1", timeout=60)
    h.run("curl -sL https://github.com/vi/websocat/releases/latest/download/websocat.x86_64-unknown-linux-musl -o /usr/local/bin/websocat && chmod +x /usr/local/bin/websocat", timeout=30)
    r = h.run("websocat --version 2>&1")
    print("websocat:", r["stdout"].strip() if r else "err")

    # 2. Configure sshd
    h.run('sudo sed -i "s/#PermitRootLogin.*/PermitRootLogin yes/" /etc/ssh/sshd_config')
    h.run('sudo sed -i "s/#PasswordAuthentication.*/PasswordAuthentication yes/" /etc/ssh/sshd_config')
    h.run('echo "root:e2bpass" | sudo chpasswd')
    h.run("sudo ssh-keygen -A 2>&1 | tail -1")
    h.run("sudo /usr/sbin/sshd")
    time.sleep(1)
    r2 = h.run("ss -tlnp | grep :22 || echo NO_LISTEN")
    print("sshd:", r2["stdout"].strip() if r2 else "err")

    # 3. Start websocat bridge: WS 8081 -> SSH 22
    h.run("nohup sudo websocat -b --exit-on-eof ws-l:0.0.0.0:8081 tcp:127.0.0.1:22 > /tmp/websocat.log 2>&1 & echo PID=$!", timeout=5)
    time.sleep(2)
    r3 = h.run("ss -tlnp | grep 8081 || echo NO_LISTEN")
    print("websocat listen:", r3["stdout"].strip() if r3 else "err")

    # 4. Expose port 8081
    pf = h.expose_port(8081)
    wss_url = f"wss://8081-{sid}.e2b.app"
    print(f"WSS endpoint: {wss_url}")

    # 5. Test SSH from local
    ssh_cmd = [
        "ssh",
        "-o", "StrictHostKeyChecking=no",
        "-o", "UserKnownHostsFile=/dev/null",
        "-o", "ConnectTimeout=15",
        "-o", "PreferredAuthentications=password",
        "-o", "PubkeyAuthentication=no",
        "-o", f"ProxyCommand=websocat --binary -B 65536 - {wss_url}",
        f"root@{sid}",
        "echo SSH_OK && whoami && hostname"
    ]
    print(f"SSH command: ssh ... root@{sid}")
    result = subprocess.run(ssh_cmd, capture_output=True, text=True, timeout=30,
                          input="e2bpass\n")
    print(f"ssh rc: {result.returncode}")
    print(f"ssh stdout: {result.stdout}")
    print(f"ssh stderr: {result.stderr[:400]}")

    if "SSH_OK" in result.stdout:
        print("\n✅ SSH WORKS! Now test reverse tunnel...")

        # 6. SSH -R: forward local:18080 into sandbox:18080
        print("starting reverse tunnel: local:18080 -> sandbox:18080")
        tunnel_cmd = [
            "ssh",
            "-o", "StrictHostKeyChecking=no",
            "-o", "UserKnownHostsFile=/dev/null",
            "-o", "PreferredAuthentications=password",
            "-o", "PubkeyAuthentication=no",
            "-o", "ConnectTimeout=15",
            "-o", f"ProxyCommand=websocat --binary -B 65536 - {wss_url}",
            "-R", "18080:127.0.0.1:18080",  # reverse: sandbox:18080 -> local:18080
            "-N",  # no command, just tunnel
            f"root@{sid}",
        ]
        # Start tunnel in background
        tunnel_proc = subprocess.Popen(
            tunnel_cmd, stdin=subprocess.PIPE,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
        time.sleep(1)
        tunnel_proc.stdin.write(b"e2bpass\n")
        tunnel_proc.stdin.flush()
        time.sleep(3)

        # 7. Test: from sandbox, curl localhost:18080 should reach our local
        # First start a simple HTTP server on local:18080
        import threading, http.server
        class Handler(http.server.BaseHTTPRequestHandler):
            def do_GET(self):
                self.send_response(200)
                self.end_headers()
                self.wfile.write(b"hello from local via SSH tunnel!")
            def log_message(self, *a): pass
        srv = http.server.HTTPServer(("127.0.0.1", 18080), Handler)
        threading.Thread(target=srv.serve_forever, daemon=True).start()
        print("local test server on :18080")

        r4 = h.run("curl -s http://localhost:18080/ 2>&1 || echo CURL_FAIL; echo END")
        print(f"sandbox -> localhost:18080: {r4['stdout'] if r4 else 'err'}")

        srv.shutdown()
        tunnel_proc.kill()
    else:
        print("❌ SSH failed, cannot test tunnel")
