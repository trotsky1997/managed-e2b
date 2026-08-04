"""SSH to E2B sandbox via websocat — key-based auth."""
import os, time, subprocess, pathlib
os.environ["E2B_API_KEY"] = "***REMOVED***"

from managed_e2b import SandboxLifecycle

# Read our public key
pubkey = pathlib.Path.home().joinpath(".ssh/id_e2b.pub").read_text().strip()

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

    # 2. Configure sshd + add our public key
    h.run('sudo sed -i "s/#PermitRootLogin.*/PermitRootLogin yes/" /etc/ssh/sshd_config')
    h.run('sudo sed -i "s/#PubkeyAuthentication.*/PubkeyAuthentication yes/" /etc/ssh/sshd_config')
    h.run("sudo mkdir -p /root/.ssh && sudo chmod 700 /root/.ssh")
    # Write our public key to authorized_keys
    h.run(f'echo "{pubkey}" | sudo tee /root/.ssh/authorized_keys')
    h.run("sudo chmod 600 /root/.ssh/authorized_keys")
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
    print(f"WSS: {wss_url}")

    # 5. Test SSH with key auth
    ssh_cmd = [
        "ssh",
        "-i", str(pathlib.Path.home() / ".ssh/id_e2b"),
        "-o", "StrictHostKeyChecking=no",
        "-o", "UserKnownHostsFile=/dev/null",
        "-o", "ConnectTimeout=15",
        "-o", f"ProxyCommand=websocat --binary -B 65536 - {wss_url}",
        f"root@{sid}",
        "echo SSH_OK && whoami"
    ]
    print(f"SSH with key auth...")
    result = subprocess.run(ssh_cmd, capture_output=True, text=True, timeout=30)
    print(f"ssh rc: {result.returncode}")
    print(f"ssh stdout: {result.stdout}")
    print(f"ssh stderr: {result.stderr[:300]}")

    if "SSH_OK" in result.stdout:
        print("\n✅ SSH WORKS!")

        # 6. Start reverse tunnel: local:18080 -> sandbox:18080
        import threading, http.server
        class Handler(http.server.BaseHTTPRequestHandler):
            def do_GET(self):
                self.send_response(200)
                self.end_headers()
                self.wfile.write(b"hello from local adapter via SSH tunnel!")
            def log_message(self, *a): pass
        srv = http.server.HTTPServer(("127.0.0.1", 18080), Handler)
        threading.Thread(target=srv.serve_forever, daemon=True).start()
        print("local test server on :18080")

        tunnel_cmd = [
            "ssh",
            "-i", str(pathlib.Path.home() / ".ssh/id_e2b"),
            "-o", "StrictHostKeyChecking=no",
            "-o", "UserKnownHostsFile=/dev/null",
            "-o", f"ProxyCommand=websocat --binary -B 65536 - {wss_url}",
            "-R", "18080:127.0.0.1:18080",
            "-N",
            "-f",  # background
            f"root@{sid}",
        ]
        print("starting reverse tunnel...")
        tunnel_proc = subprocess.Popen(tunnel_cmd, stdin=subprocess.PIPE,
                                      stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        time.sleep(3)

        # 7. Test from sandbox
        r4 = h.run("curl -s http://localhost:18080/ 2>&1 || echo CURL_FAIL; echo END")
        print(f"sandbox -> localhost:18080: {r4['stdout'] if r4 else 'err'}")

        if "hello from local" in (r4["stdout"] if r4 else ""):
            print("\n✅✅ REVERSE TUNNEL WORKS! Sandbox can reach local service!")
        else:
            print("\n❌ tunnel not working")

        srv.shutdown()
        # Kill the tunnel
        subprocess.run(["taskkill", "/f", "/im", "ssh.exe"], capture_output=True)
    else:
        print("❌ SSH failed")
