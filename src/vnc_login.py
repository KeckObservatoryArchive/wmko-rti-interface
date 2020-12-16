import re
from telnetlib import Telnet
import sshtunnel
import subprocess

def open_firewall(addy, port, user, password):
    tn = Telnet(addy, port)
    tn.read_until(b"User: ", timeout=5)
    tn.write(f'{user}\n'.encode('ascii'))
    tn.read_until(b"password: ", timeout=5)
    tn.write(f'{password}\n'.encode('ascii'))
    tn.read_until(b"Enter your choice: ", timeout=5)
    tn.write('1\n'.encode('ascii'))
    result = tn.read_all().decode('ascii')
    if re.search('User authorized for standard services', result):
        print('User authorized for standard services')
        return True
    else:
        print("ERROR: ", result)
        return False

def is_local_port_in_use(port):
    cmd = f'lsof -i -P -n | grep LISTEN | grep ":{port} (LISTEN)" | grep -v grep'
    print(f'Checking for port {port} in use: ' + cmd)
    proc = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE)
    data = proc.communicate()[0]
    data = data.decode("utf-8").strip()
    if len(data) == 0: return False
    else:              return True

def open_ssh_tunnel(server, username, password, 
                    remote_port, local_port, ssh_pkey=None):
    #check port in use
    if is_local_port_in_use(local_port):
        print(f"ERROR: Local port {local_port} is in use.")
        return
    #log
    address_and_port = f"{username}@{server}:{remote_port}"
    print(f"Opening SSH tunnel for {address_and_port} "
          f"on local port {local_port}.")
    #open ssh tunnel
    try:
        thread = sshtunnel.SSHTunnelForwarder(
            server,
            ssh_username=username,
            ssh_password=password,
            ssh_pkey=ssh_pkey,
            remote_bind_address=('127.0.0.1', remote_port),
            local_bind_address=('0.0.0.0', local_port),
        )
        thread.start()
        return thread, local_port
    except Exception as e:
        print(f"ERROR: Failed to open SSH tunnel for "
              f"{username}@{server}:{remote_port} "
              f"on local port {local_port}.")
        return False

def open_vncviewer(vncviewer, vncserver, port, vncargs=None):
    cmd = [vncviewer]
    if vncargs:
        vncargs = vncargs.split()
        cmd = cmd + vncargs
    cmd.append(f'{vncserver}:{port:4d}')
    print(f"VNC viewer command: {' '.join(cmd)}")
    null = subprocess.DEVNULL
    proc = subprocess.Popen(cmd, stdin=null, stdout=null, stderr=null)
    proc.wait()

if __name__ == "__main__":
    #poke firewall
    fw_addy = '128.171.99.33'
    fw_port = 259
    fw_user = 'staff'
    fw_pass = '$$hT3$t!'
    open_firewall(fw_addy, fw_port, fw_user, fw_pass)
    #open ssh tunnel
    local_port    = 5907
    remote_port   = 5907
    remote_server = 'vm-koaserver2.keck.hawaii.edu'
    ssh_user      = 'ttucker'
    ssh_pass      = 'thebest4u'
    ssh_key_path  = None
    tunnel, port = open_ssh_tunnel(remote_server, ssh_user, ssh_pass, 
                    remote_port, local_port, ssh_key_path)
    #open vnc
    vncviewer = '/usr/bin/vncviewer'
    vncargs = None
    #vncviewer = '/Applications/TigerVNC Viewer 1.10.1.app/Contents/MacOS/TigerVNC Viewer'
    #vncargs = '-Shared -passwd=/Users/jriley/.vnc/passwd-keck-jriley'
    open_vncviewer(vncviewer, 'localhost', local_port, vncargs)
    print("DONE")
    tunnel.stop()