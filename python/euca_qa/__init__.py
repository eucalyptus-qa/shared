import re
import subprocess
import sys
import time
import paramiko
import arch
import random

__version__ = 1

############ boilerplate exception hook ###############
import bdb
import traceback
try:
    import epdb as debugger
except ImportError:
    import pdb as debugger

def euca_except_hook(debugger_flag, debug_flag):
    def excepthook(typ, value, tb):
        if typ is bdb.BdbQuit:
            sys.exit(1)
        sys.excepthook = sys.__excepthook__

        if debugger_flag and sys.stdout.isatty() and sys.stdin.isatty():
            if debugger.__name__ == 'epdb':
                debugger.post_mortem(tb, typ, value)
            else:
                debugger.post_mortem(tb)
        elif debugger_flag and debugger.__name__ == 'epdb':
            random.seed(); port = random.randint(8100, 9000)
            debugger.serve_post_mortem(tb, typ, value, port=port)
        elif debug_flag:
            traceback.print_exception(typ, value, tb)
            sys.exit(1)
        else:
            print value
            sys.exit(1)

    return excepthook

class RemoteClient(object):
    _transport = None
    _sftpclient = None
    _client = None

    def __init__(self, ip, user='root'):
        self._client = paramiko.SSHClient()
        self._client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        self._client.connect(ip, username=user)

    @property
    def transport(self):
        if self._transport is None:
            self._transport = self._client.get_transport()
        return self._transport

    @property
    def sftpclient(self):
        if self._sftpclient is None:
            self._sftpclient = self._client.open_sftp()
        return self._sftpclient

    def run_command(self, cmd):
        channel = self.transport.open_session()
        channel.exec_command(cmd)    
        out = ""
        err = ""
        while True:    
            while channel.recv_ready():
                data = channel.recv(1024)
                print >> sys.stdout, data
                out += data
            while channel.recv_stderr_ready():
                data = channel.recv_stderr(1024)
                print >> sys.stderr, data
                err += data
                 
            if channel.exit_status_ready():
                break                
            time.sleep(1)        
                 
        return [ channel.recv_exit_status(), out, err ]
        
    def putfile(self, src, dest):
        return self.sftpclient.put(src, dest)

    def getfile(self, src, dest):
        return self.sftpclient.get(src, dest)


class RemoteHost(object):
    def __init__(self, **kwargs):
        """
        TestHost(ip='10.1.1.1', dist='centos', release='5', arch='i686',
                 source='pkgbuild', roles=['clc', 'ws'])
        """
        self.ip         = kwargs['ip']
        self.dist       = kwargs['dist'].lower()
        self.release    = kwargs['release'].lower()
        self.arch       = kwargs.get('arch', 'noarch').lower()
        self.build_type = kwargs.get('build_type', 'source').lower()
        self.roles      = [role.lower() for role in kwargs.get('roles', [])]
        self.ssh_opts   = kwargs.get('ssh_opts',
                                     ['-q', '-o', 'StrictHostKeyChecking=no'])
        self.clients    = {}

        if self.build_type == 'source':
            self.eucalyptus = '/opt/eucalyptus'
        else:
            self.eucalyptus = '/'

        if self.arch == '64':
            if self.dist in ['debian', 'ubuntu']:
                # Debian-style distros call their x86-64 packages amd64 in spite
                # of the fact that it implies athlon compatibility and thus
                # Intel-incompatibility.  However, defaulting to amd64 here will
                # likely work around scripts that assume they mean the same
                # thing.
                self.arch = 'amd64'
            else:
                self.arch = 'x86_64'
        elif self.arch == '32':
            self.arch = 'i686'

    def can_build(self, target_arch, target_dist, target_release):
        """
        Return whether or not this host can build for a given architecture and
        distribution.

        Incompatibilities between distributions can arise because of running
        kernel versions or differences in package-building systems.

        Architecture compatibility examples:
            self.arch  target_arch  Result
            ---------  -----------  ------
            x86_64     x86_64       True
            x86_64     i386         True
            i386       x86_64       False
            i686       i386         True
            x86_64     amd64        False
            amd64      x86_64       True
            ppc64      ppc          True
            (any)      noarch       True
            (any)      None         True
        """
        if target_arch not in arch.getArchList(self.arch) \
                and target_arch is not None:
            return False

        if target_dist is None and target_release is None:
            # Don't care what distro we build for
            return True
        el = ['rhel', 'centos']
        if target_dist.lower() == 'fedora':
            # Fedora's glibc needs Linux >= 2.6.32 to run.
            if self.dist == 'fedora' and 12 <= float(self.release):
                return True
            elif self.dist in el and 6 <= float(self.release):
                return True
        if target_dist.lower() in el and self.dist in el + ['fedora']:
            return True
        if target_dist.lower() == 'opensuse' and \
                self.dist in el + ['fedora', 'opensuse']:
            return True
        if target_dist.lower() == 'debian' and self.dist == 'debian':
            return True
        if target_dist.lower() == 'ubuntu' and self.dist == 'ubuntu':
            return True
        if target_dist.lower() == 'debian' and self.dist == 'ubuntu':
            return True
        return False

    def copy_to(self, src_file, dest_file, user='root'):
        """
        Copy src_file to dest_file on this remote host with rsync.  If src_file
        is a directory it is recursively copied.
        """
        args = ['rsync', '-qrLptH', '-e', 'ssh ' + ' '.join(self.ssh_opts)]
        args.extend([src_file, user + '@' + self.ip + ':' + dest_file])
        self.__call_subprocess(args, allowed_retvals=[0, 24])

    def copy_from(self, src_file, dest_file, user='root'):
        """
        Copy src_file from this remote host to dest_file with rsync.  If
        src_file is a directory it is recursively copied.
        """
        args = ['rsync', '-qrLptH', '-e', 'ssh ' + ' '.join(self.ssh_opts)]
        args.extend([user + '@' + self.ip + ':' + src_file, dest_file])
        self.__call_subprocess(args, allowed_retvals=[0, 24])

    def run_cmd(self, cmd, user='root'):
        """
        Run a command on the remote host.  If the command is unsuccessful this
        will raise a CalledProcessError.
        """
        args = ['/usr/bin/ssh'] + self.ssh_opts + ['-l', user, self.ip, cmd]
        self.__call_subprocess(args)

    def __call_subprocess(self, args, allowed_retvals=[0]):
        """
        Run a command using the subprocess module.
        """
        s = subprocess.Popen(args, stdout=sys.stdout, stderr=sys.stderr)
        s.wait()
        exitcode = s.poll()
        if exitcode not in allowed_retvals:
            raise subprocess.CalledProcessError(exitcode, args)

    def run_command(self, cmd, user='root', return_output=False):
        if not self.clients.has_key(user):
            self.clients[user] = RemoteClient(self.ip, user=user)
        ret, out, err = self.clients[user].run_command(cmd)
        if ret != 0:
            print "[TEST REPORT] FAILED running '%s' on %s" % (cmd, self.ip)
        if return_output:
            return [ ret, out, err ]
        return ret

    def putfile(self, src, dest, user='root'):
        if not self.clients.has_key(user):
            self.clients[user] = RemoteClient(self.ip, user=user)
        try:
            self.clients[user].putfile(src, dest)
            return 0
        except:
            print "[TEST REPORT] FAILED copying %s to %s:%s" % (src, self.ip, dest)
            return 1

    def getfile(self, src, dest, user='root'):
        if not self.clients.has_key(user):
            self.clients[user] = RemoteClient(self.ip, user=user)
        try:
            self.clients[user].getfile(src, dest)
            return 0
        except:
            print "[TEST REPORT] FAILED copying %s:%s to %s:%s" % (self.ip, src, dest)
            return 1

    def install_pkgs(self, pkgs):
        if self.dist in ['fedora', 'centos', 'rhel']:
            return self.run_command('/usr/bin/yum install -y --nogpgcheck %s' %
                             ' '.join(pkgs))
        elif self.dist in ['debian', 'ubuntu']:
            return self.run_command('apt-get -y --force-yes install %s' %
                             ' '.join(pkgs))
        # TODO: openSUSE

    def getVersion(self):
        [ ret, out, err ] = self.run_command('cat %s/etc/eucalyptus/eucalyptus-version' % self.eucalyptus,
                                             return_output=True)
        return out.strip()

    def check_service(self, service):
        [ ret, out, err ] = self.run_command('%s/etc/init.d/%s status' % (self.eucalyptus, service),
                                             return_output=True)
        if out.find("running" != -1):
            return "running"
        if out.find("stopped" != -1):
            return "stopped"
        if out.find("locked" != -1):
            return "locked"

    def is_running(self, service):
        return (self.check_status() == "running")

    def start_service(self, service):
        [ ret, out, err ] = self.run_command('%s/etc/init.d/%s start' % (self.eucalyptus, service),
                                             return_output = True)
        return ret

    def stop_service(self, service):
        [ ret, out, err ] = self.run_command('%s/etc/init.d/%s stop' % (self.eucalyptus, service),
                                             return_output = True)
        return ret

    def has_role(self, role):
        for r in self.roles:
            if re.sub('[0-9]+$', '', r) == role:
                return True
        return False

class HostList(list):
    def get_cc_groups(self):
        return self.getgroups('cc')

    def get_nc_groups(self):
        return self.getgroups('nc')

    def getgroups(self, role):
        groups = {}
        for host in self:
            for r in host.roles:
                if r.startswith(role):
                    group = r[2:]
                    groups.setdefault(group, []).append(host)
                    break
        return groups 

    def get_vb_groups(self):
        ccgroups = self.get_cc_groups()
        ncgroups = self.get_nc_groups()
        vbgroups = {}

        for group in ncgroups.keys():
            if ncgroups[group][0].dist == 'vmware':
                vbgroups[group] = ccgroups[group]
                for host in vbgroups[group]:
                    if 'vb' not in host.roles:
                       host.roles.append('vb')

        return vbgroups

def read_test_config(filename='../input/2b_tested.lst'):
    """
    Parse a test configuration file (usually '../input/2b_tested.lst') and
    return a dictionary with all the values it contains.

    Some possible values include:
        - test_name                     (string)
        - unique_id,   task_id          (string)
        - email                         (string)
        - pxe_type                      ('qaimage' / 'kickstart')
        - build_type                    ('normal')
        - test_type                     ('normal')
        - test_seq,    test_sequence    (string)
        - network,     network_mode     ('managed' / 'static' / 'system')
        - subnet_ip,   internal_subnet  (string)
        - managed_ips, external_ips     (list of strings)
        - bzr_branch,  bzr_branch_uri   (string)
        - git_url                       (string)
        - git_branch                    (string)
        - branch                        (shortname of branch)
        - memo                          (list of strings, one per memo line)
        - hosts                         (list of RemoteHosts)
    """
    config = {'hosts': HostList(), 'esxhosts': [], 'memodict': {}}
    with open(filename) as config_file:
        in_memo = False
        for line in config_file:
            # Memo-handling
            if line.strip().lower() == 'memo':
                in_memo = True
                config.setdefault('memo', [])
            elif line.strip().lower() == 'end_memo':
                in_memo = False
            elif in_memo:
                config['memo'].append(line.strip())
                if line.find('=') == -1:
                    # this line is blank or malformed
                    continue
                k,v = line.strip().split('=', 1)
                config['memodict'][k] = v
            elif _is_host_def(line):
                hostdef = _create_remotehost_from_def(line)
                config['hosts'].append(hostdef)
                if (hostdef.dist == 'vmware'):
                    config['esxhosts'].append(hostdef)
            elif line.lower().startswith('test_name'):
                config['test_name'] = line.strip().split(None, 1)[1]
            elif line.lower().startswith('unique_id') or \
                    line.lower().startswith('task_id'):
                config['unique_id'] = line.strip().split(None, 1)[1]
                config['task_id']   = line.strip().split(None, 1)[1]
            elif line.lower().startswith('email'):
                config['email'] = line.strip().split(None, 1)[1]
            elif line.lower().startswith('pxe_type'):
                config['pxe_type'] = line.strip().split(None, 1)[1].lower()
            elif line.lower().startswith('build_type'):
                config['build_type'] = line.strip().split(None, 1)[1].lower()
            elif line.lower().startswith('test_type'):
                config['test_type'] = line.strip().split(None, 1)[1].lower()
            elif line.lower().startswith('test_seq'):
                config['test_seq']      = line.strip().split(None, 1)[1]
                config['test_sequence'] = line.strip().split(None, 1)[1]
            elif line.lower().startswith('network'):
                config['network']      = line.strip().split(None, 1)[1].lower()
                config['network_mode'] = line.strip().split(None, 1)[1].lower()
            elif line.lower().startswith('subnet_ip') or \
                    line.lower().startswith('internal_subnet'):
                config['subnet_ip'] = line.strip().split(None, 1)[1]
                config['internal_subnet'] = line.strip().split(None, 1)[1]
            elif line.lower().startswith('managed_ips') or \
                    line.lower().startswith('external_ips'):
                config['managed_ips']  = line.strip().split()[1:]
                config['external_ips'] = line.strip().split()[1:]
            elif line.lower().startswith('machine_pool'):
                config['machine_pool'] = line.strip().split()[1:]
            elif line.lower().startswith('bzr_branch'):
                config['bzr_branch']     = line.strip().split(None, 1)[1]
                config['bzr_branch_uri'] = line.strip().split(None, 1)[1]
            elif line.lower().startswith('bzr_revision'):
                config['bzr_revision'] = line.strip().split(None, 1)[1]
                config['revision']     = line.strip().split(None, 1)[1]
                config['revno']        = line.strip().split(None, 1)[1]
            elif line.lower().startswith('git_repo'):
                if line.find("#") != -1:
                    (config['git_url'], config['git_branch']) = line.strip().split(None, 1)[1].split("#")
                else:
                    config['git_url'] = line.strip().split(None, 1)[1]
                    config['git_branch'] = "master"
    return config

def read_inputs(filename='input.txt'):
    """
    Read a processed test configuration in the form that appears on Eucalyptus
    QA test hosts.  (e.g. input.txt)  This is *not* the form that the QA server
    interprets.  (e.g. 2b_tested.lst)
    """
    inputs = {'hosts': [], 'branch': None, 'revno': None, 'esxhosts': []}
    with open(filename) as input_file:
        in_memo = False
        for line in input_file:
            if line.lower().startswith('memo'):
                in_memo = True
                inputs.setdefault('memo', [])
            elif line.lower().startswith('end_memo'):
                in_memo = False
            elif in_memo:
                inputs['memo'].append(line.strip())
            elif line.lower().startswith('testname'):
                inputs['testname']  = line.strip().split(None, 1)[1]
                inputs['test_name'] = line.strip().split(None, 1)[1]
            elif line.lower().startswith('test_seq'):
                inputs['test_seq']      = line.strip().split(None, 1)[1]
                inputs['test_sequence'] = line.strip().split(None, 1)[1]
            elif line.lower().startswith('pxe_type'):
                inputs['pxe_type'] = line.strip().split(None, 1)[1].lower()
            elif line.lower().startswith('network'):
                inputs['network']      = line.strip().split(None, 1)[1].lower()
                inputs['network_mode'] = line.strip().split(None, 1)[1].lower()
            elif line.lower().startswith('bzr_directory'):
                inputs['bzr_directory'] = line.strip().split()[1]
                inputs['branch']        = line.strip().split()[1]
            elif line.lower().startswith('bzr_revision'):
                inputs['bzr_revision'] = line.strip().split()[1]
                inputs['revno']        = line.strip().split()[1]
                inputs['revision']     = line.strip().split()[1]
            elif _is_host_def(line.strip()):
                hostdef = _create_remotehost_from_def(line)
                inputs['hosts'].append(hostdef)
                if (hostdef.dist == 'vmware'):
                    inputs['esxhosts'].append(hostdef)
    return inputs

def _is_host_def(line):
    if 5 < len(line.split()):
        return all(map(re.match, ['(?:\d{1,3}\\.){3}\d{1,3}',  # IPv4
                                  '\w+', '\w+', '\w+', '\w+', '\\[[^\\]]+\\]'],
                                 line.strip().split(None, 5)))
    else:
        return False

def _create_remotehost_from_def(line):
    split_line = line.split(None, 5)
    return RemoteHost(
        ip         = split_line[0],
        dist       = split_line[1],
        release    = split_line[2],
        arch       = split_line[3],
        build_type = split_line[4],
        roles      = split_line[5].strip('[]\n').split())
