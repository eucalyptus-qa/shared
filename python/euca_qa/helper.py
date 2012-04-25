#!/usr/bin/python

import euca_qa
import time
import os

class AbstractHelper(object):
    _config = None

    def __init__(self, config=None):
        self._config = config

    @property
    def config(self):
        if self._config is None:
            self._config = euca_qa.read_test_config()
        return self._config

    def run(self):
        raise NotImplementedError

class EnableComponents(AbstractHelper):
    def run(self):
        self.enable_cloud()
        self.enable_walrus()
        self.enable_sc()
        self.enable_vmbroker()

    def enable_cloud(self):
        for host in self.config['hosts']:
            for role in host.roles:
                if role.startswith('clc'):
                    host.run_command('%s/usr/sbin/euca_conf --enable cloud' % host.eucalyptus)

    def enable_sc(self):
        for host in self.config['hosts']:
            for role in host.roles:
                if role.startswith('sc'):
                    host.run_command('%s/usr/sbin/euca_conf --enable sc' % host.eucalyptus)

    def enable_walrus(self):
        for host in self.config['hosts']:
            for role in host.roles:
                if role.startswith('walrus'):
                    host.run_command('%s/usr/sbin/euca_conf --enable walrus' % host.eucalyptus)

    def enable_vmbroker(self):
        for k, v in self.config['hosts'].get_vb_groups():
            for host in v:
                host.run_command('%s/usr/sbin/euca_conf --enable vmwarebroker' % host.eucalyptus)
            

class StartComponents(AbstractHelper):
    def run(self):
        self.start_cc()
        self.start_nc()
        enabler = EnableComponents(config=self.config)
        enabler.run()
        self.start_cloud_components()

    def start_cc(self):
        ccgroups = self.config['hosts'].get_cc_groups()
        for g in sorted(ccgroups.keys()):
            for host in ccgroups[g]:
                if not host.is_running('eucalyptus-cc'):
                    result = host.start_service('eucalyptus-cc')
                    # sleep ?

    def start_nc(self):
        ccgroups = self.config['hosts'].get_cc_groups()
        for g in sorted(ccgroups.keys()):
            for host in ccgroups[g]:
                if host.dist == 'vmware':
                    continue
                if not host.is_running('eucalyptus-nc'):
                    host.start_service('eucalyptus-nc')
                
    def start_cloud_components(self):
        for host in self.config['hosts']:
            for role in host.roles:
                if (role.startswith("clc") or role.startswith("ws") or 
                    role.startswith("sc") or role.startswith("vb")):
                    host.start_service('eucalyptus-cloud')
                    break

class StopComponents(AbstractHelper):
    def run(self):
        self.stop_cc()
        self.stop_nc()
        self.stop_cloud_components()

    def stop_cc(self):
        ccgroups = self.config['hosts'].get_cc_groups()
        for g in sorted(ccgroups.keys()):
            for host in ccgroups[g]:
                if host.is_running('eucalyptus-cc'):
                    result = host.stop_service('eucalyptus-cc')
                    # sleep ?

    def stop_nc(self):
        ccgroups = self.config['hosts'].get_cc_groups()
        for g in sorted(ccgroups.keys()):
            for host in ccgroups[g]:
                if host.dist == 'vmware':
                    continue
                if host.is_running('eucalyptus-nc'):
                    host.stop_service('eucalyptus-nc')
                
    def stop_cloud_components(self):
        for host in self.config['hosts']:
            for role in host.roles:
                if (role.startswith("clc") or role.startswith("ws") or 
                    role.startswith("sc") or role.startswith("vb")):
                    host.stop_service('eucalyptus-cloud')
                    break

class SyncKeys(AbstractHelper):
    def run(self):
        for host in self.config['hosts']:
            for role in host.roles:
                if role.startswith('clc'):
                    host.run_command("sed -i 's/#   StrictHostKeyChecking ask/   StrictHostKeyChecking no/' /etc/ssh/ssh_config")
                    if os.path.exists('./id_rsa.pub.clc'):
                        os.remove('./id_rsa.pub.clc')
                    host.getfile('/root/.ssh/id_rsa.pub', './id_rsa.pub.clc')
                    for h in self.config['hosts']:
                        h.putfile('./id_rsa.pub.clc', '/root/id_rsa.pub.clc')
                        h.run_command('cat /root/id_rsa.pub.clc >> /root/.ssh/authorized_keys')

                if role.startswith('cc'):
                    host.run_command("sed -i 's/#   StrictHostKeyChecking ask/   StrictHostKeyChecking no/' /etc/ssh/ssh_config")
                    if os.path.exists('./id_rsa.pub.cc'):
                        os.remove('./id_rsa.pub.cc')
                    host.getfile('/root/.ssh/id_rsa.pub', './id_rsa.pub.cc')
                    for h in self.config['hosts']:
                        h.putfile('./id_rsa.pub.cc', '/root/id_rsa.pub.cc')
                        h.run_command('cat /root/id_rsa.pub.clc >> /root/.ssh/authorized_keys')

class DisableDNS(AbstractHelper):
    def run(self):
        ret = 0
        for host in self.config['hosts']:
            if host.has_role('clc'):
                if host.getVersion().startswith('2.') or host.getVersion().startswith('eee-2.'):
                    print "%s : Disabling DNS on this Cloud-Controller\n" % host.ip
                    ret |= host.run_command("""sed -i 's/DISABLE_DNS="Y"/DISABLE_DNS="N"/' %s/etc/eucalyptus/eucalyptus.conf""" % host.eucalyptus)
                    ret |= host.stop_service("eucalyptus-cloud")
                    ret |= host.start_service("eucalyptus-cloud")
                    break
                else:
                    host.run_command("""i=20; while [ $i -gt 0 ]; do rm admin_cred.zip; euca_conf --get-credentials admin_cred.zip; 
                                        if [ $( du admin_cred.zip | cut -f1 ) -gt 0 ]; then break; fi; sleep 3; i=$(( $i - 1 )); done""")
                    host.run_command(""". eucarc; euca-modify-property -p bootstrap.webservices.use_instance_dns=false""")

        if ret > 0:
            print "[TEST REPORT] FAILED to disable DNS"
        else:
            print "[TEST REPORT] SUCCESS"
        time.sleep(30)
        return ret

class EnableDNS(AbstractHelper):
    def run(self):
        ret = 0
        for host in self.config['hosts']:
            if host.has_role('clc'):
                if host.getVersion().startswith('2.') or host.getVersion().startswith('eee-2.'):
                    print "%s : Setting up DNS on this Cloud-Controller\n" % host.ip
                    ret |= host.run_command("""sed -i 's/DISABLE_DNS="Y"/DISABLE_DNS="N"/' %s/etc/eucalyptus/eucalyptus.conf || true""" % host.eucalyptus)
                    ret |= host.stop_service("eucalyptus-cloud")
                    ret |= host.run_command("pkill dnsmasq || true")
                    ret |= host.start_service("eucalyptus-cloud")
                    break
                else:
                    host.run_command('i=20; while [ $i -gt 0 ]; do rm admin_cred.zip; euca_conf --get-credentials admin_cred.zip; if [ $( du admin_    cred.zip | cut -f1 ) -gt 0 ]; then break; fi; sleep 3; i=$(( $i - 1 )); done')
                    host.run_command(""". eucarc; euca-modify-property -p bootstrap.webservices.use_instance_dns=true""")

        if ret > 0:
            print "[TEST REPORT] FAILED to enable DNS"
        else:
            print "[TEST REPORT] SUCCESS"
        time.sleep(30)
        return ret

