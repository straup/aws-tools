#!/usr/bin/env python

import boto
import ConfigParser
import optparse
import logging

import commands
import os
import os.path
import pipes
import sys
import time
import types

class launch:

    def __init__(self, options):

        self.options = options

        self.cfg = ConfigParser.ConfigParser()
        self.cfg.read(options.config)

        self.default_section = 'default'

        self.conn = None

        self.cache_images = {}
        self.cache_groups = {}

        # if you set this to DEBUG, you'll get boto's debugging too

        if options.verbose:
            logging.basicConfig(level=logging.DEBUG)
        else:
            logging.basicConfig(level=logging.INFO)

    def read_config(self, key, section=None):

        value = None

        if self.cfg.has_section(section) and self.cfg.has_option(section, key):
            return self.cfg.get(section, key)

        if self.cfg.has_option(self.default_section, key):
            return self.cfg.get(self.default_section, key)

        logging.warning("undefined config '%s'" % key)
        return None

    def connect(self, create_security_group=True):

        try:

            key = self.read_config('aws_key')
            secret = self.read_config('aws_secret')

            from boto.ec2.connection import EC2Connection
            self.conn = EC2Connection(key, secret)
        except Exception, e:
            logging.error("failed to create EC2 connection: %s" % e)
            return False

        if create_security_group:

            proj = self.read_config('project_name')

            if not self.ensure_security_group(proj, default_ports=True):
                return False

        return True

    def setup_ebs_volume(self, **kwargs):

        instance = kwargs.get('instance', None)

        if not instance:
            logging.error('Required instance parameter missing, can not setup EBS')
            return None

        # We have been passed a volume id, so just
        # try to work with that...

        volume = None
        run_mkfs = True

        if kwargs.get('volume_id', None):

            volume = self.attach_ebs_volume(**kwargs)
            run_mkfs = False

            if not volume:
                return None

        else:

            # Why is it called 'placement'?
            # I have no idea. Blame boto. See also:
            # http://code.google.com/p/boto/source/browse/trunk/boto/ec2/instance.py#156

            kwargs['zone'] = instance.placement

            try:
                volume = self.create_ebs_volume(**kwargs)
                logging.info('created new volume in zone %s' % kwargs['zone'])
            except Exception, e:
                logging.error('failed to create volume in zone %s, %s' % (kwargs['zone'], e))

            if not volume:
                logging.error('failed to create volume, giving up...')
                return None

            if not self.attach_ebs_volume(instance=instance, volume=volume):
                return None

        #

        host = instance.public_dns_name
        mount = self.mount_ebs_volume(host=host, run_mkfs=run_mkfs)

        return volume

    def create_ebs_volume(self, **kwargs):

        size = kwargs.get('size', 1)
        zone = kwargs.get('zone', 'us-east-1d')

        logging.info("creating ebs volume: size: %sGB zone: %s" % (size, zone))

        volume = self.conn.create_volume(size, zone)

        if not volume:
            logging.error('failed to create new volume')
            return None

        logging.info('created new volume with ID %s' % volume.id)

        status = None
        tries = 0
        max_tries = 30

        while status != 'available':

            volumes = self.conn.get_all_volumes([volume.id])
            volume = volumes[0]
            status = volume.status

            logging.info('volume status is %s' % status)

            if status == 'available':
                break

            tries += 1
            if tries >= max_tries:
                logging.error('max tries for volume status has been exceeded')
                return None

            time.sleep(10)

        return volume

    # Note: both the instance and the volume need to be in the same availability zone

    def attach_ebs_volume(self, **kwargs):

        instance = kwargs.get('instance', None)
        device = kwargs.get('device', '/dev/sdf')

        if not instance:
            logging.error('Required EC2 instance argument missing')
            return False

        volume = kwargs.get('volume', None)

        if not volume:
            volume_id = kwargs.get('volume_id', None)

            if not volume_id:
                logging.error('Required EC2 volume or volume_id arguments missing')
                return False

            volumes = self.conn.get_all_volumes(volume_id)

            if len(volumes) == 0:
                logging.error('Faied to locate volume %s' % volume_id)
                return False

            volume = volumes[0]

        if volume.status != 'available':
            logging.error('Volume %s is not available; current status is %s' % (volume.id, volume.status))
            return False

        logging.info('attach EBS volume %s to %s' % (volume.id, device))

        status = self.conn.attach_volume(volume.id, instance.id, device)
        logging.info('volume status: %s' % status)

        # http://groups.google.com/group/boto-users/browse_thread/thread/c4051181a1b8904d
        # http://developer.amazonwebservices.com/connect/thread.jspa?threadID=30362&tstart=0

        while status != 'in-use':
            time.sleep(15)
            volume.update()
            status = volume.status
            logging.info('volume status: %s' % status)

        logging.info('volume is ok to mount!')
        return True

    def mount_ebs_volume(self, **kwargs):

        if not self.options.ssh_key:
            logging.error('No SSH key defined, so unable to mount EBS volume')
            return False

        host = kwargs.get('host', None)

        device = kwargs.get('device', '/dev/sdf')
        mount_point = kwargs.get('mount_point', '/ebs')

        logging.info('mount %s on %s' % (device, mount_point))

        if kwargs.get('run_mkfs', False):
            # -F is for 'force'
            # -L is label
            # -m is number of reserved blocks for root (to work in)

            mkfs_cmd = 'mkfs -F -L CACHE2 -m 10 -t ext3 %s' % device

            status = 1

            # we have to do this kind of crap because even though
            # the volume is marked as 'in-use' it may not actually
            # be ready...

            while status != 0:
                status = self.execute_ssh_command(host, mkfs_cmd)

                if status != 0:
                    logging.info('waiting for EBS device...')
                    time.sleep(15)

        # ok now finish it up...

        self.execute_ssh_command(host, 'mkdir %s' % mount_point)
        self.execute_ssh_command(host, 'mount %s %s' % (device, mount_point))

        return True

    def load_image(self, **kwargs):

        ami = self.read_config('aws_ami')

        if self.cache_images.get(ami, False):
            return self.cache_images[ami]

        try:
            image = self.conn.get_image(ami)
        except Exception, e:
            logging.error("failed to load AMI %s: %s" % (ami, e))
            sys.exit()

        self.cache_images[ ami ] = image
        return image

    def ensure_security_group(self, name, **kwargs):

        if self.cache_groups.get(name, False):
            return self.cache_groups[name]

        groups = self.conn.get_all_security_groups()

        for group in groups :
            if group.name == name:
                return group

        try :
            group = self.conn.create_security_group(name, name)

        except Exception, e :
            logging.error("failed to create security group %s: %s" % (name, e))
            return None

        if kwargs.get('default_ports', False):

            # This rule conveniently add the tcp/udp 1 - 65535 rules
            # WTF.

            group.authorize(ip_protocol='icmp', from_port=-1, to_port=-1, src_group=group)

            group.authorize('icmp', -1, -1, '0.0.0.0/0')
            group.authorize('tcp', 22, 22, '0.0.0.0/0')
            group.authorize('tcp', 80, 80, '0.0.0.0/0')

            logging.info("created new security group %s" % name)

        if name == 'postgres':
            group.authorize(ip_protocol='icmp', from_port=-1, to_port=-1, src_group=group)
            group.authorize('tcp', 5432, 5432, '0.0.0.0/0')

        if name == 'gmond':
            group.authorize('tcp', 8649, 8649, '10.252.154.80/32')
            group.authorize('tcp', 8651, 8651, '10.252.154.80/32')

        self.cache_groups[ name ] = group
        return group

    def launch_instance(self, **kwargs):

        ima = kwargs.get('class', self.default_section)

        # what are we actually launching?

        image = self.load_image(**kwargs)

        if not image:
            return None

        # http://code.google.com/p/boto/source/browse/trunk/boto/ec2/image.py
        # http://code.google.com/p/boto/wiki/EC2InstanceTypes

        project_name = self.read_config('project_name')

        groups = self.read_config('aws_security_groups')

        if groups:
            groups = groups.split(',')
        else:
            groups = []

        if not project_name in groups:
            groups.append(project_name)

        if kwargs.get('groups', False) and type(kwargs['groups']) == types.ListType:
            groups.extend(kwargs['groups'])

        for group_name in groups:
            self.ensure_security_group(group_name)

        instance_type = self.read_config('aws_type')
        keypair = self.read_config('aws_keypair')

        bootstrap_files = self.read_config('userdata_files', ima)
        userdata = self.mk_userdata(bootstrap_files, **kwargs)

        try :
            reservation = image.run(1, 1, keypair, groups, userdata, None, instance_type)
        except Exception, e :
            logging.error("failed to create image: %s" % e)
            return None

        instance = reservation.instances[0]
        status = instance.state
        attempts = 0
        max_attempts = 20

        while status != 'running':
            attempts += 1
            time.sleep(5)

            try:
                instance.update()
            except Exception, e:
                logging.warning("failed to update instance %s: %s" % (instance.id, e))

                if attempts >= max_attempts:
                    logging.error("failed to create instance after %s attempts ...giving up" % max_attempts)
                    return None

            status = instance.state

        instance = self.refresh_instance(instance)
        self.instance_info(ima, instance)

        return instance

    def mk_userdata(self, bootstrap_files, **kwargs):

        if not bootstrap_files:
            return

        bootstrap_files = bootstrap_files.split(',')

        # see also:
        # http://simonwillison.net/2003/Jul/28/simpleTemplates/

        stuff = {}

        if kwargs.get('class', False):
            stuff = { 'machine_class' : kwargs['class'] }

        for name in (self.cfg.sections()):
            for pair in self.cfg.items(name):
                key, value = pair
                key = "%s_%s" % (name, key)
                stuff[ key ] = value

        data = ''

        for fname in bootstrap_files:

            fh = open(fname, 'r')
            data += fh.read() % stuff
            data += '\n'

        data += 'mkdir -p /etc/ec2/'
        data += '\n'
        data += '`date "+%s"` > /etc/ec2/created'
        data += '\n'

        return data

    # This is a hack to account for the fact that the boto _update
    # method doesn't update the private IP when an instance is spun
    # up.

    def refresh_instance(self, instance):

        try:
            id = str(instance.id)
            r = self.conn.get_all_instances(id)
            instance = r[0].instances[0]
        except Exception, e:
            logging.error("failed to refresh instance, %s" % e)
            return None

        return instance

    def instance_info(self, ima, instance):

        logging.info("[%s] instance id: %s" % (ima, instance.id))
        logging.info("[%s] public hostname: %s" % (ima, instance.public_dns_name))
        logging.info("[%s] public IP address: %s" % (ima, instance.ip_address))
        logging.info("[%s] private IP address: %s" % (ima, instance.private_ip_address))

    def ensure_setup(self, launched, check_file='/etc/stamen/created'):

        if not self.options.ssh_key:
            logging.error('No SSH key defined, so unable to ensure userdata setup')
            return False

        ok = 0
        required_ok = len(launched)

        seen = {}

        cycles = 0
        max_cycles = 100

        while ok < required_ok:
            for instance in launched:

                hostname = instance.public_dns_name

                if seen.get(hostname, False):
                    continue

                ssh_command = 'cat %s' % check_file

                status = self.execute_ssh_command(hostname, ssh_command)

                if status == 0:
                    logging.info("%s is up!" % hostname)
                    ok += 1

                    seen[ hostname ] = True

            cycles += 1

            if cycles == max_cycles:
                logging.warning("exceeded max cycles to wait for start up, bailing")
                break

            if ok < required_ok:
                logging.info("sleeping...")
                time.sleep(30)

    def execute_ssh_commands(self, host, ssh_commands, abort_on_error=False):

        for cmd in ssh_commands:

            status = self.execute_ssh_command(host, cmd)

            if abort_on_error:
                return status

    def execute_ssh_command(self, host, cmd):

        ssh_cmd = "ssh -l root -i %s %s \"%s\"" % (self.options.ssh_key, host, cmd)
        logging.info(ssh_cmd)

        (status, out) = commands.getstatusoutput(ssh_cmd)

        if status != 0:
            logging.error("SSH command (%s) failed: %s" % (ssh_cmd, out))

        return status

if __name__ == '__main__' :

    if not boto.Version.startswith('1.9'):
        logging.error("Boto too old. Need 1.9, have %s" % boto.Version)
        sys.exit()

    parser = optparse.OptionParser(usage="""launch.py [options]""")

    parser.add_option('-c', '--config', dest='config',
                        help='...',
                        action='store')

    parser.add_option('-s', '--ssh-key', dest='ssh_key',
                        help='',
                        action='store', default=None)

    parser.add_option('-v', '--verbose', dest='verbose',
                        help='...',
                        action='store_true', default=False)

    # Can has config?

    options, args = parser.parse_args()

    cfg = ConfigParser.ConfigParser()
    cfg.read(options.config)

    ec2 = launch(options)
    ec2.connect()

    instance = ec2.launch_instance()
    ec2.ensure_setup([instance])

    if ec2.read_config('ebs_attach_volume'):

        volume_id = ec2.read_config('ebs_volume_id')
        volume_size = ec2.read_config('ebs_volume_size')

        args = { 'instance' : instance }

        if volume_id:
            args['volume_id'] = volume_id

        if volume_size:
            args['size'] = volume_size

        volume = ec2.setup_ebs_volume(**args)

    #

    logging.info("done")
    sys.exit()

# -*- indent-tabs-mode:nil tab-width:4 -*-
