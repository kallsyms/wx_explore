import os
import shutil
import subprocess
import sys
import tarfile
import tempfile
import urllib.request


def shell(cmd, **kwargs):
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    o, _ = p.communicate()
    return o


def untar(fn, d):
    t = tarfile.open(fn, 'r:*')
    t.extractall(d)
    t.close()


def main(args):
    domain = args.get('domain', 'conus')
    initial_model = args.get('model', 'gfs')
    start_offset = int(args.get('begin', 0))
    end_offset = int(args.get('end', 12))

    log = b''

    if not os.path.exists('/etc/mtab'):
        os.symlink('/proc/self/mounts', '/etc/mtab')

    if args.get('force_reimage', False) and os.path.exists('/usr1'):
        shutil.rmtree('/usr1')

    if not os.path.exists('/usr1'):
        os.mkdir('/usr1')

        uems_tmp, _ = urllib.request.urlretrieve("http://vtxwx-model-data.s3.us-south.cloud-object-storage.appdomain.cloud/uems.tar.gz")
        untar(uems_tmp, '/usr1')

    domain_tarball = os.path.join('/usr1/uems/runs', domain + '.tar.gz')
    if not os.path.exists(domain_tarball):
        urllib.request.urlretrieve("http://vtxwx-model-data.s3.us-south.cloud-object-storage.appdomain.cloud/" + domain + ".tar.gz", domain_tarball)

    with tempfile.TemporaryDirectory(dir='/usr1/uems/runs') as tmp_dir:
        untar(domain_tarball, tmp_dir)

        os.chdir(tmp_dir)
        log += shell(['/bin/bash', '-c', f'. /usr1/uems/etc/EMS.profile; ./ems_prep --dset {initial_model} --cycle :{start_offset}:{end_offset} --nudge'])
        log += shell(['/bin/bash', '-c', '. /usr1/uems/etc/EMS.profile; ./ems_run --nudge'])
        log += shell(['/bin/bash', '-c', '. /usr1/uems/etc/EMS.profile; ./ems_post'])

        os.chdir('/')

    return {"body": log.decode('utf-8')}

if __name__ == "__main__":
    main({a.split('=',1) for a in sys.argv[1:]})
