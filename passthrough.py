#!/usr/bin/env python

from __future__ import with_statement

import os
import sys, traceback
import errno
import json
import base64
from pathlib import Path
from signal import signal, SIGINT
import time
import random

from fuse import FUSE, FuseOSError, Operations, fuse_get_context

mypassthrough = None

def handler(signal_received, frame):
    # on gère un cleanup propre
    print('')
    print('SIGINT or CTRL-C detected. Exiting gracefully')
    mypassthrough._release_lock()
    exit(0)


class Passthrough(Operations):
    def __init__(self, root):
        self.root = root
        self.current_lock = 0
        self.base_64_name = base64.b64encode(root.encode('utf8')).decode("ascii")
        self.shadow_root = self.base_64_name+"/"
        to_remove = len(root)
        if not os.path.isdir(self.shadow_root):  
                      
            rand = - random.randint(1,1000)
            self._turn_on(rand)
            for relative_root, dirs, files in os.walk(root, topdown=False):
                relative_root = relative_root[to_remove:]
                shadow_root = os.path.join(self.shadow_root, relative_root)
                if os.path.isfile(shadow_root) or os.path.isfile(os.path.join(shadow_root, "._attrs_18ab")):
#                     print(relative_root+" already there")
                     continue
                os.makedirs(shadow_root, exist_ok =True)
                f = open(os.path.join(shadow_root, "._attrs_18ab"), "w")
                towrite = {}
                try:
                    towrite['attr'] = self._fs_getattr(relative_root)
                    towrite['stat'] = self._fs_statfs(relative_root)
                except Exception as e:
                    continue
                f.write(json.dumps(towrite))
                f.close()
                for name in files:
                    path = os.path.join(relative_root, name)
                    try:
                        self._refresh(path)
                    except Exception as e:
                        continue
                for name in dirs:
                    path = os.path.join(relative_root, name)
                    try:
                        self._refresh(path)
                    except Exception as e:
                        continue
            self._release_lock(rand)


        print("listing finished")

    # Helpers
    # =======
    def _turn_on(self, lock_number):
        self._get_lock(lock_number)
        while (not os.path.ismount(self.root)):
            time.sleep(2)
            
        return
        
    def _get_lock(self, fd=None):
        if(fd==None):
            self.current_lock = self.current_lock + 1
            print("holding "+"hddlock/"+self.base_64_name+"_"+str(self.current_lock))
            Path("hddlock/"+self.base_64_name+"_"+str(self.current_lock)).touch()
        else:
            print("holding "+"hddlock/"+self.base_64_name+"_fd_"+str(fd))
            Path("hddlock/"+self.base_64_name+"_fd_"+str(fd)).touch()
        
        
    def _release_lock(self, fd=None):
        if(fd==None):
            print("releasing "+"hddlock/"+self.base_64_name+"_"+str(self.current_lock))
            os.rename("hddlock/"+self.base_64_name+"_"+str(self.current_lock), "hddlock/released_"+self.base_64_name+"_"+str(self.current_lock))
            self.current_lock = self.current_lock - 1
        else:
            print("releasing hddlock/"+self.base_64_name+"_fd_"+str(fd))
            os.rename("hddlock/"+self.base_64_name+"_fd_"+str(fd), "hddlock/released_"+self.base_64_name+"_fd_"+str(fd))
        
    def _refresh(self, path):
        shadow_path = self._full_shadow_path(path)
        towrite = {}
        #print("refresh "+path+" to shadow_path " +shadow_path)
        towrite['attr'] = self._fs_getattr(path)
        towrite['stat'] = self._fs_statfs(path)
        if(os.path.isdir(self._full_path(path))):
            os.makedirs(shadow_path, exist_ok =True)
            f = open(os.path.join(shadow_path,"._attrs_18ab"), "w")
            f.write(json.dumps(towrite))
            f.close()
        else:
            f = open(shadow_path, "w")
            #print("json to write "+json.dumps(towrite))
            f.write(json.dumps(towrite))
            f.close()
        return towrite

    def _full_path(self, partial):
        if partial.startswith("/"):
            partial = partial[1:]
        path = os.path.join(self.root, partial)
        return path
    def _full_shadow_path(self, partial):
        if partial.startswith("/"):
            partial = partial[1:]
        path = os.path.join(self.shadow_root, partial)
        return path

    # Filesystem methods
    # ==================

    def access(self, path, mode):
        print("access "+path)
        full_path = self._full_path(path)
        if not os.access(full_path, mode):
            raise FuseOSError(errno.EACCES)

    def chmod(self, path, mode):
        print("chmod")
        full_path = self._full_path(path)
        rand = - random.randint(1,1000)
        self._turn_on(rand)
        try:
            ret = os.chmod(full_path, mode)
            self._refresh(path)
            self._release_lock(rand)
            return ret
        except Exception as e:
            self._refresh(path)
            self._release_lock(rand)
            raise e

    def chown(self, path, uid, gid):
        print("chown")
        full_path = self._full_path(path)
        rand = - random.randint(1,1000)
        self._turn_on(rand)
        try:
            ret = os.chown(full_path, uid, gid)
            self._refresh(path)
            self._release_lock(rand)
            return ret
        except Exception as e:
            self._refresh(path)
            self._release_lock(rand)
            raise e
        

    def getattr(self, path, fh=None):
        print("getattr "+path)

        full_path = self._full_shadow_path(path)
        if os.path.isdir(full_path):
            with open(os.path.join(full_path, "._attrs_18ab")) as f:
                return json.load(f)['attr']
        elif os.path.isfile(full_path):
            with open(full_path) as f:
                return json.load(f)['attr']
        else:
            return self._refresh(path)['attr']

                     
    def _fs_getattr(self, path, fh=None):
        rand = - random.randint(1,1000)
        self._turn_on(rand)
        full_path = self._full_path(path)
        try:
            st = os.lstat(full_path)
            self._release_lock(rand)
            return dict((key, getattr(st, key)) for key in ('st_atime', 'st_ctime',
                     'st_gid', 'st_mode', 'st_mtime', 'st_nlink', 'st_size', 'st_uid'))
        except Exception as e:
            self._release_lock(rand)
            print("error")
            raise e
            return None


    def readdir(self, path, fh):
        print("readdir")
        full_path = self._full_shadow_path(path)
        dirents = ['.', '..']
        if os.path.isdir(full_path):
            dirents.extend(os.listdir(full_path))
            dirents.remove("._attrs_18ab")
        for r in dirents:
            yield r

    def readlink(self, path):
        print("readlink")
        rand = - random.randint(1,1000)
        self._turn_on(rand)
        pathname = os.readlink(self._full_path(path))
        if pathname.startswith("/"):
            # Path name is absolute, sanitize it.
            self._release_lock(rand)
            return os.path.relpath(pathname, self.root)
        else:
            self._release_lock(rand)
            return pathname

    def mknod(self, path, mode, dev):
        print("mknod")
        rand = - random.randint(1,1000)
        self._turn_on(rand)
        ret = os.mknod(self._full_path(path), mode, dev)
        self._refresh(path)
        self._release_lock(rand)
        return ret

    def rmdir(self, path):
        print("rmdir")
        rand = - random.randint(1,1000)
        print("rmdir1")
        self._turn_on(rand)
        full_path = self._full_path(path)
        ret = os.rmdir(full_path)
        print("rmdi2")
        os.remove(os.path.join(self._full_shadow_path(path), "._attrs_18ab"))
        os.rmdir(self._full_shadow_path(path))
        print("rmdir3")
        self._release_lock(rand)
        print("rmdir4")
        return ret

    def mkdir(self, path, mode):
        print("mkdir")
        rand = - random.randint(1,1000)
        self._turn_on(rand)
        ret = os.mkdir(self._full_path(path), mode)
        self._refresh(path)
        self._release_lock(rand)
        return ret

    def statfs(self, path):
        print("statfs")
        full_path = self._full_shadow_path(path)
        if os.path.isdir(full_path):
            with open(os.path.join(full_path, "._attrs_18ab")) as f:
                return json.load(f)['stat']
        else:
            with open(full_path) as f:
                return json.load(f)['stat']
            
    def _fs_statfs(self, path):
        rand = - random.randint(1,1000)
        self._turn_on(rand)
        full_path = self._full_path(path)
        try:
            stv = os.statvfs(full_path)
            self._release_lock(rand)
            return dict((key, getattr(stv, key)) for key in ('f_bavail', 'f_bfree',
                'f_blocks', 'f_bsize', 'f_favail', 'f_ffree', 'f_files', 'f_flag',
                'f_frsize', 'f_namemax'))
        except Exception as e:
            self._release_lock(rand)
            print("error")
            raise e
            return None
    def unlink(self, path):
        print("unlink")
        rand = - random.randint(1,1000)
        self._turn_on(rand)
        try:
            ret = os.unlink(self._full_path(path))
            ret = os.unlink(self._full_shadow_path(path))
            self._release_lock(rand)
            return ret
        except Exception as e:
            self._release_lock(rand)
            traceback.print_exc(file=sys.stdout)
            print("error")
            raise e
        

    def symlink(self, name, target):
        print("symlink")
        rand = - random.randint(1,1000)
        self._turn_on(rand)
        ret = os.symlink(target, self._full_path(name))
        self._refresh(name)
        self._refresh(target)
        self._release_lock(rand)
        return ret

    def rename(self, old, new):
        print("rename")
        rand = - random.randint(1,1000)
        self._turn_on(rand)
        ret = os.rename(self._full_path(old), self._full_path(new))
        os.remove(self._full_shadow_path(old), "._attrs_18ab")
        self._refresh(new)
        self._release_lock(rand)
        return ret

    def link(self, target, name):
        print("link")
        rand = - random.randint(1,1000)
        self._turn_on(rand)
        ret = os.link(self._full_path(name), self._full_path(target))
        self._release_lock(rand)
        return ret

    def utimens(self, path, times=None):
        print("utimens")
        rand = - random.randint(1,1000)
        self._turn_on(rand)
        ret = os.utime(self._full_path(path), times)
        self._release_lock(rand)
        return ret

    # File methods
    # ============

    def open(self, path, flags):
        print("open "+path)
        rand = - random.randint(1,1000)
        self._turn_on(rand)
        full_path = self._full_path(path)
        try:
            ret = os.open(full_path, flags)
        except Exception as e:
            print("error")
            self._release_lock(rand)
            raise e
        self._get_lock(ret)
        self._release_lock(rand) #release lock of turn_on
        print("fd "+str(ret))
        self._refresh(path)
        return ret

    def create(self, path, mode, fi=None):
        print("create")
        
        rand = - random.randint(1,1000)
        self._turn_on(rand)
        uid, gid, pid = fuse_get_context()
        full_path = self._full_path(path)
        try:
            fd = os.open(full_path, os.O_WRONLY | os.O_CREAT, mode)
            self._get_lock(fd)
            self._release_lock(rand) #release lock of turn_on
            os.chown(full_path,uid,gid) #chown to context uid & gid
            self._refresh(path)
            return fd
        except Exception as e:
            print("error")
            self._release_lock(rand)
            raise e

    def read(self, path, length, offset, fh):
        print("read")
        os.lseek(fh, offset, os.SEEK_SET)
        return os.read(fh, length)

    def write(self, path, buf, offset, fh):
        print("write")
        os.lseek(fh, offset, os.SEEK_SET)
        ret = os.write(fh, buf)
        self._refresh(path)
        return ret

    def truncate(self, path, length, fh=None):
        print("truncate")
        rand = - random.randint(1,1000)
        self._turn_on(rand)
        full_path = self._full_path(path)
        try:
            with open(full_path, 'r+') as f:
                f.truncate(length)
            self._refresh(path)
            self._release_lock(rand)
        except Exception as e:
            print("error")
            self._release_lock(rand)
            raise e

    def flush(self, path, fh):
        print("flush")
        ret = os.fsync(fh)
        self._refresh(path)
        return ret

    def release(self, path, fh):
        print("release "+str(fh))
        
        ret = os.close(fh)
        self._refresh(path)
        self._release_lock(fh)
        return ret

    def fsync(self, path, fdatasync, fh):
        print("fsync")
        ret = self.flush(path, fh)
        self._refresh(path)
        return ret


def main(mountpoint, root):
    mypassthrough = Passthrough(root)
    signal(SIGINT, handler)
    FUSE(mypassthrough, mountpoint, nothreads=True, foreground=True, allow_other=True, nonempty=True)


if __name__ == '__main__':
    main(sys.argv[2], sys.argv[1])
