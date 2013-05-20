# coding: utf-8

import os
import sys
import sqlite3
import logging
import hashlib
import argparse
import ConfigParser
import tarfile
import tempfile
import datetime
import boto

class GlacierSyncConfigException(Exception):
    pass
    
class GlacierSyncUnexpectedError(Exception):
    pass

# create sha1 hash of a file without consuming too much memory for large files
def _filehash(filename):
    sha1 = hashlib.sha1()
    with open(filename,'rb') as f:
        for chunk in iter(lambda: f.read(128*sha1.block_size), b''): 
             sha1.update(chunk)
    return sha1.hexdigest()

# if you have problems with boto you can call this function to enable boto logging
def _enableBotoDebugLogging():
    logger = logging.getLogger('boto')
    logger.level = logging.DEBUG
    logger.addHandler(logging.StreamHandler(sys.stdout))

class Main():
    def _parseConfig(self,configpath):
        data={
            'profiles':{}
        }
        path=os.path.abspath(configpath)
        with open(path,'r') as f:
            config=ConfigParser.ConfigParser()
            config.readfp(f)
            for section_name in config.sections():
                if section_name=='global':
                    for k,v in config.items(section_name): data[k]=v
                else:
                    profile={}
                    for k,v in config.items(section_name): profile[k]=v
                    data['profiles'][section_name]=profile
        return data

    def _create_db(self):
        cur=self._db.cursor()
        try:
            cur.execute("""
                CREATE TABLE glaciersync_files (
                    profile text,
                    basepath text,
                    fname text,
                    mtime real,
                    bytesize integer,
                    archive_id text
                );
            """)
            cur.execute("""
                CREATE INDEX glaciersyncfiles_profile_basepath_fname
                ON glaciersync_files 
                (profile,basepath,fname);
            """)
            cur.execute("""
                CREATE TABLE glaciersync_hashes (
                    archive_id text,
                    hash text
                );
            """)
            cur.execute("""
                CREATE INDEX glaciersynchashes_hash
                ON glaciersync_hashes
                (hash);
            """)
            cur.execute("""
                CREATE TABLE glaciersync_tars (
                    archive_id text,
                    mtime real
                );
            """)
            cur.execute("""
                CREATE TABLE glaciersync_tar_files (
                    tar_rowid integer,
                    file_rowid integer
                );
            """)
        except sqlite3.OperationalError, msg:
            if str(msg)!='table glaciersync_files already exists':
                raise

        
    def _connect_db(self):
        if not self._config.has_key('database_dir'):
            raise GlacierSyncConfigException('The following configuration key is required: "%s"'%key)
        self._db=sqlite3.connect(
            os.path.abspath(os.path.join(
                self._config['database_dir'],
                self._profile_name+'.sqlite3'
            ))
        )
        self._create_db()
    
    def _connect_glacier(self):
        for key in ['aws_access_key','aws_secret_key','aws_region_name']:
            if not self._config.has_key(key):
                raise GlacierSyncConfigException('The following configuration key is required: "%s"'%key)
        if not self._profile.has_key('vault_name'):
            raise GlacierSyncConfigException('The following profile configuration key is required: %s'%msg)
        self._glacier_connection=boto.connect_glacier(
            aws_access_key_id=self._config['aws_access_key'],
            aws_secret_access_key=self._config['aws_secret_key'],
            region_name=self._config['aws_region_name'],
        )
        self._glacier_vault=self._glacier_connection.create_vault(self._profile['vault_name'])

    def _get_archive_id(self,fname):
        fhash=_filehash(fname)
        cur=self._db.cursor()
        cur.execute("""
            select archive_id from glaciersync_hashes
            where hash=?
        """,(fhash,))
        data=cur.fetchall()
        if len(data)>0:
            print('hash already exists, no upload necessary')
            archive_id=data[0][0]
        else:
            print('uploading to glacier')
            archive_id = self._glacier_vault.upload_archive(fname,fhash)
            #archive_id = 'fake'
            print('inserting into hashes (archive_id=%s)'%archive_id)
            cur.execute("""
                insert into glaciersync_hashes (archive_id,hash)
                values (?,?)
            """,(archive_id,fhash))
            self._db.commit()
        return archive_id

    def _insert_file(self,basepath,relfname,fmtime,fsize,archive_id):
        cur=self._db.cursor()
        cur.execute("""
            insert into glaciersync_files
            (profile,basepath,fname,mtime,bytesize,archive_id)
            values (?,?,?,?,?,?)
        """,(self._profile_name,basepath,relfname,fmtime,fsize,archive_id))
        self._db.commit()

    def _update_file(self,basepath,relfname,fmtime,fsize,archive_id):
        cur=self._db.cursor()
        cur.execute("""
            update glaciersync_files
            set mtime=?,bytesize=?,archive_id=?
            where profile=? and basepath=? and fname=?
        """,(fmtime,fsize,archive_id,self._profile_name,basepath,relfname))
        self._db.commit()
        
    def _get_file_action(self,basepath,relfname,fmtime,fsize):
        cur=self._db.cursor()
        cur.execute("""
            select mtime,bytesize
            from glaciersync_files 
            where profile=? and basepath=? and fname=?
        """,(self._profile_name,basepath,relfname))
        need_insert=False
        need_update=False
        data=cur.fetchall()
        sys.stdout.write('.')
        sys.stdout.flush()
        if len(data)==0:
            need_insert=True
            print("\nnew file")
        elif len(data)>0 and (data[0][0]!=fmtime or data[0][1]!=fsize):
            need_update=True
            print("\nupdated file")
        return (need_insert,need_update)
    
    def _flush_profile_tar(self):
        if len(self._profile_current_tar)==1:
            print("\ntar only contains 1 file - uploading directly")
            (need_insert,need_update,basepath,relfname,fname,fmtime,fsize)=self._profile_current_tar[0]
            archive_id=self._get_archive_id(fname)
            if need_insert:
                self._insert_file(basepath,relfname,fmtime,fsize,archive_id)
            if need_update:
                self._update_file(basepath,relfname,fmtime,fsize,archive_id)
            self._any_changes=True
        elif len(self._profile_current_tar)>0:
            print('\nupload tar with %i files'%len(self._profile_current_tar))
            f=tempfile.NamedTemporaryFile(delete=False)
            tempfname=f.name
            f.close()
            with tarfile.open(tempfname, "w") as tar:
                for need_insert,need_update,basepath,relfname,fname,fmtime,fsize in self._profile_current_tar:
                    tar.add(fname)
            tempfmtime=os.path.getmtime(fname)
            archive_id=self._get_archive_id(fname)
            os.unlink(fname)
            cur=self._db.cursor()
            cur.execute("""
                insert into glaciersync_tars (archive_id, mtime)
                values (?,?)
            """,(archive_id,tempfmtime))
            tar_rowid=cur.lastrowid
            self._db.commit()
            for need_insert,need_update,basepath,relfname,fname,fmtime,fsize in self._profile_current_tar:
                if need_insert:
                    print("insert file")
                    self._insert_file(basepath,relfname,fmtime,fsize,'')
                if need_update:
                    print("update file")
                    self._update_file(basepath,relfname,fmtime,fsize,'')
                cur.execute("""
                    select rowid from glaciersync_files
                    where profile=? and basepath=? and fname=?
                """,(self._profile_name,basepath,relfname))
                data=cur.fetchall()
                file_rowid=data[0][0]
                cur.execute("""
                    insert into glaciersync_tar_files (tar_rowid,file_rowid)
                    values (?,?)
                """,(tar_rowid,file_rowid))
                self._db.commit()
            self._profile_current_tar=[]
            self._profile_current_tar_size=0
            self._any_changes=True

    def _process_tar_file(self,need_insert,need_update,basepath,relfname,fname,fmtime,fsize):
        if need_insert:
            print("\nnew file delayed for tar")
        else:
            print("\nupdated file delayed for tar")
        self._profile_current_tar_size=self._profile_current_tar_size+fsize
        self._profile_current_tar.append((need_insert,need_update,basepath,relfname,fname,fmtime,fsize))
        if self._profile_current_tar_size>self._byte_threshold:
            self._flush_profile_tar()
    
    def _process_file(self,fname,basepath):
        relfname=fname.replace(basepath+os.sep,'',1)
        fmtime=os.path.getmtime(fname)
        fsize=os.path.getsize(fname)
        (need_insert,need_update)=self._get_file_action(basepath,relfname,fmtime,fsize)
        if need_insert or need_update:
            if fsize<=self._byte_threshold:
                self._process_tar_file(need_insert,need_update,basepath,relfname,fname,fmtime,fsize)
            else:
                archive_id=self._get_archive_id(fname)
                if need_insert:
                    self._insert_file(basepath,relfname,fmtime,fsize,archive_id)
                if need_update:
                    self._update_file(basepath,relfname,fmtime,fsize,archive_id)
            self._any_changes=True
            print('done')
        
    def _walk_path(self,basepath):
        print('#### processing path "%s"'%basepath)
        basepath=unicode(basepath)
        for path, dirs, files in os.walk(basepath):
            for file in files:
                self._process_file(os.path.join(path,file),basepath)
    
    def _upload_db(self):
        print('uploading db to glacier')
        archive_description='glaciersync sqlite3 db '+str(datetime.datetime.now())
        fname=os.path.abspath(os.path.join(
            self._config['database_dir'],
            self._profile_name+'.sqlite3'
        ))
        archive_id = self._glacier_vault.upload_archive(fname,archive_description)
        #archive_id = 'fake'
        print('db archive id = %s'%archive_id)
    
    def _run_profile(self,profile_name):
        print('## processing profile "%s"'%profile_name)
        profile=self._config['profiles'][profile_name]
        self._profile=profile
        self._profile_name=profile_name
        self._connect_db()
        self._connect_glacier()
        self._byte_threshold=int(self._profile.get('archive_byte_size_threshold','0'))
        self._any_changes=False
        for k in profile:
            if k.startswith('path'):
                self._profile_current_tar=[]
                self._profile_current_tar_size=0
                path=profile[k]
                # must use unicode for os.walk to return unicode results (on windows)
                self._walk_path(os.path.abspath(path))
                self._flush_profile_tar()
        if self._any_changes:
            self._upload_db()
        self._db.close()
        
    def __init__(self,configpath):
        self._config=self._parseConfig(configpath)
    
    def run(self,profile_name=None):
        if profile_name is None:
            print('running for all profiles')
            for profile_name in self._config['profiles']:
                self._run_profile(profile_name)
        else:
            self._run_profile(profile_name)

parser = argparse.ArgumentParser()
parser.add_argument('-c', '--config', help="path to config file", required=True)
parser.add_argument('-p', '--profile', help="name of profile to run (otherwise runs all profiles)", default=None)
args=parser.parse_args()
main=Main(args.config)
main.run()