#!/usr/bin/env python3.6
import os, time, glob, configparser

s3bucket = 's3://backup-databases/'
configfile = '/home/dwipurnomo/python/public/.dbset.ini'
sections = ['mysqlhost1', 'mysqlhost2']


class Mysqlbackup:
    def __init__(self, section):
        self.section = section
        self.cfg = configparser.ConfigParser()
        self.cfg.read(configfile)
        self.dbuser = self.cfg.get(self.section, 'dbuser')
        self.dbname = self.cfg.get(self.section, 'dbname')
        self.dbpasswd = self.cfg.get(self.section, 'dbpasswd')
        self.dbhost = self.cfg.get(self.section, 'dbhost')
        self.dbname = self.cfg.get(self.section, 'dbname')
        self.localdir = self.cfg.get(self.section, 'localdir')

    def thisdaystamp(self):
        this_day = str(time.strftime('%Y-%m-%d'))
        return this_day

    def thishourdaystamp(self):
        this_hour_day = str(time.strftime('%Y-%m-%d:%H:%M'))
        return this_hour_day

    def thisdayfolder(self):
        this_day_folder = self.thisdaystamp().replace('-', '/')
        return this_day_folder

    def thisdays3folder(self):
        todays3folder = s3bucket + self.section + '/' + self.thisdayfolder()
        return todays3folder

    def dumpdatabases(self):
        mysql_dump = "mysqldump -u " + self.dbuser + " -p" + self.dbpasswd + " -h " + self.dbhost + " " + self.dbname + " |gzip > " + self.localdir + self.dbname + "-" + self.thishourdaystamp() + ".sql.gz"
        os.system(mysql_dump)

    def uploads3(self):
    	for file in glob.glob(self.localdir + '*.sql.gz'):
    		upload_s3 = "s3cmd put " + file + " " + self.thisdays3folder() + "/"
    		os.system(upload_s3) 

    def dbdump(self):
    	if any(File.endswith(".sql.gz") for File in os.listdir(self.localdir)):
    		for f in glob.glob(self.localdir + '*.sql.gz'):
    			os.remove(f)
    			self.dumpdatabases()
    			self.uploads3()
    	else:
    	    self.dumpdatabases()
    	    self.uploads3()

def main():
    for section in sections:
    	bak = Mysqlbackup(section)
    	bak.dbdump()

if __name__ == "__main__":
    main()
