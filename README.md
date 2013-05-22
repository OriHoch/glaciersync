glaciersync
===========

A python script that can be used to syncronize a directory tree to Amazon Glacier for backup purposes.

What does it do?
----------------

* Tested on windows but should work find on linux as well.
* You can define several sync profiles, each profile corresponds to a glacier vault. The vault will be created for you if it doesn't exist.
* Each profile can contain several directory trees.
* Supports unicode filenames and directories on windows.
* All the metadata (filenames, glacier archive ids etc..) is stored in an sqlite3 database in a fairly straightforward schema.
* Only new files or files that changed (based on size or modification time) are processed.
* Computes a hash for new or updated files so if a file is moved or renamed it won't be uploaded again to glacier
* Large files are uploaded each file in one glacier archive
* Small files are tarred together (You can define the large/small threshold in the configuration)
* If there is an unexpected error all the files processed so far are uploaded and you can just restart the program again.
* When all the files are processed - uploads the sqlite3 database to glacier as well.
* If there is a problem while uploading to glacier it sleeps for 10 seconds then retries.

What it doesn't do (yet)
------------------------

* Nice output / Progress information / UI
* Exclude filters - allow to exclude certain files based on extension/name/glob/regex
* Restore - should be easy to implement, basically should do the following:
    * get the vault inventory
    * download the last sqlite3 db (the archive description contains an identifier and a timestamp)
    * download the files based on the self-explanatory db tables
    * Because files are never deleted, there might be several files with the same hash. Make sure to only download the latest file for each hash.

Installation
------------

* Python (tested with 2.7)
* Pip
* pip install -r requirements.txt

Usage
-----

> python glaciersync.py -h

Configuration
-------------

See sample_settings.ini
