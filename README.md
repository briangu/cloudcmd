cloudcmd
========

Cloudcmd (cloud command) is a generalized storage system with the aim of pretty much enabling users to:

* store files where and how they want
* store files with a level of data integrity they want
* find and retrieve files they want

The system as a whole can be described as a generalization of a dropbox, google drive, skydrive, enabling people to combine different kinds of storage in useful ways to free themselves of single providers.

Technology features:

* Compose content-addressable-storage (CAS) endpoints to form a virtual storage layer.
* Mirroring and other replication strategies can be used to ensure data integrity.
* Easily replace underlying storage (CAS) while maintaining a consistent view of the storage contents.
* Syncing is naturally supported do to the built-in replication strategies.  For example, mirroring remote storage enables local storage to mirror remote content (and vice versa).
* Ephemeral storage, storage that may come and go, is supported as indexing occurs 'close' to the storage.  For example, if a USB drive is used, then the index will live on the drive.  When the drive leaves, the index is no longer used so the files will not be returned when searching.
* Deduplication is supported due to use of SHA-2 hashing. Adding the same file to the system will only cause the addition of a small amount of metadata.

There are 3 parts to cloudcmd.

1. The core is the engine, which implements the specified configuration.
2. A simple command-line-interface (cli) called cld to manage it.
3. Support for remote http endpoints via the srv component.

Another way to think of cloudcmd is that it shares a similar philosophy of git.  All files are hashed and referenced by hash.  In contrast to git, the system is not tree based.  The file metadata may chain relative to itself, but is not tied to a directory or commit tree.  This greatly simplifies merge complexities.  Changes to the same file will simply result in additions of metadata which can be handled on a case-by-case basis.  For many types of files, they are immutable (E.g. photos), so merge issues simply don't exist.

Examples
--------

Setup and Add photos from a trip to Hawaii to a new s3 and locally-backed configuration:

    $ cd ~
    $ cld init
    $ cld adapter -a file:///media/big_disk/cld_storage
    $ cld adapter -a <s3 URI see below>
    $ cld add ~/Pictures/hawaii_2009 s3 image vacation hawaii 2009

    NOTE: "s3 image vacation hawaii 2009" are tags associated with the added files

Move to another computer and fetch pictures from the Hawaii trip

    $ cd ~
    $ cld init
    $ cld adapter -a <s3 URI see below>
    $ cld reindex
    $ cld find hawaii 2009 | cld get

Ensure files are properly mirrored locally (mirror is the default replication):

    $ cld adapter -a file:///Users/user/cld_storage
    $ cld ensure

Find pictures from the hawaii 2009 trip:

    $ cld find hawaii 2009

Get pictures from the hawaii 2009 trip:

    $ cld find hawaii 2009 | cld get

See what's in the cloud storage:

    $ cld ls

Dump all metadata about the files in storage:

    $ cld find | cld print

See which files are tagged with hawaii

    $ cld find hawaii | cld print

Add a new tag to the hawaii pictures and push the tag changes to the cloud

    $ cld find hawaii | cld tag awesome
    $ cld ensure 

On another computer, the remote storage can be used to download images: 

    $ cld adapter -a <s3 URI> 
    $ cld reindex -u s3
    $ cld find hawaii 2009 | cld get 

Fetch all the hawaii files (dropping the path info with -f)

    $ cld find hawaii | cld get -f

Get help:

    $ cld help

Get help for specific command:

    $ cld help <command>

Setup
-----

Setup cloudcmd with a local file directory adapter and an s3 adapter:

    $ cd ~
    $ cld init

(cld will search for the nearest .cld folder in the directory tree)

The init command creates a .cld directory that holds all the config.

    $ cat ~/.cld/config.json

    {
      "defaultTier": 1,
      "adapters": [],
      "adapterHandlers": {
        "file": "cloudcmd.common.adapters.FileAdapter",
        "s3" : "cloudcmd.common.adapters.S3Adapter"
      }
    }

Add a file system adapter using URI notation (file system adapter pointing to /media/big_disk/cld_storage)

    $ cld adapter -a file:///media/big_disk/cld_storage

Or add an adapter that only accepts files with image,vacation,movie tags and at tier 1

    $ cld adapter -a file:///media/big_disk/cld_storage?tier=1&tags=movie,vacation,image

S3 adapter support

    $ cld adapter -a s3://<aws-key>@<bucket-id>?secret=<aws-secret>

    If the bucket does not exist, it will be created.

OAuth HTTP adapter (Works with HttpAdapter and CloudServer):

    $ cld adapter -a http://consumerKey:consumerSecret:userKey:userSecret@host:port/<path>

Example config:

    {
      "adapterHandlers": {
        "file": "cloudcmd.common.adapters.FileAdapter",
        "s3": "cloudcmd.common.adapters.S3Adapter"
      },
      "adapters": ["file:///media/big_disk/cld_storage"],
      "defaultTier": 1
    }


Cloudcmd enables users to create additional configurations to all for different storage solutions:

For example, say a user wanted to store huge files reliably, but didn't want to have the s3 expense.  The user can create a new configuration that uses two huge disks (raid 1 mirror) and store the huge files there.

    $ cd ~
    $ mkdir huge_files
    $ cd huge_files
    $ cld init
    $ cld adapter file:///media/huge_disk_a
    $ cld adapter file:///media/huge_disk_b
    $ cld add ~/Videos hawaii videos

Build it
-----------

At this point cloudcmd is under active development, so setup can be a little tricky but very doable.

Maven and a few other related projects required.  CloudCmd will include a web server for hosting storage, so that's why viper.io is included.

    git clone git://github.com/briangu/cloudcmd.git
    cd cloudcmd
    git submodule init
    git submodule update
    cd viper.io
    mvn clean install
    cd ..
    mvn clean install

Next, open your .bashrc, .profile, or whatever you use and add a CLOUDCMD_HOME environment variable pointing to your cloudcmd root directory.

Lastly, Make sure the JAVA_HOME environment variable is pointing to a JDK.

For bash style shells: 

    export CLOUDCMD_HOME=~/scm/cloudcmd
    export PATH=$CLOUDCMD_HOME/cld/bin:$PATH

