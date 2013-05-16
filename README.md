cloudcmd
========

Cloudcmd (cloud command) is a generalized storage system that supports combining different kinds of underlying storage to produce a virtual, searchable, object store. 

The system is composed of a collection of content-addressable-storage endpoints, upon which a virtualization layer is built.  By virtualizing the CAS endpoints, mirroring and other replication strategies can be used to ensure data integrity.  Additionally, since the underlying storage is abstracted, it can be easily replaced without accecting the perception of an enduring file storage utility. 

When using remote or mobile storage (e.g. USB or portable drive), syncing is naturally supported do to the built-in replication strategies.  For example, if the mirroring strategy is used, then attaching to remote storage would enable the local storage to mirror remote content (and vice versa).

Indexing occurs 'close' to the storage.  For example, if a USB drive is used, then the index will live on the drive.  When the drive leaves, the index is no longer used so the files will not be returned when searching.  This effectively means cloudcmd supports ephemeral storage.

There are 3 parts to cloudcmd.  The core is the engine and there is also a simple command-line-interface (cli) called cld to manage it.  Cloudcmd also supports remote http endpoints, and this is the srv component.

Examples
--------

Add photos from a trip to Hawaii:

    $ cld add ~/Pictures/hawaii_2009 s3 image vacation hawaii 2009

    NOTE: "s3 image vacation hawawii 2009" are tags associated with the added files

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

    $ cld adapter file:////media/big_disk/cld_storage

Or add an adapter that only accepts files with image,vacation,movie tags and at tier 1

    $ cld adapter file:////media/big_disk/cld_storage?tier=1&tags=movie,vacation,image

TBD: s3 adapter support

    $ cld adapter s3:///<aws id>:<aws secret>@<s3 bucket>

Afterwards, the config file can be like:

    {
      "adapterHandlers": {
        "file": "cloudcmd.common.adapters.FileAdapter",
        "s3": "cloudcmd.common.adapters.S3Adapter"
      },
      "adapters": ["file:///media/big_disk/cld_storage"],
      "defaultTier": 1
    }

Cloudcmd enables users to create additonal configurations to all for different storage solutions:

For example, say a user wanted to store huge files reliably, but didn't want to have the s3 expense.  The user can create a new configuration that uses two huge disks (raid 1 mirror) and store the huge files there.

    $ cd ~
    $ mkdir huge_files
    $ cd huge_files
    $ cld init
    $ cld adapter file:///media/huge_disk_a
    $ cld adapter file:///media/huge_disk_b
    $ cld add ~/Videos hawaii videos

TODO: support storage profiles?

Build it
-----------

At this point cloudcmd is under active development, so setup can be a little tricky but very doable.

Maven and a few other related projects required.  CloudCmd will include a web server for hosting storage, so that's why viper.io is included.

    git clone git://github.com/briangu/cloudcmd.git
    cd cloudcmd
    git submodule init
    git submodule update
    mvn clean install

Next, open your .bashrc, .profile, or whatever you use and add a CLOUDCMD_HOME environment variable pointing to your cloudcmd root directory.

Lastly, Make sure the JAVA_HOME environment variable is pointing to a JDK.

For bash style shells: 

    export CLOUDCMD_HOME=~/scm/cloudcmd
    export PATH=$CLOUDCMD_HOME/cld/bin:$PATH

