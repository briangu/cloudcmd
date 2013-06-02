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
* Tag-based content routing. Adapters may accept or reject tags to filter which content goes where.  For example, all files tagged with 'cloud' could be sent to s3 while the s3 adapter may reject all 'movie' files.
* Tiered storage as a cost factor.  Adapters may be labeled with a numerical tier to describe the relative cost to other adapters.  For example, file adapters may be tier 1 while an s3 adapter may be tier 2.  By default, lower tier adapters will be used first for retrieval.

There are 3 parts to cloudcmd.

1. The core is the engine, which implements the specified configuration.
2. A simple command-line-interface (cli) called cld to manage it.
3. Support for remote http endpoints via the srv component.

Another way to think of cloudcmd is that it shares a similar philosophy of git.  All files are hashed and referenced by hash.  In contrast to git, the system is not tree based.  The file metadata chain may be relative to itself, but is not tied to a directory or commit tree.  This greatly simplifies merge complexities.  Changes to the same file will simply result in additions of metadata which can be handled on a case-by-case basis.  For many types of files, they are immutable (e.g. photos), so merge issues simply don't exist.

For Amazon S3 storage, RRS (reduced redundancy storage) is used by default since there is likely a local-disk backed copy.  As a result, this reduces costs.  Full S3 storage-class may be used if specified in the adapter URI.

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

Tags
-----

In order to allow appropriate usage of each kind of adapter, whether the motivation is cost or speed, tags enable adapters to have strict accept/reject policies.  

Tags are usually specified at 'add' time.  For files, the extension is automatically included as a tag.  This is a convenience to allow adapters to simply reject/accpt files based on extension.  Content may be retagged as well.

By default, all adapters accept all content.

Tags are specified by adding a query tag on the adapter URI:

    file:///Volumes/disk/cldstorage/?tags=jpg

Multiple tags are separated by commas.

    file:///Volumes/disk/cldstorage/?tags=jpg

There are two kinds of adapter tags, accepts and rejects.

Accepts tags are specified as-is, while reject tags are prefixed by a hyphen '-':

    file:///Volumes/disk/cldstorage/?tags=jpg,png,-gif,-tiff

In this example, the adapter will accept content tagged with jpg and png, and reject content tagged with gif and tiff.

A very useful scenario is use tag filtering to manage which content goes where for cost purposes.  For example, let's say there are 3 adapters in the configuration.  The first is an s3 adapter and the other two are file adapters using external storage.  Imagine we are storing raw photos and that we extract the thumbnails out using dcraw.  To save cost, we can have the s3 adapter accept everything and reject CR2 (raw) files.
   
    s3: tags=-CR2
    file adapters do not need tags, as they accept everything.

In this configuration the raw files are mirrored across two local external disks and the thumbnails are stored on s3.

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
    
To enable full storage-class (non-RRS), use the useRRS URI query param:

    $ cld adapter -a s3://<aws-key>@<bucket-id>?secret=<aws-secret>&useRRS=false

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

    git clone git://github.com/briangu/cloudcmd.git
    cd cloudcmd
    mvn clean install

Next, open your .bashrc, .profile, or whatever you use and add a CLOUDCMD_HOME environment variable pointing to your cloudcmd root directory.

Lastly, Make sure the JAVA_HOME environment variable is pointing to a JDK.

For bash style shells: 

    export CLOUDCMD_HOME=~/scm/cloudcmd
    export PATH=$CLOUDCMD_HOME/cld/bin:$PATH

