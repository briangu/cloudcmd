cloudcmd
========

Cloudcmd is a distributed cloud storage engine that enables file indexing, de-duplication, tagging, and replication to arbitrary storage endpoints implemented as adapters.

The cloudcmd engine has a simple command line interface (cli) called cld that is heavily influenced by git.  Most cld commands accept JSON emitted by other cld commands (e.g. find) to allow easy piping and usage of archive file sets.

Example
-------

Setup cloudcmd with a local file directory adapter and an s3 adapter:

    $ cd ~
    $ cld init
    $ cld adapter --add file:///~/.cld_storage
    $ cld adapter --add s3:///<s3 bucket>/<aws id>/<aws secret>

The following shows how a user might index a set of pictures tagged for s3 and other useful attributes.

    $ cld index ~/Pictures/hawaii_2009 s3 image vacation hawaii 2009

Send the files to all the adapters

    $ cld push

Send the files to all the adapters at or below tier 2

    $ cld push -t 2

See what's in the cloud storage

    $ cld find | cld print

See which files are tagged with hawaii

    $ cld find hawaii | cld print

Add a new tag to the hawaii pictures and push the tag changes to the cloud

    $ cld find hawaii | cld tag awesome
    $ cld push

On another computer, the user can sync to the same storage

    ...<same setup>...
    $ cld pull
    $ mkdir ~/Pictures/hawaii_2009
    $ cd ~/Pictures/hawaii_2009

Fetch all the hawaii files (dropping the path info with -f)

    $ cld find hawaii | cld get -f


Build it
-----------

At this point cloudcmd is under active development, so setup can be a little tricky but very doable.

Maven and a few other related projects required.  CloudCmd will include a web server for hosting storage, so that's why viper.io is included.

    git clone git://github.com/briangu/httpjsonclient.git
    cd httpjsonclient
    mvn clean install

    git clone git://github.com/briangu/viper.io.git
    cd viper.io
    ./bin/mvn-install.sh
    mvn clean install

    git clone git://github.com/briangu/cli-util.git
    cd cli-util
    mvn clean install

    git clone git://github.com/briangu/cyclops.git
    cd cyclops
    mvn clean install

    git clone git://github.com/briangu/cloudcmd.git
    cd cloudcmd
    mvn clean install

Next, open your .bashrc, .profile, or whatever you use and add a CLOUDCMD_HOME environment variable pointing to your cloudcmd root directory.

Lastly, Make sure the JAVA_HOME environment variable is pointing to a JDK.

For bash style shells: 

    export CLOUDCMD_HOME=~/scm/cloudcmd
    export PATH=$CLOUDCMD_HOME/cld/bin


