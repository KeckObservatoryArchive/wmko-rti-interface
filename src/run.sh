#!/usr/bin/sh

# Load environment before starting testall

ROOTDIR=`dirname $0`
cd $ROOTDIR

/usr/local/anaconda/bin/python manager.py koa_rti_main $1 --port 55557 
