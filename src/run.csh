#!/usr/bin/csh

# Load environment before starting testall

set rootdir = `dirname $0`
cd $rootdir

set cmd = $1
if ($cmd != "start" && $cmd != "stop") then
    echo "run.csh start/stop"
    exit
endif

if ( $?KROOT ) then
    set kroot = $KROOT
else
    set kroot = "/kroot"
endif

if ($cmd == "start") then
    # Required for KCWI KTL access
    foreach ktl (kbds kfcs)
        if (-e $RELDIR/data/kcwi/$ktl.envvar) then
            source $RELDIR/data/kcwi/$ktl.envvar
        endif
    end
    # Required for OSIRIS KTL access
    if (-e $RELDIR/data/nires.envvar) then
        source $RELDIR/data/nires.envvar
    endif
    # Required for OSIRIS KTL access
    if (-e $RELDIR/data/osiris.envvar) then
        source $RELDIR/data/osiris.envvar
    endif
endif

/usr/local/anaconda/bin/python manager.py koa_rti_main $cmd --port 55557 
