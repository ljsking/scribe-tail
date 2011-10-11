SCRIBE_TAIL=/home1/irteam/repo/scribe-bmt/scribe_tail/scribe_tail
#target_log_path=/home1/irteam/repo/scribe-bmt/tmp.log
dt=`date +%Y%m%d`
target_log_path=/maildata/log/wmtad/$dt.log
logpath=/home1/irteam/repo/scribe-bmt
id=$1
$SCRIBE_TAIL --filename=$target_log_path --log-file=$logpath/scribe_tail.log --daemonize --category=wmtad --scribe_host=10.25.84.67
