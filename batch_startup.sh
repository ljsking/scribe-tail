SCRIBE_TAIL=/home1/irteam/repo/scribe-bmt/scribe_tail/scribe_tail
process_nu=10
target_log_path=/home1/irteam/repo/scribe-bmt/tmp.log
logpath=/home1/irteam/repo/scribe-bmt
for ((  i = 0 ;  i < $process_nu;  i++  ))
do
    $SCRIBE_TAIL --filename=$target_log_path --log-file=$logpath/scribe_tail.$i.log --daemonize --category=wmta
done
