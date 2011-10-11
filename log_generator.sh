src=/home1/irteam/repo/scribe-bmt/sample.log
dest=/home1/irteam/repo/scribe-bmt/tmp.log
while [ 1 ]
do
	cat $src >> $dest
done
