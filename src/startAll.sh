#!/bin/bash
singleProcs="./populateStaging.pl ./extractStagingData.pl" 
multiProcs="./loadDataTarget.pl ./checkDataTarget.pl"
multiProcsPuids="p1 p2"

unemph='\e[0m'       # Text Reset
emph='\e[1;32m'       # Green


mkdir -p var/


outFiles="" 
## Single processes 

for p in $singleProcs ; do 
bName=`basename $p .pl`
outFile="${bName}.out"
outFiles="${outFiles} -f $outFile"
echo -e "${emph} Starting $p  ${unemph}"
nohup perl -w  $p  >>var/$outFile &
pgrep -f $p >>var/${bName}.pid
done


### Multiple Procs
for pid in $multiProcsPuids ; do
for p in $multiProcs ; do 
bName=`basename $p .pl`
outFile="${bName}-${pid}.out"
outFiles="${outFiles} -f  $outFile "
echo -e "${emph} Starting $p  with PUID $pid ${unemph}"
nohup perl -w  $p --puid $pid   >> var/$outFile &
pgrep -f $p >>var/${bName}.pid
done 
done

## Tail all the output in one file
cd var/ 
tail $outFiles >>allOutput.out &
cd ..
