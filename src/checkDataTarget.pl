#!/usr/bin/perl -w 

use Config::Auto;
use strict ;
use warnings ;
use Data::Dumper ;
use Getopt::Std;
use Carp ;
use DBI ;
use DBSupport ;
use Text::CSV_XS ;
use Benchmark ;
use LogSupport ;
use Getopt::Long;

sub getTableRows($$);
sub getDataMD5($$$);
sub compareNumberRows($$$$);
sub compareMD5($$$$);

## private
my $log ;
my $outPath;
my $puid = '' ;

GetOptions('puid=s'=>\$puid );
 
if($puid eq ''){
    die("You need to specify and Process UUID --puid processUID");
}

my $config = Config::Auto::parse("./config.xml", format => "xml");

my $logFile = sprintf("%s/checker-%s.log", $config->{storage}->{var}, $puid,);
$log = LogSupport::logInit("Target Checker", $logFile);

my $dbhTarget  = getDbh($config->{'target'});
my $dbhSys = getDbh($config->{'staging'});
my $dbhSource = getDbh($config->{'source'});
$outPath = $config->{storage}{tmp} ;
my $mysqlTarget = getMysqlCmdParams($config->{'target'});
my $mysqlSource = getMysqlCmdParams($config->{'source'});

while(1){
    my $r ;
    $r= $dbhSys->do("update sys_queue set status='targetChecking', puid = '$puid' where status = 'targetImported'  limit 1") or do{
	$log->error($!);
	$r = 0 ;
	sleep(5);
    };
if ($r > 0){
    my $t0 = Benchmark->new();

    $log->info("Got something to process");
    my $sth = $dbhSys->prepare("select t_name  from sys_queue where status = 'targetChecking' and puid = '$puid'")
	or do {$log->error($!) ; die($!);};
    $sth->execute() or do {$log->error($!) ; die($!);};
    while(my @rstArray = $sth->fetchrow_array){
	my $table = $rstArray[0]; 
	my $query ;
	my $sthTarget;
	my $sthSource;
	my $outFile = $outPath . "/" . $table . ".csv";
	$log->info("Checking $table ....");
	my $isError =  0 ;
	my %compareResult = compareNumberRows($dbhTarget, $table , $dbhSource, $table);
	if($compareResult{'rows'} == 1 ){
	    $log->error("Error in the number of imported rows for table $table");
	    $isError = 1;
	}
	%compareResult = compareMD5($mysqlTarget, $table, $mysqlSource, $table);
	if($compareResult{target} ne $compareResult{source}){
	    $log->error("Error MD5 missmatching on data dump for table $table");
	    $isError = 1;
	}

	if($isError){
	    $dbhSys->do(sprintf("update sys_queue set status = 'targetCheckedFailed' where t_name = '%s'", 
			 $table));
	
	}else{
	$dbhSys->do(sprintf("update sys_queue set status = 'targetCheckedPassed' where t_name = '%s'", 
			 $table));
	}
	my $t1 = Benchmark->new();    
	my $td = timestr(timediff($t1, $t0));
	$log->info(sprintf("Table %s checked in  %s ", $table ,$td)) ;

    }
}else{
    $log->info("Checking Nothing to do sleeping for 20 sec");
    sleep(20);
}

}


sub getTableRows($$){
    my $dbh = shift ;
    my $table = shift ;
    my $query = sprintf("select count(*) as Rows from  %s " , $table); 
    my $sth = $dbh->prepare($query) or die($!);
    $sth->execute() or do {$log->error($!) ; die($!);};
    my %tableProp ;
    while(my $row = $sth->fetchrow_hashref ){
	$tableProp{'rows'} = $row->{'Rows'};
	print "Rows " . $tableProp{'rows'} . "\n" ;
	}
	return %tableProp ;

}


sub compareNumberRows($$$$){
    my $dbhTarget = shift ;
    my $targetTable = shift ;
    my $dbhSource = shift ;
    my $sourceTable = shift ;
    my %targetProp =getTableRows($dbhTarget, $targetTable);
    my %sourceProp = getTableRows($dbhSource, $sourceTable);
    my %compareResult ;
    if($targetProp{'rows'}!= $sourceProp{'rows'}){
	$compareResult{'rows'} = 1 ;
    }else{
	$compareResult{'rows'} = 0 ;
    }
    
    $compareResult{'target'} = %targetProp ;
    $compareResult{'source'} = %sourceProp ;

    return  %compareResult ;

}

sub getDataMD5($$$){
    my $mysqlParams = shift ;
    my $table = shift ;
    my $suffix = shift ;
    my $outFile = sprintf("%s/%s.%s", $outPath, $table, $suffix);
    my $cmd = sprintf(qq{mysql %s  -e "select * from %s " > %s },
	$mysqlParams, $table, $outFile);
    my $r = `$cmd` ;
    $cmd = sprintf("md5sum %s ", $outFile);
    $r = `$cmd` ;
    return $r ;
}

sub compareMD5($$$$){
    my $targetParams = shift ;
    my $targetTable = shift ;
    my $sourceParams  = shift ;
    my $sourceTable = shift ;
    my $targetR  = getDataMD5($targetParams, $targetTable, 'trg');
    my $sourceR  = getDataMD5($sourceParams, $sourceTable, 'src');
    $log->info("MD5 on data for target $targetTable is " . $targetR) ;
    $log->info("MD5 on data for source $sourceTable is " . $sourceR) ;
    my @tmp = split(/\s/, $sourceR);
    $sourceR =  $tmp[0];
    @tmp = split(/\s/, $targetR);
    $targetR = $tmp[0];
    return (target=>$targetR , source=>$sourceR);
}
