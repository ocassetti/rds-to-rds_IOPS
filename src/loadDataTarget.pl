#!/usr/bin/perl -w 

use Config::Auto;
use strict ;
use warnings ;
use Data::Dumper ;
use Getopt::Std;
use Benchmark;
use Carp ;
use DBI ;
use DBSupport ;
use Text::CSV_XS ;
use LogSupport ;
use Getopt::Long ;

my $puid = '' ;

GetOptions('puid=s'=>\$puid );
 
if($puid eq ''){
    die("You need to specify and Process UUID --puid processUID");
}


my $config = Config::Auto::parse("./config.xml", format => "xml");
my $logFile = sprintf("%s/loader-%s.log", $config->{storage}->{var}, $puid);
my $log = LogSupport::logInit("Target Loader", $logFile);


my $dbhImport  = getDbh($config->{'target'});
my $dbhSys = getDbh($config->{'staging'});
my $outPath = $config->{storage}{tmp} ;

$log->info("Disabling MYSQL foreign key checks");
$dbhImport->do("set foreign_key_checks=0") or do {$log->error($!) ; die($!);};


while(1){
    my $r ;
    $r = $dbhSys->do("update sys_queue set status='targetProcessing', puid = '$puid' where status = 'targetQueue' limit 1") or do{
	$log->error($!);
	$r=0;
	sleep(5);
    };
if ($r > 0){
    my $t0 = Benchmark->new();

    $log->info("Got something to process");
    my $sth = $dbhSys->prepare("select t_name  from sys_queue where status = 'targetProcessing' and puid='$puid' ")
	or do {$log->error($!) ; die($!);};
    $sth->execute() or do {$log->error($!) ; die($!);};
    while(my @rstArray = $sth->fetchrow_array){
	my $table = $rstArray[0]; 
	my $query ;
	my $sthExport;
	my $outFile = $outPath . "/" . $table . ".csv";
	$log->info("Importing table $table") ;
	$query = sprintf( 
qq{LOAD DATA LOCAL INFILE '%s' INTO  TABLE %s
FIELDS TERMINATED BY '\\t' OPTIONALLY ENCLOSED BY '"'  
LINES TERMINATED BY '\\n' } ,  
			  $outFile, $table); 
	$log->info($query );
	$dbhImport->do($query) or do {$log->error($!) ; die($!);};
	$dbhSys->do(sprintf("update sys_queue set status = 'targetImported' where t_name = '%s'", 
			    $table));
	my $t1 = Benchmark->new();    
	my $td = timestr(timediff($t1, $t0));
	$log->info(sprintf("Table %s loaded into target in %s ", $table ,$td)) ;

    }
}else{
    $log->info("Import Nothing to do sleeping for 20 seconds");
    sleep(20);
}

}


