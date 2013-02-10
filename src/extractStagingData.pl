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


my $config = Config::Auto::parse("./config.xml", format => "xml");
my $logFile = sprintf("%s/%s", $config->{storage}->{var}, "extractor.log");
my $log = LogSupport::logInit("Staging Extractor", $logFile);

my $dbh  = getDbh($config->{'staging'});
my $stage = $config->{'staging'} ;
my $stagePrms = getMysqlCmdParams($stage);
my $outPath = $config->{storage}{tmp} ;

while(1){
    my $r = $dbh->do("update sys_queue set status='exportProcessing' where status = 'stagingQueue' limit 1") ;
if ($r > 0){
    my $t0 = Benchmark->new();

    $log->info("Got something to process");
    my $sth = $dbh->prepare("select t_name  from sys_queue where status = 'exportProcessing' limit 1 ")
	or do {$log->error($!) ; die($!);};
    $sth->execute() or do {$log->error($!) ; die($!);};
    while(my @rstArray = $sth->fetchrow_array){
	my $table = $rstArray[0]; 
	my $query ;
	my $sthExport;
	my $outFile = $outPath . "/" . $table . ".csv";
	$log->info("Processing table $table") ;
	$query = qq{SELECT count(*) as total  FROM $table }; 
	$sthExport = $dbh->prepare($query) or do {$log->error($!) ; die($!);};
	$sthExport->execute() or do {$log->error($!) ; die($!);};
	my $count = $sthExport->fetchrow_hashref();
	$log->info("Counting ..." . $count->{'total'} ) ;
	if($count->{'total'} > 0){
	    $query = qq{SELECT * INTO OUTFILE '$outFile' 
FIELDS TERMINATED BY '\\t' OPTIONALLY ENCLOSED BY '"'  
LINES TERMINATED BY '\\n'
FROM $table };
	    $sthExport = $dbh->prepare($query) or do {$log->error($!) ; die($!);};
	    $sthExport->execute() or do {$log->error($!) ; die($!);};

	    $dbh->do(sprintf("update sys_queue set status = 'targetQueue' where t_name = '%s'", 
			 $table));
	    my $t1 = Benchmark->new();    
	    my $td = timestr(timediff($t1, $t0));
	    $log->info(sprintf("Table %s exported from staging in %s ", $table ,$td)) ;

	}else{
	    $dbh->do(sprintf("update sys_queue set status = 'stagingDiscard' where t_name = '%s'", 
			     $table));
    }

    }
}else{
    $log->info( "Nothing to do sleeping for 20 seconds");
    sleep(20);
}

}

