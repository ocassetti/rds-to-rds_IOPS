#!/usr/bin/perl -w 

use Config::Auto;
use strict ;
use warnings ;
use Data::Dumper ;
use Getopt::Std;
use Carp ;
use DBI ;
use DBSupport ;
use Benchmark ;
use LogSupport ;
        
my $config = Config::Auto::parse("./config.xml", format => "xml");
my $logFile = sprintf("%s/%s", $config->{storage}->{var}, "populator.log");
my $log = LogSupport::logInit("Staging Populator", $logFile);

my $sourceDbh = getDbh($config->{'source'});
my $stageDbh = getDbh($config->{'staging'});
my $stage = $config->{'staging'} ;
my $source = $config->{'source'} ;
my $stagePrms = getMysqlCmdParams($stage);
my $sourcePrms = getMysqlCmdParams($source);


my $dbh = $stageDbh ;

$log->info("Disabling MYSQL foreign key checks");
$dbh->do("set foreign_key_checks=0") or do {$log->error($!) ; die($!);};

my $queryForTables = sprintf("select table_name from information_schema.tables where table_schema = '%s' and table_type = 'BASE TABLE'", 
    $stage->{name});

my $sth = $dbh->prepare($queryForTables)
    or do {$log->error($!) ; die($!);};
$sth->execute() or do {$log->error($!) ; die($!);};
 
while(my @rstArray = $sth->fetchrow_array){
    my $table = $rstArray[0]; 
    if($table ne 'sys_queue'){
	my $t0 = Benchmark->new();
	$dbh->do(sprintf("insert into sys_queue (t_name, status) VALUES ('%s' , 'stagingImporting')", $table )) or do {
	    $log->error($!) ; die($!);};
	my $cmd = "mysqldump --no-create-info " . $sourcePrms ." " .$table ." |mysql " . $stagePrms ;
	$log->info("Executing $cmd ") ;
	my $r = `$cmd` ;
	##TODO Check return value 
	$log->info("Return value for $cmd was $r ") ;
	$dbh->do(sprintf("update sys_queue set status = 'stagingQueue' where t_name = '%s' ", $table )) or do {$log->error($!) ; die($!);};
	my $t1 = Benchmark->new();    
	my $td = timestr(timediff($t1, $t0));
	$log->info(sprintf("Table %s imported into staging in %s ", $table ,$td)) ;

    }
}
