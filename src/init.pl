#!/usr/bin/perl -w 

use Config::Auto;
use strict ;
use warnings ;
use Data::Dumper ;
use Getopt::Std;
use Carp ;
use DBI ;
use DBSupport ;
use LogSupport ;

sub createTargetSchema($) ;
sub createStageSchema($);

my $config = Config::Auto::parse("./config.xml", format => "xml");

my $logFile = sprintf("%s/%s", $config->{storage}->{var}, "init.log");
my $log = LogSupport::logInit("Init", $logFile);

$log->info("Initial config " . Dumper($config));

$log->info("Creating staging database");
createStage($config);
$log->info("Creating staging schema");
createStageSchema($config);

$log->info("Creating target database");
createTarget($config);
$log->info("Creating target schema");
createTargetSchema($config);


sub createTargetSchema($){
    my $c = shift ;
    my $target = $c->{'target'} ;
    my $source = $c->{'source'} ;
    my $targetPrms = getMysqlCmdParams($target);
    my $sourcePrms = getMysqlCmdParams($source);
    my $cmd = qq{mysqldump --no-data $sourcePrms | mysql $targetPrms };
    $log->info("Executing  ". $cmd );
    return `$cmd` ;
}



sub createStageSchema($){
    my $c = shift ;
    my $stage = $c->{'staging'} ;
    my $source = $c->{'source'} ;
    my $stagePrms = getMysqlCmdParams($stage);
    my $sourcePrms = getMysqlCmdParams($source);
    my $cmd = qq{mysqldump --no-data $sourcePrms | mysql $stagePrms };
    $log->info("Executing  ". $cmd );
    my $r= `$cmd` ;

    my $dbh = getDbh($stage) ;
    $dbh->do("set foreign_key_checks=0");
    ### Now we have drop all indexes from the stage db so creating them staging restore 
    ### will be much faster
    $log->info("Dropping indexes to speed up staging db");
    my $queryForTables = sprintf("select table_name from information_schema.tables where table_schema = '%s' and table_name like 'mem_%%' and table_type = 'BASE TABLE'", 
				 $stage->{name});
    my $sth = $dbh->prepare($queryForTables)
	or do {$log->error($!) ; die($!);};
    $sth->execute() or do {$log->error($!) ; die($!);};
    while(my @rstArray = $sth->fetchrow_array){
	my $table = $rstArray[0]; 
	my $sthIdx = $dbh->prepare(sprintf("show indexes from %s" , $table)) or do{
	    $log->error($!) ; die($!);};
	$sthIdx->execute() or do {$log->error($!) ; die($!);};
	my %droppedIdxes ;
	$droppedIdxes{PRIMARY} = 1;
	while(my $idxRef = $sthIdx->fetchrow_hashref()){
	    my $idxName = $idxRef->{'Key_name'} ;
	    if(! defined($droppedIdxes{$idxName}) ){
		$log->info("Dropping index $idxName from $table ");
		$dbh->do(sprintf("alter table %s drop index %s", $table , $idxName)) or do{
		    $log->error($!) ; 
		     die($!);};
	        $droppedIdxes{$idxName} = 1;
	       }
		}
	}
   
    }
    
