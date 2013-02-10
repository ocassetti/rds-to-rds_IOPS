#!/usr/bin/perl 

package DBSupport ;

require Exporter;
@ISA = qw(Exporter);
@EXPORT = qw(getMysqlCmdParams createDB createTarget createStage getDbh);


sub getMysqlCmdParams($){
    my $c = shift ;
    return sprintf(" -u%s -p%s -h%s %s " , $c->{user}, $c->{password}, $c->{host},
	$c->{name} );

}


sub createDB($){
    my $c = shift ;
    my $database = $c->{'name'};
    my $dbhost = $c->{'host'};
    my $user = $c->{'user'};
    my $pass = $c->{'password'};
    my %copyConf = %$c ;
    $copyConf{'name'} = 'mysql' ;
    my $dbh = getDbh(\%copyConf);
    my $r = $dbh->do(qq{CREATE DATABASE $database DEFAULT CHARACTER SET utf8 COLLATE utf8_unicode_ci}) or die("Canno create database $!");
}


sub createTarget($){
    my $c = shift ;
    return createDB($c->{'target'});
    
}

sub createStage($){
    my $c = shift ;
    createDB($c->{'staging'});
    #TODO check return value and decide if we proceed
    my $dbh = getDbh($c->{'staging'});
    $dbh->do(qq{CREATE TABLE sys_queue (
t_name VARCHAR(128) PRIMARY KEY,
status VARCHAR(128),  
puid VARCHAR(128),
KEY status (status),
KEY puid (puid)) }) or die("Cannot create table $! \n") ;
}


sub getDbh($){
    my $c = shift ;
    my $database = $c->{'name'};
    my $dbhost = $c->{'host'};
    my $user = $c->{'user'};
    my $pass = $c->{'password'};

    my $dbh = DBI->connect( "DBI:mysql:database=$database;mysql_local_infile=1;host=$dbhost", $user, $pass ) or die("Cannot connect $! \n");
    
    return $dbh ;

}

1;
