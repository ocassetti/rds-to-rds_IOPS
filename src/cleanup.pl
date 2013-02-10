#!/usr/bin/perl -w 

use Config::Auto;
use strict ;
use warnings ;
use Data::Dumper ;
use DBI ;
use DBSupport ;


sub dropTargetSchema($) ;
sub dropStageSchema($);
sub dropDB($);


my $config = Config::Auto::parse("./config.xml", format => "xml");

dropTargetSchema($config);
dropStageSchema($config);

my $dir =  $config->{storage}->{tmp} ;
`rm -rvf $dir/* ` ;
$dir = $config->{storage}->{var};
`rm -rvf $dir/*` ;


sub dropTargetSchema($){
    my $c = shift ;
    my $target = $c->{'target'} ;
    dropDB($target);
}



sub dropStageSchema($){
    my $c = shift ;
    my $stage = $c->{'staging'} ;
    dropDB($stage);
}
    
sub dropDB($){
    my $target = shift;
    my %copyConf = %$target ;
    $copyConf{'name'} = 'mysql' ;
    my $dbh = getDbh(\%copyConf);
    $dbh->do(sprintf("drop database %s", $target->{name})) or warn($!);

}
