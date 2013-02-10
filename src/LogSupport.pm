#!/usr/bin/perl 

package LogSupport ;

use Log::Log4perl;
use Log::Log4perl::Layout;
use Log::Log4perl::Level ;

sub logInit($$) ;

sub logInit($$){
    my $componentName = shift;
    my $logFile = shift ;
    my $log = Log::Log4perl->get_logger($componentName); 
    my $layout = Log::Log4perl::Layout::PatternLayout->new("%c\t%d\t[%r]\t%H\t%F\t%L\t{%m}%n");
       my $file_appender = Log::Log4perl::Appender->new(
                        "Log::Log4perl::Appender::File",
                        name      => "filelog",
           filename  => $logFile); 
       my $stdout_appender =  Log::Log4perl::Appender->new(
                        "Log::Log4perl::Appender::Screen",
                        name      => "screenlog",
           stderr    => 0);
 
    $stdout_appender->layout($layout);
    $file_appender->layout($layout);
 
    $log->add_appender($stdout_appender);
    $log->add_appender($file_appender);
    $log->level($INFO);
    return $log ;
}



1;
