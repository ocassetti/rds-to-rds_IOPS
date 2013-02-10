rds-to-rds_IOPS
===============

This is a set of scripts, mainly in Perl scripts,  to copy data from a standard Amazon AWS RDS to an AWS RDS IOPS.

Amazon does not provide a direct way to migrate data from a standard RDS to the RDS IOPS. These scripts help you to 
migrate the data  from one type to the other. I use a staging instance that is a local mysql server. 
This mysql servers is used essentially to run "SELECT INTO OUTFILE" since this operation can not be executed on
AWS RDS. 
The data is loaded in the RDS IOPS using the "LOAD DATA LOCAL INFILE" as suggested by AWS guidelines. 
I compute the MD5 of the data imported.

================

How to run

First thing you need to define a config.xml file with the
source staging and target database properties.
Second you can run perl  init.pl which will initialize the local and the target database.
Third you can start all the workers by using startAll.sh 
If you need to clean up use perl cleanup.pl

==================
Example of a config files 
config.xml

<?xml version="1.0" encoding="UTF-8" ?>
<config>

<source>
<host>host name of the RDS instance where the source data is kept</host>
<user>mysql user </user>
<password>mysql </password>
<name>the name of the schema to copy</name>
</source>

<target>
<host>host name of the RDS IOPS </host>
<user></user>
<password></password>
<name></name>
</target>
 
<staging>
<host>localhost</host>
<user></user>
<password></password>
<name></name>
</staging>

<storage>
<tmp>temporary directory where temporary is saved</tmp>
<var>where logs are saved </var>
</storage>

</config>
