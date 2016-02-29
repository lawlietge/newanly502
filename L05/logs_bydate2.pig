-- Simson L. Garfinkel
-- ANLY502 --- parse the forensicswiki Apache log files using a regular expression.
-- based on code in the Amazon Elastic MapReduce Developer Guide

--
-- Registering the piggybank does not appear to be needed anymore.
---
-- REGISTER file:/home/hadoop/pig/lib/piggybank.jar;
---
--
-- Map locally defined functions to the Java functions in the piggybank
--
DEFINE EXTRACT       org.apache.pig.piggybank.evaluation.string.EXTRACT();
DEFINE REGEX_EXTRACT org.apache.pig.piggybank.evaluation.string.RegexExtract();

-- This example only uses EXTRACT



--
-- Here is a short sample of the logs:
-- s3://gu-anly502/ps03/forensicswiki.2012-01.unzipped/access.log.2012-01-01
-- 
-- raw_logs reads in the logfile as a single relation with a single value --- the line
-- raw_logs = load 's3://gu-anly502/ps03/forensicswiki.2012-01.unzipped/access.log.2012-01-01' as (line:chararray);
raw_logs = load 's3://gu-anly502/ps03/forensicswiki.2012.txt' as (line:chararray);


-- Log format:
--- 203.82.82.46 - - [03/Jan/2012:00:55:55 -0800] "GET /w/index.php?title=MediaWiki:Print.css&usemsgcache=yes&ctype=text%2Fcss&smaxage=18000&action=raw&maxage=18000 HTTP/1.1" 200 310 "http://www.forensicswiki.org/wiki/File_Carving" "Mozilla/5.0 (Windows NT 5.1) AppleWebKit/535.7 (KHTML, like Gecko) Chrome/16.0.912.63 Safari/535.7" 

-- Notice that the regular expression below ignores the GMT offset
-- Also notes that the \ needs to be escaped as \\ because pig's parser turns \\ into \
--
-- logs_base processes each of the lines 
-- FLATTEN takes the extracted values and flattens them into a single tupple
--
logs_base = 
  FOREACH
   raw_logs
  GENERATE
   FLATTEN ( EXTRACT( line,
     '^(\\S+) (\\S+) (\\S+) \\[([^\\]]+)\\] "(\\S+) (\\S+) \\S+" (\\S+) (\\S+) "([^"]*)" "([^"]*)"'
     ) ) AS (
     host: chararray, identity: chararray, user: chararray, datetime_str: chararray, verb: chararray, url: chararray, request: chararray, status: int,
     size: int, referrer: chararray, agent: chararray
     );

-- Notice that even though datetime_str has the full date and time, we are only parsing the date part
logs  = FOREACH logs_base GENERATE ToDate(SUBSTRING(datetime_str,0,11),'dd/MMM/yyyy') AS date, host, url, size;
logs2 = FOREACH logs      GENERATE SUBSTRING(ToString(date),0,10) AS date, host, url, size;

by_date = GROUP logs2 BY (date);
date_counts = FOREACH by_date GENERATE 
    group AS date,    -- the key you grouped on
    COUNT(logs2);      -- the number of log lines with this date

date_counts_sorted = ORDER date_counts BY date;
dump date_counts_sorted;
