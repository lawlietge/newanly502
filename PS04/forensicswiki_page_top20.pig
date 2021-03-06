--
-- part2 problem 4
-- Create a Pig program that reports the number of URLs served each day in 2012 by the forensicswiki.org website

-- Clear the output directory location
--
rmf forensicswiki_count_by_date

--
-- Map locally defined functions to the Java functions in the piggybank
--
DEFINE EXTRACT       org.apache.pig.piggybank.evaluation.string.EXTRACT();

-- This URL uses just one day
-- raw_logs = load 's3://gu-anly502/ps03/forensicswiki.2012-01.unzipped/access.log.2012-01-01' as (line:chararray);
--
-- This URL reads a month:
-- raw_logs = load 's3://gu-anly502/ps03/forensicswiki.2012-01.unzipped/access.log.2012-01-??' as (line:chararray);
--
-- This URL reads all of 2012:
raw_logs = load 's3://gu-anly502/ps03/forensicswiki.2012.txt' as (line:chararray);

 
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
logs  = FOREACH logs_base GENERATE ToDate(SUBSTRING(datetime_str,0,11),'dd/MMM/yyyy') AS date, url;
date_logs = FOREACH logs      GENERATE SUBSTRING(ToString(date),0,10) AS date, url;
logs_2012 = FOREACH date_logs GENERATE REGEX_EXTRACT_ALL(date, '(2012.*)') AS date, url;
wiki_logs = FOREACH logs_2012 GENERATE REGEX_EXTRACT_ALL(url, '(index.php\\?title=|/wiki/)([^ &]*)') AS date, url;


by_url = GROUP wiki_logs BY (url);
url_counts = FOREACH by_url GENERATE
    group AS url,    -- the key you grouped on
    COUNT(wiki_logs);      -- the number of log lines wiht this date
url_counts_sorted = ORDER url_counts BY $1 DESC;
url_counts_sorted = limit url_counts_sorted 20; 
dump url_counts_sorted;




store url_counts_sorted INTO 'forensicswiki_page_top20' USING PigStorage();


-- Get the results
--
fs -getmerge forensicswiki_page_top20 forensicswiki_page_top20.txt

