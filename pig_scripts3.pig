 -- tweets with word happy

tweets = LOAD 'hdfs://localhost:9000/user/kalyan/tweets.csv' USING PigStorage (',') AS (tweetid:chararray, tweet:chararray, username:chararray);

tweet_happy = filter tweets By tweet matches '.*Happy.*';

tweet_happy_id = foreach tweet_happy generate tweetid,tweet; 

tweet_result = order tweet_happy_id by tweetid;

Dump tweet_result;
