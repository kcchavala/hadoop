-- more than one tweet in CA

users = LOAD 'hdfs://localhost:9000/user/kalyan/users.csv' USING PigStorage(',') AS (userid:chararray,name:chararray,state:chararray);


tweets = LOAD 'hdfs://localhost:9000/user/kalyan/tweets.csv' USING PigStorage (',') AS (tweetid:chararray,tweet:chararray,username:chararray);


total_user_ca = join tweets by username, users by userid; 

get_state = FILTER total_user_ca by state == 'CA';

ca_username = group get_state by username;

ca_users = foreach ca_username generate flatten(get_state.username), COUNT(get_state.tweet)as copy;

dist = distinct ca_users;

top_ca_tweet = FILTER dist BY copy>1;

DUMP top_ca_tweet;
