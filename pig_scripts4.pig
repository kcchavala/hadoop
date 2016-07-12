users = LOAD 'hdfs://localhost:9000/user/kalyan/users.csv' USING PigStorage(',') AS (userid:chararray,name:chararray,state:chararray);


tweets = LOAD 'hdfs://localhost:9000/user/kalyan/tweets.csv' USING PigStorage (',') AS (tweetid:chararray,tweet:chararray,username:chararray);

total_ca = join tweets by username,users by userid;
ca_users = group total_ca by username;
top_ca = foreach ca_users generate FLATTEN(total_ca.username), COUNT(total_ca.tweet)as copy;
top_ca_user = FILTER top_ca by copy > 1;
dist = DISTINCT top_ca_user;
DUMP dist;
