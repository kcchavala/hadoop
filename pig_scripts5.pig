users = LOAD 'hdfs://localhost:9000/user/kalyan/users.csv' USING PigStorage(',') AS (userid:chararray,name:chararray,state:chararray);


tweets = LOAD 'hdfs://localhost:9000/user/kalyan/tweets.csv' USING PigStorage (',') AS (tweetid:chararray,tweet:chararray,username:chararray);

top_user = join tweets by username , users by userid;

top_post = group top_user by username;

post_user = foreach top_post generate flatten(top_user.username), COUNT(top_user.tweet)as copy;

top_post_users = FILTER post_user BY copy == 0;

DUMP top_post_users;


