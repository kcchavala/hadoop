 users = LOAD 'hdfs://localhost:9000/user/kalyan/users.csv' USING PigStorage(',') AS (userid:chararray,name:chararray,state:chararray);


-- users_each_state

by_group = GROUP users BY state;

group_count = FOREACH by_group GENERATE FLATTEN(users.state), COUNT(users);

dist = DISTINCT group_count;



DUMP dist;

