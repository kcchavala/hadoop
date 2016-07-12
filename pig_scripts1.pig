 users = LOAD 'hdfs://localhost:9000/user/kalyan/users.csv' USING PigStorage(',') AS (userid:chararray,name:chararray,state:chararray);


-- users_each_state

by_group = GROUP users BY state;

group_count = FOREACH by_group GENERATE FLATTEN(users.state),COUNT(users) as copy;

dist = DISTINCT group_count;

top_users = Order dist BY copy DESC;

top_five =LIMIT top_users 5;



DUMP top_five;

