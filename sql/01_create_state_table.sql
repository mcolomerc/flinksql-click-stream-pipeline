-- FlinkSQL Step 1: Create state table to track latest searchId per user
-- This creates an upsert table that maintains the current searchId for each user

DROP TABLE IF EXISTS user_search_state;

CREATE TABLE user_search_state (
  userId STRING,
  searchId STRING,
  searchTime TIMESTAMP(3),
  PRIMARY KEY (userId) NOT ENFORCED
) WITH (
  'changelog.mode' = 'upsert',
  'value.format' = 'avro-registry'
);
