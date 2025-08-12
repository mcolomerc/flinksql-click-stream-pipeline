-- FlinkSQL Step 2: Populate state table with search events
-- This maintains the latest searchId for each user

INSERT INTO user_search_state
SELECT 
  userId,
  searchId,
  eventTime as searchTime
FROM `{input_topic}`
WHERE eventType = 'search' AND searchId IS NOT NULL;
