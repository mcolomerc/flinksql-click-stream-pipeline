-- FlinkSQL Step 3: Main enrichment using temporal join
-- Enriches all events with proper searchId from state using temporal join

INSERT INTO `{output_topic}`
SELECT 
  CAST(c.userId AS BYTES) as key,
  c.eventTime,
  c.userId,
  c.clickId,
  c.eventType,
  COALESCE(c.searchId, s.searchId, CONCAT('enriched-', c.userId)) as searchId,
  c.productId,
  c.query,
  c.referrer,
  c.metadata
FROM `{input_topic}` c
LEFT JOIN user_search_state FOR SYSTEM_TIME AS OF c.eventTime s
  ON c.userId = s.userId;
