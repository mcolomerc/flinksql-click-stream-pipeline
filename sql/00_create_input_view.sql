-- FlinkSQL Step 0: Modify watermark on existing input table for temporal joins
-- The table will be auto-created by Confluent Cloud with timestamp-millis support

ALTER TABLE `{input_topic}` MODIFY (
  WATERMARK FOR eventTime AS eventTime - INTERVAL '5' SECOND
);
