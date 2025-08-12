# FlinkSQL Click Stream Enrichment Pipeline

A complete real-time data enrichment pipeline using **Confluent Cloud FlinkSQL** that enriches product click events with the **latest searchId** from previous search events by the same user. Uses temporal joins to maintain user state and implements Schema Registry AVRO integration for type safety.

## 🎯 Overview

This pipeline demonstrates modern stream processing with Confluent Cloud's managed FlinkSQL service. It processes click stream events where:

- **Search events** contain a `searchId` and search query
- **Product click events** initially have `searchId = null`
- **FlinkSQL enriches** product clicks with the **latest `searchId`** from the same user's previous search events
- **Temporal joins** maintain user state to track the most recent search per user
- **Real-time enrichment** happens as events flow through the pipeline
- **AVRO schemas** ensure data consistency and enable schema evolution
- **End-to-end automation** with cleanup and monitoring

## 🏗️ Architecture

```
┌─────────────┐    ┌──────────────────┐    ┌─────────────┐
│   Producer  │───▶│  Input Topic     │───▶│ FlinkSQL    │
│ (AVRO)      │    │ (AVRO Schema)    │    │ Enrichment  │
└─────────────┘    └──────────────────┘    └─────────────┘
                            │                       │
                            ▼                       ▼
                   ┌──────────────────┐    ┌─────────────┐
                   │ Schema Registry  │    │ Output Topic│
                   │ (Type Safety)    │    │ (Enriched)  │
                   └──────────────────┘    └─────────────┘
                                                   │
                                                   ▼
                                          ┌─────────────┐
                                          │  Consumer   │
                                          │ (AVRO)      │
                                          └─────────────┘
```

### ✨ Key Features

- **🔄 Schema Registry Integration**: Full AVRO serialization/deserialization
- **📊 Native Confluent Cloud**: Leverages managed FlinkSQL service  
- **⚡ Real-time Processing**: Event-time based enrichment
- **🧼 Auto Cleanup**: Automated resource management
- **🔧 Easy Setup**: Single command execution with `make all-in-one`
- **📈 Monitoring**: Built-in success/failure indicators 

## 🚀 Quick Start

### 1. Clone & Setup
```bash
git clone <repository-url> 
```

### 2. Configure Environment
```bash
cp .env.template .env
# Edit .env with your Confluent Cloud credentials
```

### 3. Run Complete Pipeline
```bash
make all-in-one
```

That's it! The pipeline will:
1. ✅ Create virtual environment and install dependencies
2. ✅ Setup configuration and validate credentials
3. ✅ Create Kafka topics with AVRO schemas
4. ✅ Deploy FlinkSQL enrichment job
5. ✅ Start consumer for enriched events
6. ✅ Generate test click stream events
7. ✅ Display real-time enrichment results
8. ✅ Clean up all resources

## ⚙️ Configuration

Edit `.env` with your Confluent Cloud details:

```bash
# Kafka Cluster
BOOTSTRAP_SERVERS=pkc-xxxxx.region.aws.confluent.cloud:9092
SASL_USERNAME=your-kafka-api-key
SASL_PASSWORD=your-kafka-api-secret
KAFKA_CLUSTER_ID=lkc-xxxxx

# Confluent Cloud Environment
CONFLUENT_ENV_ID=env-xxxxx
CONFLUENT_CLOUD_ENVIRONMENT_ID=env-xxxxx

# FlinkSQL Service
FLINK_REST_ENDPOINT=https://flink.region.aws.confluent.cloud
FLINK_ORG_ID=your-org-id
FLINK_API_KEY=your-flink-api-key
FLINK_API_SECRET=your-flink-api-secret
FLINK_COMPUTE_POOL_ID=lfcp-xxxxx

# Schema Registry (Required for AVRO)
SCHEMA_REGISTRY_API_KEY=your-sr-api-key
SCHEMA_REGISTRY_API_SECRET=your-sr-api-secret
SCHEMA_REGISTRY_ENDPOINT=https://psrc-xxxxx.region.gcp.confluent.cloud
```

## 📊 Data Flow & Schema

### Temporal Join Architecture

The pipeline uses a **4-step temporal join approach**:

1. **Input Table Setup**: Adds watermarks to the input topic for temporal processing
2. **State Table Creation**: Creates an upsert table to maintain latest `searchId` per user
3. **State Population**: Inserts search events into the state table as they arrive
4. **Temporal Enrichment**: Joins product clicks with user state using `FOR SYSTEM_TIME AS OF`

```sql
-- Step 4: Temporal Join Query
INSERT INTO output_topic
SELECT 
  c.userId,
  c.eventType,
  COALESCE(c.searchId, s.searchId, CONCAT('enriched-', c.userId)) as searchId,
  -- ... other fields
FROM input_topic c
LEFT JOIN user_search_state FOR SYSTEM_TIME AS OF c.eventTime s
  ON c.userId = s.userId;
```

This ensures **real-time enrichment** where each product click gets the **latest actual searchId** from that user's previous search events.

### Input Events (AVRO Schema)
```json
{
  "eventTime": "2025-08-12T15:08:07.863593+00:00",
  "userId": "user1",
  "clickId": "unique-click-id",
  "eventType": "search" | "product_click",
  "searchId": "search-uuid" | null,
  "productId": "product_1" | null,
  "query": "search query" | null,
  "referrer": "google.com" | "search_results",
  "metadata": {
    "device": "desktop",
    "browser": "chrome"
  }
}
```

### FlinkSQL Enrichment Logic
The SQL enrichment implements a **temporal join** approach:

1. **State Table**: Maintains the latest `searchId` per user using upsert mode
2. **Temporal Join**: Uses `FOR SYSTEM_TIME AS OF` to join product clicks with user's latest search state
3. **Real-time Processing**: Events are processed as they arrive with proper watermarks
4. **Fallback Logic**: When no previous search exists, applies `enriched-{userId}` pattern

The pipeline maintains user state and enriches each product click with the **actual searchId** from that user's most recent search event.

### Output Events (Enriched)
```json
{
  "eventTime": "2025-08-12T15:08:09.863593+00:00",
  "userId": "user1",
  "clickId": "unique-click-id",
  "eventType": "product_click",
  "searchId": "85894f91-6de5-4674-85ef-76748c1bc095",  // ← ENRICHED with actual searchId!
  "productId": "product_1",
  "query": null,
  "referrer": "search_results",
  "metadata": {
    "device": "desktop",
    "browser": "chrome"
  }
}
```

## 🔧 Manual Execution

Run individual components for debugging:

```bash
# Setup environment
python setup.py

# Create topics and schemas
python topics.py

# Deploy FlinkSQL job
python flink_sql.py

# Generate test events
python producer.py

# Consume enriched results
python consumer.py

# Clean up resources
python cleanup.py
```

## 📈 Expected Results

When running `make all-in-one`, you should see:

```
✅ Message delivered to input_pipeline_xxx [1] at offset 20
✅ Message delivered to input_pipeline_xxx [2] at offset 21

📨 Enriched Message:
   🎯 User: user1
   📦 Event Type: product_click
   🛍️  Product ID: product_1
   🔍 SearchId: 85894f91-6de5-4674-85ef-76748c1bc095  // ← Real searchId from user's previous search!
   📝 ClickId: 7a86a720-6ab5-49fd-ae58-1780290ddf58
   ⏰ Time: 2025-08-12T15:08:09.863593+00:00

✅ ENRICHED: Product click successfully linked to user's latest search
📈 Enrichment Rate: 6/14 messages (43%) - Real searchId enrichment
📉 Fallback Rate: 8/14 messages (57%) - Using enriched-{userId} pattern
```

**Enrichment Logic:**
- When a user performs a **search**, their `searchId` is stored in the state table
- Subsequent **product clicks** by the same user get enriched with that **actual searchId**
- If no previous search exists, fallback to `enriched-{userId}` pattern
- Temporal joins ensure **real-time** enrichment as events flow through

## 🧹 Cleanup

The pipeline automatically cleans up resources, but you can manually run:

```bash
python cleanup.py
```

This removes:
- All Flink SQL statements
- Created Kafka topics  
- Schema Registry subjects
- Consumer groups

## 🛠️ Troubleshooting

### Common Issues

| Issue | Solution |
|-------|----------|
| `❌ Failed to setup consumer` | Check Schema Registry credentials in `.env` |
| `❌ Consumer error: UTF-8 decode` | Verify AVRO deserializer is working |
| `❌ Topic creation failed` | Validate Kafka API keys and cluster ID |
| `❌ FlinkSQL deployment failed` | Check Flink compute pool and API credentials |

### Debug Mode

Enable verbose logging:
```bash
export DEBUG=1
make all-in-one
```

## 🎯 Use Cases

This pipeline pattern is ideal for:

- **E-commerce Analytics**: Link product clicks to search queries
- **User Journey Tracking**: Connect user actions across sessions  
- **Real-time Personalization**: Enrich events with user context
- **Marketing Attribution**: Track campaign effectiveness
- **Fraud Detection**: Correlate suspicious activities

## ⚖️ Implementation Alternatives & Scale Considerations

The optimal implementation approach depends on your scale requirements:

#### Windowed Aggregation Approach

```sql
-- Alternative: Use tumbling windows instead of temporal joins
SELECT userId, 
       LAST_VALUE(searchId) as latestSearchId
FROM input_topic
WHERE eventType = 'search'
GROUP BY userId, TUMBLE(eventTime, INTERVAL '1' HOUR)
```

- **State Storage**: Windowed state with configurable retention
- **Pros**: Better memory management, handles user churn
- **Cons**: Less precise timing, potential searchId delays
- **Use When**: Moderate scale with acceptable latency trade-offs


#### Batch + Stream Hybrid Approach

```sql
-- Batch: Periodic user state snapshots (every 15 minutes)
CREATE TABLE user_state_snapshots AS
SELECT userId, LAST_VALUE(searchId) as searchId, window_end
FROM search_events
GROUP BY userId, TUMBLE(eventTime, INTERVAL '15' MINUTES);

-- Stream: Enrich with latest snapshot + recent searches
SELECT c.*, 
       COALESCE(recent.searchId, snapshot.searchId, 'enriched-' || c.userId) as searchId
FROM product_clicks c
LEFT JOIN recent_searches recent ON c.userId = recent.userId
LEFT JOIN user_state_snapshots snapshot ON c.userId = snapshot.userId;
```

- **State Storage**: Hybrid batch snapshots + streaming updates
- **Pros**: Handles massive scale, cost-effective, fault-tolerant
- **Cons**: Complex architecture, potential staleness
- **Use When**: Massive scale, cost optimization priority

### 💡 **Optimization Tips**

- **State TTL**: Configure state cleanup for inactive users
- **Partitioning**: Use consistent userId hashing for parallel processing
- **Compression**: Enable state compression for memory efficiency
- **Monitoring**: Track state size, processing latency, and enrichment rates

## 📚 Technologies Used

- **Confluent Cloud**: Managed Kafka + FlinkSQL
- **Schema Registry**: AVRO schema management
- **Python**: Producer/Consumer applications
- **FlinkSQL**: Stream processing and enrichment
- **Docker**: Local development (optional)

## 📄 License

This project is open source and available under the [MIT License](LICENSE).

---

**FlinkSQL Click Stream** - Real-time event enrichment made simple 🚀
