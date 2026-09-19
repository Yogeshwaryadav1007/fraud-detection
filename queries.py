"""All Cypher lives here so it can be reviewed, profiled and indexed as a unit."""

UPSERT = """
MERGE (c:Card {id: $card_id})
MERGE (a:Account {id: $account_id})
MERGE (cu:Customer {id: $customer_id})
MERGE (m:Merchant {id: $merchant_id})
  ON CREATE SET m.category = $mcc, m.country = $country
MERGE (t:Transaction {id: $transaction_id})
  SET t.amount = $amount, t.currency = $currency, t.at = datetime($occurred_at)
MERGE (cu)-[:OWNS]->(a)
MERGE (a)-[:HAS_CARD]->(c)
MERGE (c)-[:MADE]->(t)
MERGE (t)-[:AT_MERCHANT]->(m)
FOREACH (_ IN CASE WHEN $device_id IS NULL THEN [] ELSE [1] END |
  MERGE (d:Device {id: $device_id})
  MERGE (t)-[:FROM_DEVICE]->(d)
  MERGE (c)-[u:USED_DEVICE]->(d) ON CREATE SET u.first_seen = datetime()
  SET u.last_seen = datetime(), u.count = coalesce(u.count, 0) + 1)
FOREACH (_ IN CASE WHEN $ip_address IS NULL THEN [] ELSE [1] END |
  MERGE (i:IP {address: $ip_address})
  MERGE (t)-[:FROM_IP]->(i)
  MERGE (c)-[v:USED_IP]->(i) ON CREATE SET v.first_seen = datetime()
  SET v.last_seen = datetime(), v.count = coalesce(v.count, 0) + 1)
"""

# --- detection patterns -----------------------------------------------------

SHARED_DEVICE = """
MATCH (c:Card {id: $card_id})-[:USED_DEVICE]->(d:Device)<-[:USED_DEVICE]-(other:Card)
WHERE other.id <> $card_id
WITH d, collect(DISTINCT other.id) AS cards
WHERE size(cards) >= 2
RETURN d.id AS device_id, size(cards) + 1 AS card_count, cards[..8] AS sample
ORDER BY card_count DESC LIMIT 3
"""

SHARED_IP = """
MATCH (c:Card {id: $card_id})-[:USED_IP]->(i:IP)<-[:USED_IP]-(other:Card)
WHERE other.id <> $card_id
WITH i, collect(DISTINCT other.id) AS cards
WHERE size(cards) >= 4
RETURN i.address AS ip, size(cards) + 1 AS card_count
ORDER BY card_count DESC LIMIT 3
"""

FRAUD_PROXIMITY = """
MATCH path = shortestPath(
  (c:Card {id: $card_id})-[:USED_DEVICE|USED_IP|MADE|AT_MERCHANT*1..4]-(f:Card)
)
WHERE f.flagged_fraud = true AND f.id <> $card_id
RETURN f.id AS fraud_card, length(path) AS hops
ORDER BY hops ASC LIMIT 3
"""

MULE_FAN_IN = """
MATCH (a:Account {id: $account_id})<-[:TRANSFER_TO]-(src:Account)
WHERE src.id <> $account_id
WITH a, count(DISTINCT src) AS sources
WHERE sources >= 5
RETURN sources
"""

MERCHANT_RISK = """
MATCH (m:Merchant {id: $merchant_id})<-[:AT_MERCHANT]-(t:Transaction)
WHERE t.at > datetime() - duration('P30D')
WITH m, count(t) AS total,
     sum(CASE WHEN t.fraud_confirmed = true THEN 1 ELSE 0 END) AS frauds
WHERE total >= 20
RETURN total, frauds, toFloat(frauds) / total AS fraud_rate
"""

COMMUNITY_SIZE = """
MATCH (c:Card {id: $card_id})-[:USED_DEVICE|USED_IP*1..3]-(peer:Card)
RETURN count(DISTINCT peer) AS ring_size
"""
