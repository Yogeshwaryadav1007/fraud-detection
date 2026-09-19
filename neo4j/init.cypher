// Constraints and indexes — run once at bootstrap.
CREATE CONSTRAINT card_id       IF NOT EXISTS FOR (c:Card)        REQUIRE c.id IS UNIQUE;
CREATE CONSTRAINT account_id    IF NOT EXISTS FOR (a:Account)     REQUIRE a.id IS UNIQUE;
CREATE CONSTRAINT customer_id   IF NOT EXISTS FOR (c:Customer)    REQUIRE c.id IS UNIQUE;
CREATE CONSTRAINT merchant_id   IF NOT EXISTS FOR (m:Merchant)    REQUIRE m.id IS UNIQUE;
CREATE CONSTRAINT device_id     IF NOT EXISTS FOR (d:Device)      REQUIRE d.id IS UNIQUE;
CREATE CONSTRAINT ip_addr       IF NOT EXISTS FOR (i:IP)          REQUIRE i.address IS UNIQUE;
CREATE CONSTRAINT txn_id        IF NOT EXISTS FOR (t:Transaction) REQUIRE t.id IS UNIQUE;

CREATE INDEX txn_time   IF NOT EXISTS FOR (t:Transaction) ON (t.at);
CREATE INDEX card_flag  IF NOT EXISTS FOR (c:Card)        ON (c.flagged_fraud);
CREATE INDEX merch_ctry IF NOT EXISTS FOR (m:Merchant)    ON (m.country);
