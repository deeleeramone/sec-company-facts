-- SEC company facts dump — Dolt schema
-- CIK is always a 10-digit zero-padded string; never numeric.

DROP TABLE IF EXISTS cik_canonical;
DROP TABLE IF EXISTS companies;
DROP TABLE IF EXISTS entities;
DROP TABLE IF EXISTS exchange_rates;
DROP TABLE IF EXISTS facts;
DROP TABLE IF EXISTS funds;
DROP TABLE IF EXISTS multi_cik_tickers;
DROP TABLE IF EXISTS primary_tickers;
DROP TABLE IF EXISTS processed_ciks;
DROP TABLE IF EXISTS standardized_statements;
DROP TABLE IF EXISTS submissions;
DROP TABLE IF EXISTS tag_meta;
DROP TABLE IF EXISTS tickers;

CREATE TABLE cik_canonical (
  cik          VARCHAR(10) NOT NULL,
  primary_cik  VARCHAR(10),
  PRIMARY KEY (cik)
);

CREATE TABLE companies (
  cik                  VARCHAR(10) NOT NULL,
  entity_name          VARCHAR(512),
  source_mtime         DATETIME,
  source_content_hash  VARCHAR(128),
  PRIMARY KEY (cik)
);

-- entities is not unique on (cik, entity_name) -> surrogate id
CREATE TABLE entities (
  id           BIGINT NOT NULL,
  cik          VARCHAR(10) NOT NULL,
  entity_name  VARCHAR(1024),
  PRIMARY KEY (id),
  KEY idx_entities_cik (cik)
);

CREATE TABLE exchange_rates (
  rate_date      DATE NOT NULL,
  from_currency  VARCHAR(8) NOT NULL,
  to_currency    VARCHAR(8) NOT NULL,
  rate           DOUBLE,
  PRIMARY KEY (rate_date, from_currency, to_currency)
);

-- facts has nullable start/accn/frame -> surrogate id PK
CREATE TABLE facts (
  id         BIGINT NOT NULL,
  cik        VARCHAR(10) NOT NULL,
  namespace  VARCHAR(32),
  tag        VARCHAR(512),
  unit       VARCHAR(64),
  `start`    DATE,
  `end`      DATE,
  val        DOUBLE,
  val_text   VARCHAR(512),
  accn       VARCHAR(32),
  fy         INT,
  fp         VARCHAR(8),
  form       VARCHAR(32),
  filed      DATE,
  frame      VARCHAR(64),
  PRIMARY KEY (id),
  KEY idx_facts_cik (cik),
  KEY idx_facts_cik_tag (cik, tag)
);

CREATE TABLE funds (
  cik        VARCHAR(10) NOT NULL,
  series_id  VARCHAR(20) NOT NULL,
  class_id   VARCHAR(20) NOT NULL,
  symbol     VARCHAR(32),
  PRIMARY KEY (cik, series_id, class_id)
);

CREATE TABLE multi_cik_tickers (
  ticker    VARCHAR(16) NOT NULL,
  cik       VARCHAR(10) NOT NULL,
  priority  INT,
  PRIMARY KEY (ticker, cik)
);

CREATE TABLE primary_tickers (
  cik     VARCHAR(10) NOT NULL,
  ticker  VARCHAR(16) NOT NULL,
  name    VARCHAR(256),
  `rank`  INT,
  PRIMARY KEY (cik, ticker)
);

CREATE TABLE processed_ciks (
  cik           VARCHAR(10) NOT NULL,
  has_balance   BOOLEAN,
  has_income    BOOLEAN,
  has_cash_flow BOOLEAN,
  computed_at   DATETIME,
  PRIMARY KEY (cik)
);

CREATE TABLE standardized_statements (
  cik             VARCHAR(10) NOT NULL,
  statement       VARCHAR(32) NOT NULL,
  period_ending   DATE NOT NULL,
  fiscal_year     INT NOT NULL,
  fiscal_period   VARCHAR(8) NOT NULL,
  calendar_year   INT,
  calendar_period VARCHAR(8),
  frequency       VARCHAR(16),
  tag             VARCHAR(128) NOT NULL,
  label           VARCHAR(256),
  parent          VARCHAR(128),
  sequence        INT,
  factor          VARCHAR(8),
  balance         VARCHAR(16),
  unit            VARCHAR(16),
  val             DOUBLE,
  currency        VARCHAR(8),
  company_type    VARCHAR(16),
  PRIMARY KEY (cik, statement, period_ending, fiscal_year, fiscal_period, tag),
  KEY idx_std_cik (cik)
);

CREATE TABLE submissions (
  cik           VARCHAR(10) NOT NULL,
  payload       LONGBLOB,
  source_mtime  DATETIME,
  PRIMARY KEY (cik)
);

CREATE TABLE tag_meta (
  cik          VARCHAR(10) NOT NULL,
  namespace    VARCHAR(32) NOT NULL,
  tag          VARCHAR(512) NOT NULL,
  label        VARCHAR(512),
  description   VARCHAR(4096),
  PRIMARY KEY (cik, namespace, tag)
);

CREATE TABLE tickers (
  cik         VARCHAR(10) NOT NULL,
  ticker      VARCHAR(16) NOT NULL,
  name        VARCHAR(256),
  is_primary  BOOLEAN,
  `rank`      INT,
  PRIMARY KEY (cik, ticker)
);
