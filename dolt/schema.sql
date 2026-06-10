-- SEC company facts dump — Dolt schema
-- CIK is always a 10-digit zero-padded string; never numeric.

DROP VIEW IF EXISTS facts;
DROP VIEW IF EXISTS tag_meta;
DROP VIEW IF EXISTS standardized_statements;
DROP TABLE IF EXISTS accessions;
DROP TABLE IF EXISTS cik_canonical;
DROP TABLE IF EXISTS cik_tags;
DROP TABLE IF EXISTS companies;
DROP TABLE IF EXISTS entities;
DROP TABLE IF EXISTS exchange_rates;
DROP TABLE IF EXISTS facts_enc;
DROP TABLE IF EXISTS funds;
DROP TABLE IF EXISTS multi_cik_tickers;
DROP TABLE IF EXISTS primary_tickers;
DROP TABLE IF EXISTS processed_ciks;
DROP TABLE IF EXISTS sources;
DROP TABLE IF EXISTS standardized_statements_enc;
DROP TABLE IF EXISTS std_presentation;
DROP TABLE IF EXISTS submissions;
DROP TABLE IF EXISTS tickers;
DROP TABLE IF EXISTS xbrl_tags;

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

-- ---------------------------------------------------------------------------
-- Dictionary dims: repeated strings in the big tables are stored once here
-- and referenced by INT id. Same-name views below expose the original
-- (pre-encoding) column surface for public consumers.
-- ---------------------------------------------------------------------------

CREATE TABLE xbrl_tags (
  tag_id       INT NOT NULL AUTO_INCREMENT,
  namespace    VARCHAR(32) NOT NULL,
  tag          VARCHAR(512) NOT NULL,
  label        VARCHAR(512),
  description  VARCHAR(4096),
  PRIMARY KEY (tag_id),
  UNIQUE KEY uq_xbrl_tags_ns_tag (namespace, tag)
);

CREATE TABLE accessions (
  accn_id  INT NOT NULL AUTO_INCREMENT,
  accn     VARCHAR(32) NOT NULL,
  PRIMARY KEY (accn_id),
  UNIQUE KEY uq_accessions_accn (accn)
);

CREATE TABLE sources (
  source_id  INT NOT NULL AUTO_INCREMENT,
  source     VARCHAR(256) NOT NULL,
  PRIMARY KEY (source_id),
  UNIQUE KEY uq_sources_source (source)
);

-- (company_type, statement, tag) functionally determines the presentation
-- columns, so they live once here instead of on 43M standardized rows.
CREATE TABLE std_presentation (
  company_type  VARCHAR(16) NOT NULL,
  statement     VARCHAR(32) NOT NULL,
  tag           VARCHAR(128) NOT NULL,
  label         VARCHAR(256),
  parent        VARCHAR(128),
  sequence      INT,
  factor        VARCHAR(8),
  balance       VARCHAR(16),
  unit          VARCHAR(16),
  PRIMARY KEY (company_type, statement, tag)
);

CREATE TABLE cik_tags (
  cik     VARCHAR(10) NOT NULL,
  tag_id  INT NOT NULL,
  PRIMARY KEY (cik, tag_id),
  KEY idx_cik_tags_tag (tag_id)
);

-- facts has nullable start/accn/frame -> surrogate id PK
CREATE TABLE facts_enc (
  id        BIGINT NOT NULL,
  cik       VARCHAR(10) NOT NULL,
  tag_id    INT NOT NULL,
  unit      VARCHAR(64),
  `start`   DATE,
  `end`     DATE,
  val       DOUBLE,
  val_text  VARCHAR(512),
  accn_id   INT,
  fy        INT,
  fp        VARCHAR(8),
  form      VARCHAR(32),
  filed     DATE,
  frame     VARCHAR(64),
  PRIMARY KEY (id),
  KEY idx_facts_cik (cik),
  KEY idx_facts_cik_tag (cik, tag_id),
  KEY idx_facts_cover (tag_id, cik, `end`, val_text, val)
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

CREATE TABLE standardized_statements_enc (
  cik             VARCHAR(10) NOT NULL,
  statement       VARCHAR(32) NOT NULL,
  period_ending   DATE NOT NULL,
  fiscal_year     INT NOT NULL,
  fiscal_period   VARCHAR(8) NOT NULL,
  calendar_year   INT,
  calendar_period VARCHAR(8),
  frequency       VARCHAR(16),
  tag             VARCHAR(128) NOT NULL,
  val             DOUBLE,
  currency        VARCHAR(8),
  company_type    VARCHAR(16) NOT NULL,
  source_id       INT NOT NULL,
  PRIMARY KEY (cik, statement, period_ending, fiscal_year, fiscal_period, tag),
  KEY idx_std_cik (cik),
  KEY idx_std_cover (tag, frequency, statement, period_ending, cik, val, currency, company_type, fiscal_year, fiscal_period),
  KEY idx_std_tag_freq (tag, frequency, statement),
  KEY idx_std_tag_period (tag, period_ending)
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

-- ---------------------------------------------------------------------------
-- Compatibility views: identical names and column sets as the original
-- (pre-encoding) tables, so existing queries keep working unchanged.
-- ---------------------------------------------------------------------------

CREATE VIEW facts AS
SELECT f.id, f.cik, x.namespace, x.tag, f.unit, f.`start`, f.`end`, f.val,
       f.val_text, a.accn, f.fy, f.fp, f.form, f.filed, f.frame
  FROM facts_enc f
  JOIN xbrl_tags x ON x.tag_id = f.tag_id
  LEFT JOIN accessions a ON a.accn_id = f.accn_id;

CREATE VIEW tag_meta AS
SELECT ct.cik, x.namespace, x.tag, x.label, x.description
  FROM cik_tags ct
  JOIN xbrl_tags x ON x.tag_id = ct.tag_id;

CREATE VIEW standardized_statements AS
SELECT s.cik, s.statement, s.period_ending, s.fiscal_year, s.fiscal_period,
       s.calendar_year, s.calendar_period, s.frequency, s.tag,
       p.label, p.parent, p.sequence, p.factor, p.balance, p.unit,
       s.val, s.currency, s.company_type, src.source
  FROM standardized_statements_enc s
  LEFT JOIN std_presentation p
         ON p.company_type = s.company_type AND p.statement = s.statement AND p.tag = s.tag
  JOIN sources src ON src.source_id = s.source_id;
