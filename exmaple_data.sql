-- ============================================================
-- SmartBI MySQL Sample Data (Macau Banking Scenario) - MySQL 8+
-- Charset: utf8mb4
-- ============================================================

DROP DATABASE IF EXISTS smartbi_demo;
CREATE DATABASE smartbi_demo CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci;
USE smartbi_demo;

-- ----------------------------
-- 1) Dimensions（維度表）
-- ----------------------------

CREATE TABLE dim_branch (
  branch_id      INT PRIMARY KEY,
  branch_code    VARCHAR(20) NOT NULL UNIQUE,
  branch_name    VARCHAR(100) NOT NULL,
  region         VARCHAR(50) NOT NULL,   -- 澳門半島/氹仔/路環/路氹城
  city           VARCHAR(50) NOT NULL,   -- 澳門
  opened_date    DATE NOT NULL
) ENGINE=InnoDB;

CREATE TABLE dim_product (
  product_id     INT PRIMARY KEY,
  product_code   VARCHAR(30) NOT NULL UNIQUE,
  product_name   VARCHAR(100) NOT NULL,
  product_type   ENUM('DEPOSIT','LOAN','CARD','WEALTH') NOT NULL,
  is_active      TINYINT NOT NULL DEFAULT 1
) ENGINE=InnoDB;

CREATE TABLE dim_calendar (
  biz_date       DATE PRIMARY KEY,
  year           INT NOT NULL,
  month          INT NOT NULL,
  day            INT NOT NULL,
  yyyy_mm        CHAR(7) NOT NULL,
  is_month_end   TINYINT NOT NULL DEFAULT 0
) ENGINE=InnoDB;

-- ----------------------------
-- 2) Core Entities（主體實體）
-- ----------------------------

CREATE TABLE core_customer (
  customer_id       BIGINT PRIMARY KEY,
  customer_no       VARCHAR(30) NOT NULL UNIQUE,
  full_name         VARCHAR(100) NOT NULL, -- PII（假資料）
  id_no             VARCHAR(40) NOT NULL,  -- PII（假資料）
  phone             VARCHAR(30) NULL,      -- PII
  email             VARCHAR(120) NULL,     -- PII
  birth_date        DATE NULL,
  gender            ENUM('M','F','U') NOT NULL DEFAULT 'U',
  risk_level        ENUM('LOW','MEDIUM','HIGH') NOT NULL DEFAULT 'MEDIUM',
  kyc_status        ENUM('PASSED','PENDING','FAILED') NOT NULL DEFAULT 'PASSED',
  created_at        DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB;

CREATE TABLE core_account (
  account_id     BIGINT PRIMARY KEY,
  account_no     VARCHAR(40) NOT NULL UNIQUE, -- 仍視為敏感識別資訊（樣本）
  customer_id    BIGINT NOT NULL,
  branch_id      INT NOT NULL,
  product_id     INT NOT NULL,                -- DEPOSIT 類產品
  currency       CHAR(3) NOT NULL DEFAULT 'MOP', -- 澳門幣
  status         ENUM('ACTIVE','FROZEN','CLOSED') NOT NULL DEFAULT 'ACTIVE',
  opened_at      DATETIME NOT NULL,
  closed_at      DATETIME NULL,
  CONSTRAINT fk_account_customer FOREIGN KEY (customer_id) REFERENCES core_customer(customer_id),
  CONSTRAINT fk_account_branch   FOREIGN KEY (branch_id)   REFERENCES dim_branch(branch_id),
  CONSTRAINT fk_account_product  FOREIGN KEY (product_id)  REFERENCES dim_product(product_id)
) ENGINE=InnoDB;

-- 存款帳戶：每日餘額快照（fact）
CREATE TABLE fact_account_balance_daily (
  biz_date       DATE NOT NULL,
  account_id     BIGINT NOT NULL,
  end_balance    DECIMAL(18,2) NOT NULL,
  available_bal  DECIMAL(18,2) NOT NULL,
  hold_amount    DECIMAL(18,2) NOT NULL DEFAULT 0,
  PRIMARY KEY (biz_date, account_id),
  CONSTRAINT fk_bal_account FOREIGN KEY (account_id) REFERENCES core_account(account_id),
  CONSTRAINT fk_bal_date    FOREIGN KEY (biz_date)   REFERENCES dim_calendar(biz_date)
) ENGINE=InnoDB;

-- 交易明細（fact）
CREATE TABLE fact_transaction (
  txn_id         BIGINT PRIMARY KEY,
  biz_date       DATE NOT NULL,
  account_id     BIGINT NOT NULL,
  txn_ts         DATETIME NOT NULL,
  txn_type       ENUM('DEPOSIT','WITHDRAW','TRANSFER_IN','TRANSFER_OUT','FEE','INTEREST') NOT NULL,
  channel        ENUM('BRANCH','ATM','MOBILE','WEB','API') NOT NULL,
  amount         DECIMAL(18,2) NOT NULL,      -- 入帳正、出帳負
  merchant_cat   VARCHAR(50) NULL,
  counterparty   VARCHAR(120) NULL,           -- 可能敏感（樣本）
  CONSTRAINT fk_txn_account FOREIGN KEY (account_id) REFERENCES core_account(account_id),
  CONSTRAINT fk_txn_date    FOREIGN KEY (biz_date)   REFERENCES dim_calendar(biz_date),
  INDEX idx_txn_date (biz_date),
  INDEX idx_txn_acct (account_id, biz_date),
  INDEX idx_txn_type (txn_type, biz_date)
) ENGINE=InnoDB;

-- 放款主檔（loan domain）
CREATE TABLE core_loan (
  loan_id        BIGINT PRIMARY KEY,
  loan_no        VARCHAR(40) NOT NULL UNIQUE,
  customer_id    BIGINT NOT NULL,
  branch_id      INT NOT NULL,
  product_id     INT NOT NULL, -- LOAN 類產品
  principal_amt  DECIMAL(18,2) NOT NULL,
  interest_rate  DECIMAL(8,5) NOT NULL,
  start_date     DATE NOT NULL,
  end_date       DATE NOT NULL,
  status         ENUM('ACTIVE','PAID_OFF','DEFAULT','WRITE_OFF') NOT NULL DEFAULT 'ACTIVE',
  CONSTRAINT fk_loan_customer FOREIGN KEY (customer_id) REFERENCES core_customer(customer_id),
  CONSTRAINT fk_loan_branch   FOREIGN KEY (branch_id)   REFERENCES dim_branch(branch_id),
  CONSTRAINT fk_loan_product  FOREIGN KEY (product_id)  REFERENCES dim_product(product_id),
  INDEX idx_loan_customer (customer_id),
  INDEX idx_loan_status (status)
) ENGINE=InnoDB;

-- 放款：每日餘額/逾期（fact）
CREATE TABLE fact_loan_balance_daily (
  biz_date        DATE NOT NULL,
  loan_id         BIGINT NOT NULL,
  outstanding_bal DECIMAL(18,2) NOT NULL,
  overdue_days    INT NOT NULL DEFAULT 0,
  overdue_amt     DECIMAL(18,2) NOT NULL DEFAULT 0,
  PRIMARY KEY (biz_date, loan_id),
  CONSTRAINT fk_loan_bal_loan FOREIGN KEY (loan_id) REFERENCES core_loan(loan_id),
  CONSTRAINT fk_loan_bal_date FOREIGN KEY (biz_date) REFERENCES dim_calendar(biz_date),
  INDEX idx_loan_bal_date (biz_date)
) ENGINE=InnoDB;

-- 風險：月度信用分（toy）
CREATE TABLE fact_credit_score_monthly (
  yyyy_mm       CHAR(7) NOT NULL,
  customer_id   BIGINT NOT NULL,
  score         INT NOT NULL,
  score_band    ENUM('A','B','C','D','E') NOT NULL,
  model_ver     VARCHAR(20) NOT NULL DEFAULT 'v1',
  PRIMARY KEY (yyyy_mm, customer_id),
  CONSTRAINT fk_score_customer FOREIGN KEY (customer_id) REFERENCES core_customer(customer_id)
) ENGINE=InnoDB;

-- ----------------------------
-- 3) Seed Data（樣本資料）
-- ----------------------------

-- 澳門分行（示例：澳門半島/氹仔/路氹城/路環）
INSERT INTO dim_branch (branch_id, branch_code, branch_name, region, city, opened_date) VALUES
(1, 'MO-PEN-001', '澳門半島中區分行', '澳門半島', '澳門', '2008-04-01'),
(2, 'MO-TAI-001', '氹仔分行',       '氹仔',     '澳門', '2013-07-15'),
(3, 'MO-COT-001', '路氹城分行',     '路氹城',   '澳門', '2016-10-20'),
(4, 'MO-COL-001', '路環分行',       '路環',     '澳門', '2018-05-09');

-- 產品（簡化）
INSERT INTO dim_product (product_id, product_code, product_name, product_type, is_active) VALUES
(101, 'D-SAV',  '活期儲蓄',   'DEPOSIT', 1),
(102, 'D-CHK',  '活期存款',   'DEPOSIT', 1),
(103, 'D-FXD',  '定期存款',   'DEPOSIT', 1),
(201, 'L-HOME', '按揭貸款',   'LOAN',    1),
(202, 'L-PERS', '個人貸款',   'LOAN',    1),
(301, 'C-VISA', '信用卡VISA', 'CARD',    1),
(401, 'W-FUND', '基金理財',   'WEALTH',  1);

-- Calendar（2026-01 部分日期）
INSERT INTO dim_calendar (biz_date, year, month, day, yyyy_mm, is_month_end) VALUES
('2026-01-01', 2026, 1, 1,  '2026-01', 0),
('2026-01-02', 2026, 1, 2,  '2026-01', 0),
('2026-01-03', 2026, 1, 3,  '2026-01', 0),
('2026-01-04', 2026, 1, 4,  '2026-01', 0),
('2026-01-05', 2026, 1, 5,  '2026-01', 0),
('2026-01-06', 2026, 1, 6,  '2026-01', 0),
('2026-01-07', 2026, 1, 7,  '2026-01', 0),
('2026-01-08', 2026, 1, 8,  '2026-01', 0),
('2026-01-09', 2026, 1, 9,  '2026-01', 0),
('2026-01-10', 2026, 1, 10, '2026-01', 0),
('2026-01-31', 2026, 1, 31, '2026-01', 1);

-- 客戶（假 PII；用於後續示範「語意層禁止輸出明細」）
INSERT INTO core_customer (customer_id, customer_no, full_name, id_no, phone, email, birth_date, gender, risk_level, kyc_status) VALUES
(10001, 'CUST-MO-0001', '何俊傑', 'M0000001', '+853-6000-1001', 'jun.ho@example.com',    '1991-03-12', 'M', 'LOW',    'PASSED'),
(10002, 'CUST-MO-0002', '梁詠琳', 'M0000002', '+853-6000-1002', 'wing.leong@example.com','1988-10-03', 'F', 'MEDIUM', 'PASSED'),
(10003, 'CUST-MO-0003', '黃子謙', 'M0000003', '+853-6000-1003', 'chi.huang@example.com', '1996-01-28', 'M', 'HIGH',   'PASSED'),
(10004, 'CUST-MO-0004', '鄭雅雯', 'M0000004', '+853-6000-1004', 'ya.zheng@example.com',  '1993-07-19', 'F', 'MEDIUM', 'PENDING'),
(10005, 'CUST-MO-0005', '陳思敏', 'M0000005', '+853-6000-1005', 'sman.chan@example.com', '1999-12-01', 'F', 'LOW',    'PASSED');

-- 帳戶（幣別以 MOP/HKD 混合，符合澳門常見使用）
INSERT INTO core_account (account_id, account_no, customer_id, branch_id, product_id, currency, status, opened_at) VALUES
(20001, 'ACCT-MO-0001-01', 10001, 1, 101, 'MOP', 'ACTIVE', '2025-12-15 10:00:00'),
(20002, 'ACCT-MO-0002-01', 10002, 1, 102, 'HKD', 'ACTIVE', '2025-11-20 09:30:00'),
(20003, 'ACCT-MO-0003-01', 10003, 2, 101, 'MOP', 'ACTIVE', '2025-10-05 14:20:00'),
(20004, 'ACCT-MO-0004-01', 10004, 4, 102, 'MOP', 'FROZEN', '2025-09-12 16:45:00'),
(20005, 'ACCT-MO-0005-01', 10005, 3, 103, 'HKD', 'ACTIVE', '2026-01-02 11:05:00');

-- 每日餘額快照
INSERT INTO fact_account_balance_daily (biz_date, account_id, end_balance, available_bal, hold_amount) VALUES
('2026-01-01', 20001, 180000.00, 180000.00, 0),
('2026-01-01', 20002,  95000.00,  95000.00, 0),
('2026-01-01', 20003,  62000.00,  62000.00, 0),
('2026-01-01', 20004,  28000.00,  26000.00, 2000.00),
('2026-01-02', 20001, 187500.00, 187500.00, 0),
('2026-01-02', 20002,  93000.00,  93000.00, 0),
('2026-01-02', 20003,  64500.00,  64500.00, 0),
('2026-01-02', 20004,  28000.00,  26000.00, 2000.00),
('2026-01-02', 20005,  30000.00,  30000.00, 0),
('2026-01-31', 20001, 210000.00, 210000.00, 0),
('2026-01-31', 20002,  90000.00,  90000.00, 0),
('2026-01-31', 20003,  70000.00,  70000.00, 0),
('2026-01-31', 20004,  28000.00,  26000.00, 2000.00),
('2026-01-31', 20005,  52000.00,  52000.00, 0);

-- 交易（把描述改成澳門常見語境：薪資/跨行轉帳/服務費/利息）
INSERT INTO fact_transaction (txn_id, biz_date, account_id, txn_ts, txn_type, channel, amount, merchant_cat, counterparty) VALUES
(90001, '2026-01-01', 20001, '2026-01-01 10:10:00', 'DEPOSIT',      'MOBILE',  7500.00,  NULL,      'Payroll Ltd.'),
(90002, '2026-01-01', 20001, '2026-01-01 12:20:00', 'TRANSFER_OUT', 'MOBILE', -1800.00,  NULL,      'ACCT-XXXX'),
(90003, '2026-01-01', 20002, '2026-01-01 09:05:00', 'WITHDRAW',     'ATM',     -800.00,  NULL,      NULL),
(90004, '2026-01-02', 20003, '2026-01-02 15:40:00', 'DEPOSIT',      'BRANCH',   2500.00, NULL,      NULL),
(90005, '2026-01-02', 20002, '2026-01-02 18:10:00', 'FEE',          'WEB',       -60.00, 'SERVICE', NULL),
(90006, '2026-01-02', 20005, '2026-01-02 11:30:00', 'DEPOSIT',      'API',     30000.00, NULL,      'Cash In'),
(90007, '2026-01-31', 20001, '2026-01-31 23:55:00', 'INTEREST',     'API',       160.00, NULL,      NULL);

-- 放款（按揭/個人貸款）
INSERT INTO core_loan (loan_id, loan_no, customer_id, branch_id, product_id, principal_amt, interest_rate, start_date, end_date, status) VALUES
(30001, 'LOAN-MO-0001', 10002, 1, 201, 3200000.00, 0.02250, '2024-06-01', '2044-06-01', 'ACTIVE'),
(30002, 'LOAN-MO-0002', 10003, 2, 202,  350000.00, 0.06800, '2025-08-15', '2028-08-15', 'ACTIVE'),
(30003, 'LOAN-MO-0003', 10004, 4, 202,  220000.00, 0.07500, '2024-04-10', '2027-04-10', 'DEFAULT');

INSERT INTO fact_loan_balance_daily (biz_date, loan_id, outstanding_bal, overdue_days, overdue_amt) VALUES
('2026-01-01', 30001, 3048000.00, 0,     0.00),
('2026-01-01', 30002,  312000.00, 0,     0.00),
('2026-01-01', 30003,  198000.00, 40, 10000.00),
('2026-01-31', 30001, 3041000.00, 0,     0.00),
('2026-01-31', 30002,  309500.00, 7,  3500.00),
('2026-01-31', 30003,  197100.00, 72, 16000.00);

-- 信用分（月度）
INSERT INTO fact_credit_score_monthly (yyyy_mm, customer_id, score, score_band, model_ver) VALUES
('2025-12', 10001, 775, 'A', 'v1'),
('2025-12', 10002, 705, 'B', 'v1'),
('2025-12', 10003, 635, 'C', 'v1'),
('2025-12', 10004, 585, 'D', 'v1'),
('2025-12', 10005, 760, 'A', 'v1'),
('2026-01', 10001, 780, 'A', 'v1'),
('2026-01', 10002, 700, 'B', 'v1'),
('2026-01', 10003, 630, 'C', 'v1'),
('2026-01', 10004, 565, 'D', 'v1'),
('2026-01', 10005, 765, 'A', 'v1');

-- ----------------------------
-- 4) BI-friendly Views（方便做 KPI；避免直接碰客戶明細）
-- ----------------------------

-- 分行/日期：存款餘額（AUM-like；以 end_balance 聚合）
CREATE OR REPLACE VIEW vw_kpi_deposit_balance_by_branch_date AS
SELECT
  b.biz_date,
  a.branch_id,
  br.branch_name,
  br.region,
  a.currency,
  SUM(b.end_balance) AS total_end_balance,
  SUM(b.available_bal) AS total_available_bal,
  SUM(b.hold_amount) AS total_hold_amount,
  COUNT(DISTINCT a.account_id) AS accounts_cnt
FROM fact_account_balance_daily b
JOIN core_account a ON a.account_id = b.account_id
JOIN dim_branch br ON br.branch_id = a.branch_id
GROUP BY b.biz_date, a.branch_id, br.branch_name, br.region, a.currency;

-- 渠道/日期：交易筆數與淨額
CREATE OR REPLACE VIEW vw_kpi_txn_by_channel_date AS
SELECT
  t.biz_date,
  t.channel,
  COUNT(*) AS txn_cnt,
  SUM(t.amount) AS net_amount
FROM fact_transaction t
GROUP BY t.biz_date, t.channel;

-- 分行/日期：放款風險概覽（含 DPD30）
CREATE OR REPLACE VIEW vw_kpi_loan_risk_by_branch_date AS
SELECT
  lb.biz_date,
  l.branch_id,
  br.branch_name,
  br.region,
  SUM(lb.outstanding_bal) AS total_outstanding,
  SUM(CASE WHEN lb.overdue_days > 0 THEN lb.outstanding_bal ELSE 0 END) AS overdue_outstanding,
  SUM(lb.overdue_amt) AS overdue_amount,
  SUM(CASE WHEN lb.overdue_days >= 30 THEN lb.overdue_amt ELSE 0 END) AS dpd30_overdue_amount
FROM fact_loan_balance_daily lb
JOIN core_loan l ON l.loan_id = lb.loan_id
JOIN dim_branch br ON br.branch_id = l.branch_id
GROUP BY lb.biz_date, l.branch_id, br.branch_name, br.region;

-- Done.
