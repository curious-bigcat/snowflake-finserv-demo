/*=============================================================================
  FINSERV DEMO — Step 02: Base Tables & Synthetic Data
  Creates 7 tables in BASE schema and populates them with GENERATOR()-based
  synthetic data (structured, semi-structured, and unstructured).
=============================================================================*/

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE FINSERV_WH;
USE DATABASE FINSERV_DB;
USE SCHEMA BASE;

-- ============================================================
-- 1. CUSTOMERS (~2,000 rows) — Structured
-- ============================================================

CREATE OR REPLACE TABLE CUSTOMERS (
    CUSTOMER_ID        NUMBER AUTOINCREMENT START 1 INCREMENT 1,
    FIRST_NAME         VARCHAR(50),
    LAST_NAME          VARCHAR(50),
    EMAIL              VARCHAR(100),
    PHONE              VARCHAR(30),
    DATE_OF_BIRTH      DATE,
    CITY               VARCHAR(50),
    STATE              VARCHAR(50),
    COUNTRY            VARCHAR(50),
    ANNUAL_INCOME      NUMBER(12,2),
    EMPLOYMENT_STATUS  VARCHAR(20),
    CREDIT_SCORE       NUMBER(4,0),
    SIGNUP_DATE        TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT PK_CUSTOMERS PRIMARY KEY (CUSTOMER_ID)
);

INSERT INTO CUSTOMERS (FIRST_NAME, LAST_NAME, EMAIL, PHONE, DATE_OF_BIRTH,
                       CITY, STATE, COUNTRY, ANNUAL_INCOME, EMPLOYMENT_STATUS, CREDIT_SCORE, SIGNUP_DATE)
SELECT
    'First_' || SEQ4()                                   AS FIRST_NAME,
    'Last_'  || UNIFORM(1, 500, RANDOM())                AS LAST_NAME,
    'user_'  || SEQ4() || '@example.com'                 AS EMAIL,
    '+1-' || LPAD(UNIFORM(200,999,RANDOM())::TEXT,3,'0')
         || '-' || LPAD(UNIFORM(1000,9999,RANDOM())::TEXT,4,'0') AS PHONE,
    DATEADD('day', -UNIFORM(7300, 25550, RANDOM()), CURRENT_DATE()) AS DATE_OF_BIRTH,
    ARRAY_CONSTRUCT('New York','London','Singapore','Tokyo','Sydney',
                    'Toronto','Mumbai','Dubai','Sao Paulo','Berlin',
                    'Paris','Chicago','Hong Kong','Seoul','Zurich')
        [UNIFORM(0,14,RANDOM())]::VARCHAR                AS CITY,
    ARRAY_CONSTRUCT('NY','England','Central','Kanto','NSW',
                    'ON','MH','Dubai','SP','Berlin',
                    'IDF','IL','HK','Seoul','ZH')
        [UNIFORM(0,14,RANDOM())]::VARCHAR                AS STATE,
    ARRAY_CONSTRUCT('USA','UK','Singapore','Japan','Australia',
                    'Canada','India','UAE','Brazil','Germany',
                    'France','USA','Hong Kong','South Korea','Switzerland')
        [UNIFORM(0,14,RANDOM())]::VARCHAR                AS COUNTRY,
    ROUND(UNIFORM(25000, 500000, RANDOM()), 2)           AS ANNUAL_INCOME,
    ARRAY_CONSTRUCT('EMPLOYED','SELF_EMPLOYED','RETIRED','STUDENT','UNEMPLOYED')
        [UNIFORM(0,4,RANDOM())]::VARCHAR                 AS EMPLOYMENT_STATUS,
    UNIFORM(300, 850, RANDOM())                          AS CREDIT_SCORE,
    DATEADD('day', -UNIFORM(0, 730, RANDOM()), CURRENT_TIMESTAMP()) AS SIGNUP_DATE
FROM TABLE(GENERATOR(ROWCOUNT => 2000));


-- ============================================================
-- 2. ACCOUNTS (~3,000 rows) — Structured
-- ============================================================

CREATE OR REPLACE TABLE ACCOUNTS (
    ACCOUNT_ID      NUMBER AUTOINCREMENT START 1 INCREMENT 1,
    CUSTOMER_ID     NUMBER NOT NULL,
    ACCOUNT_TYPE    VARCHAR(20),
    BALANCE         NUMBER(15,2),
    CREDIT_LIMIT    NUMBER(15,2) DEFAULT 0,
    INTEREST_RATE   NUMBER(6,4),
    OPENED_DATE     DATE,
    STATUS          VARCHAR(15),
    BRANCH_CODE     VARCHAR(10),
    CONSTRAINT PK_ACCOUNTS PRIMARY KEY (ACCOUNT_ID),
    CONSTRAINT FK_ACCOUNTS_CUSTOMER FOREIGN KEY (CUSTOMER_ID) REFERENCES CUSTOMERS(CUSTOMER_ID)
);

INSERT INTO ACCOUNTS (CUSTOMER_ID, ACCOUNT_TYPE, BALANCE, CREDIT_LIMIT, INTEREST_RATE, OPENED_DATE, STATUS, BRANCH_CODE)
SELECT
    UNIFORM(1, 2000, RANDOM())                           AS CUSTOMER_ID,
    ARRAY_CONSTRUCT('CHECKING','SAVINGS','INVESTMENT','CREDIT_CARD')
        [UNIFORM(0,3,RANDOM())]::VARCHAR                 AS ACCOUNT_TYPE,
    ROUND(UNIFORM(100, 250000, RANDOM()), 2)             AS BALANCE,
    CASE WHEN UNIFORM(0,3,RANDOM()) = 3 THEN UNIFORM(5000,100000,RANDOM()) ELSE 0 END AS CREDIT_LIMIT,
    ROUND(UNIFORM(1, 2500, RANDOM()) / 10000.0, 4)      AS INTEREST_RATE,
    DATEADD('day', -UNIFORM(30, 1825, RANDOM()), CURRENT_DATE()) AS OPENED_DATE,
    ARRAY_CONSTRUCT('ACTIVE','ACTIVE','ACTIVE','INACTIVE','CLOSED')
        [UNIFORM(0,4,RANDOM())]::VARCHAR                 AS STATUS,
    'BR-' || LPAD(UNIFORM(1,50,RANDOM())::TEXT, 3, '0') AS BRANCH_CODE
FROM TABLE(GENERATOR(ROWCOUNT => 3000));


-- ============================================================
-- 3. TRANSACTIONS (~10,000 rows) — Structured
-- ============================================================

CREATE OR REPLACE TABLE TRANSACTIONS (
    TXN_ID          NUMBER AUTOINCREMENT START 1 INCREMENT 1,
    ACCOUNT_ID      NUMBER NOT NULL,
    TXN_DATE        TIMESTAMP_NTZ,
    TXN_TYPE        VARCHAR(15),
    AMOUNT          NUMBER(12,2),
    MERCHANT_NAME   VARCHAR(100),
    CATEGORY        VARCHAR(30),
    CHANNEL         VARCHAR(15),
    IS_FLAGGED      BOOLEAN DEFAULT FALSE,
    CONSTRAINT PK_TRANSACTIONS PRIMARY KEY (TXN_ID),
    CONSTRAINT FK_TXN_ACCOUNT FOREIGN KEY (ACCOUNT_ID) REFERENCES ACCOUNTS(ACCOUNT_ID)
);

INSERT INTO TRANSACTIONS (ACCOUNT_ID, TXN_DATE, TXN_TYPE, AMOUNT, MERCHANT_NAME, CATEGORY, CHANNEL, IS_FLAGGED)
SELECT
    UNIFORM(1, 3000, RANDOM())                           AS ACCOUNT_ID,
    DATEADD('second', -UNIFORM(0, 15552000, RANDOM()), CURRENT_TIMESTAMP()) AS TXN_DATE,
    ARRAY_CONSTRUCT('DEBIT','CREDIT','TRANSFER')
        [UNIFORM(0,2,RANDOM())]::VARCHAR                 AS TXN_TYPE,
    ROUND(UNIFORM(1, 50000, RANDOM()) / 10.0, 2)        AS AMOUNT,
    ARRAY_CONSTRUCT('Amazon','Walmart','Starbucks','Shell Gas','Target',
                    'Apple Store','Netflix','Uber','Delta Air','Costco',
                    'Home Depot','Whole Foods','Chase Transfer','Wire Transfer','ATM')
        [UNIFORM(0,14,RANDOM())]::VARCHAR                AS MERCHANT_NAME,
    ARRAY_CONSTRUCT('GROCERIES','DINING','SHOPPING','FUEL','ENTERTAINMENT',
                    'TRAVEL','TRANSFER','UTILITIES','HEALTHCARE','INVESTMENT')
        [UNIFORM(0,9,RANDOM())]::VARCHAR                 AS CATEGORY,
    ARRAY_CONSTRUCT('ONLINE','POS','MOBILE','ATM','BRANCH')
        [UNIFORM(0,4,RANDOM())]::VARCHAR                 AS CHANNEL,
    IFF(UNIFORM(1, 100, RANDOM()) <= 3, TRUE, FALSE)     AS IS_FLAGGED
FROM TABLE(GENERATOR(ROWCOUNT => 10000));


-- ============================================================
-- 4. RISK_ASSESSMENTS (~2,000 rows) — Semi-Structured (VARIANT)
-- ============================================================

CREATE OR REPLACE TABLE RISK_ASSESSMENTS (
    ASSESSMENT_ID   NUMBER AUTOINCREMENT START 1 INCREMENT 1,
    CUSTOMER_ID     NUMBER NOT NULL,
    ASSESSED_AT     TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    RISK_DATA       VARIANT,
    CONSTRAINT PK_RISK PRIMARY KEY (ASSESSMENT_ID),
    CONSTRAINT FK_RISK_CUSTOMER FOREIGN KEY (CUSTOMER_ID) REFERENCES CUSTOMERS(CUSTOMER_ID)
);

INSERT INTO RISK_ASSESSMENTS (CUSTOMER_ID, ASSESSED_AT, RISK_DATA)
SELECT
    UNIFORM(1, 2000, RANDOM())                            AS CUSTOMER_ID,
    DATEADD('day', -UNIFORM(0, 365, RANDOM()), CURRENT_TIMESTAMP()) AS ASSESSED_AT,
    OBJECT_CONSTRUCT(
        'risk_score',       UNIFORM(1, 100, RANDOM()),
        'credit_history',   ARRAY_CONSTRUCT('EXCELLENT','GOOD','FAIR','POOR')[UNIFORM(0,3,RANDOM())]::VARCHAR,
        'debt_to_income',   ROUND(UNIFORM(5, 80, RANDOM()) / 100.0, 2),
        'risk_factors',     ARRAY_CONSTRUCT(
            OBJECT_CONSTRUCT('factor', 'payment_history',  'score', UNIFORM(1,100,RANDOM())),
            OBJECT_CONSTRUCT('factor', 'credit_utilization','score', UNIFORM(1,100,RANDOM())),
            OBJECT_CONSTRUCT('factor', 'account_age',      'score', UNIFORM(1,100,RANDOM()))
        ),
        'assessment_type',  ARRAY_CONSTRUCT('STANDARD','ENHANCED','EXPEDITED')[UNIFORM(0,2,RANDOM())]::VARCHAR,
        'model_version',    'v2.3.' || UNIFORM(0,9,RANDOM())::TEXT
    ) AS RISK_DATA
FROM TABLE(GENERATOR(ROWCOUNT => 2000));


-- ============================================================
-- 5. MARKET_DATA (~5,000 rows) — Semi-Structured (VARIANT)
-- ============================================================

CREATE OR REPLACE TABLE MARKET_DATA (
    MARKET_DATA_ID  NUMBER AUTOINCREMENT START 1 INCREMENT 1,
    TICKER          VARCHAR(10),
    TRADE_DATE      DATE,
    MARKET_DATA     VARIANT,
    CONSTRAINT PK_MARKET PRIMARY KEY (MARKET_DATA_ID)
);

INSERT INTO MARKET_DATA (TICKER, TRADE_DATE, MARKET_DATA)
SELECT
    ARRAY_CONSTRUCT('AAPL','MSFT','GOOGL','AMZN','JPM',
                    'BAC','GS','BRK.B','V','MA')
        [UNIFORM(0,9,RANDOM())]::VARCHAR                  AS TICKER,
    DATEADD('day', -UNIFORM(0, 365, RANDOM()), CURRENT_DATE()) AS TRADE_DATE,
    OBJECT_CONSTRUCT(
        'open',     ROUND(UNIFORM(50, 500, RANDOM()) + UNIFORM(0,99,RANDOM())/100.0, 2),
        'high',     ROUND(UNIFORM(50, 520, RANDOM()) + UNIFORM(0,99,RANDOM())/100.0, 2),
        'low',      ROUND(UNIFORM(40, 490, RANDOM()) + UNIFORM(0,99,RANDOM())/100.0, 2),
        'close',    ROUND(UNIFORM(45, 510, RANDOM()) + UNIFORM(0,99,RANDOM())/100.0, 2),
        'volume',   UNIFORM(100000, 50000000, RANDOM()),
        'indicators', OBJECT_CONSTRUCT(
            'rsi',          ROUND(UNIFORM(10, 90, RANDOM()), 2),
            'macd',         ROUND(UNIFORM(-5, 5, RANDOM()) + UNIFORM(0,99,RANDOM())/100.0, 4),
            'moving_avg_50', ROUND(UNIFORM(50, 500, RANDOM()) + UNIFORM(0,99,RANDOM())/100.0, 2)
        )
    ) AS MARKET_DATA
FROM TABLE(GENERATOR(ROWCOUNT => 5000));


-- ============================================================
-- 6. SUPPORT_TICKETS (~1,000 rows) — Unstructured (TEXT)
-- ============================================================

CREATE OR REPLACE TABLE SUPPORT_TICKETS (
    TICKET_ID          NUMBER AUTOINCREMENT START 1 INCREMENT 1,
    CUSTOMER_ID        NUMBER NOT NULL,
    CREATED_AT         TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    SUBJECT            VARCHAR(200),
    PRIORITY           VARCHAR(10),
    BODY               TEXT,
    RESOLUTION_STATUS  VARCHAR(20),
    ASSIGNED_TO        VARCHAR(50),
    CONSTRAINT PK_TICKETS PRIMARY KEY (TICKET_ID),
    CONSTRAINT FK_TICKETS_CUSTOMER FOREIGN KEY (CUSTOMER_ID) REFERENCES CUSTOMERS(CUSTOMER_ID)
);

INSERT INTO SUPPORT_TICKETS (CUSTOMER_ID, CREATED_AT, SUBJECT, PRIORITY, BODY, RESOLUTION_STATUS, ASSIGNED_TO)
SELECT
    UNIFORM(1, 2000, RANDOM()) AS CUSTOMER_ID,
    DATEADD('hour', -UNIFORM(0, 4380, RANDOM()), CURRENT_TIMESTAMP()) AS CREATED_AT,
    ARRAY_CONSTRUCT(
        'Account access issue',
        'Unauthorized transaction reported',
        'Request for credit limit increase',
        'Mobile app not loading',
        'Wire transfer delay',
        'Incorrect balance displayed',
        'Card declined at merchant',
        'Interest rate dispute',
        'Lost debit card',
        'Duplicate charge on statement',
        'Account closure request',
        'PIN reset needed',
        'Foreign transaction fee inquiry',
        'Direct deposit not received',
        'Fraud alert triggered'
    )[UNIFORM(0,14,RANDOM())]::VARCHAR AS SUBJECT,
    ARRAY_CONSTRUCT('LOW','MEDIUM','HIGH','CRITICAL')
        [UNIFORM(0,3,RANDOM())]::VARCHAR AS PRIORITY,
    ARRAY_CONSTRUCT(
        'I have been unable to access my account for the past 24 hours. Every time I try to log in, I receive an error message saying my credentials are invalid even though I am certain they are correct. I have tried resetting my password twice but the reset email never arrives. This is extremely urgent as I need to make a payment today.',
        'I noticed a transaction on my statement that I did not authorize. The charge is for $2,500 from an online retailer I have never used. I need this investigated immediately and the funds returned to my account. I have not shared my card details with anyone.',
        'I would like to request an increase to my credit card limit. My current limit is $10,000 and I am requesting $25,000. My income has increased significantly in the past year and I have maintained a perfect payment history.',
        'The mobile banking app has been crashing every time I try to view my account summary. I have tried uninstalling and reinstalling the app, clearing the cache, and restarting my phone. I am using the latest version of the app on iOS.',
        'I initiated a wire transfer 5 business days ago and the recipient has not received the funds. The transfer was for $15,000 to a domestic account. The funds have already been debited from my account but the recipient bank says they have no record of the incoming transfer.',
        'My account balance shows $5,000 less than what I calculated based on my recent transactions. I have gone through each transaction in my statement and cannot find the discrepancy. I need someone to review my account history.',
        'My debit card was declined at a grocery store today even though I have sufficient funds in my account. This is the third time this has happened this month. It is very embarrassing and I need this resolved immediately.',
        'I believe the interest rate on my savings account is incorrect. My agreement states 3.5% APY but I am only receiving 2.1%. I have been a customer for over 10 years and I expect this to be corrected retroactively.',
        'I lost my debit card while traveling abroad. I need it cancelled immediately and a replacement sent to my home address. I also need to check if there have been any unauthorized transactions since I lost it yesterday.',
        'I have been charged twice for the same transaction at a restaurant. Both charges are for $85.50 and appeared on the same day. I need one of these charges reversed.'
    )[UNIFORM(0,9,RANDOM())]::TEXT AS BODY,
    ARRAY_CONSTRUCT('OPEN','IN_PROGRESS','RESOLVED','CLOSED','ESCALATED')
        [UNIFORM(0,4,RANDOM())]::VARCHAR AS RESOLUTION_STATUS,
    ARRAY_CONSTRUCT('Support Team','Fraud Team','Card Services','Tech Support','Compliance')
        [UNIFORM(0,4,RANDOM())]::VARCHAR AS ASSIGNED_TO
FROM TABLE(GENERATOR(ROWCOUNT => 1000));


-- ============================================================
-- 7. COMPLIANCE_DOCUMENTS (~200 rows) — Unstructured + Semi-Structured
-- ============================================================

CREATE OR REPLACE TABLE COMPLIANCE_DOCUMENTS (
    DOC_ID         NUMBER AUTOINCREMENT START 1 INCREMENT 1,
    DOC_TYPE       VARCHAR(50),
    DOC_CONTENT    TEXT,
    METADATA       VARIANT,
    CREATED_AT     TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT PK_COMPLIANCE PRIMARY KEY (DOC_ID)
);

INSERT INTO COMPLIANCE_DOCUMENTS (DOC_TYPE, DOC_CONTENT, METADATA, CREATED_AT)
SELECT
    ARRAY_CONSTRUCT('KYC_POLICY','AML_GUIDELINE','RISK_FRAMEWORK','DATA_PRIVACY',
                    'REGULATORY_FILING','AUDIT_REPORT','INCIDENT_REPORT','TRAINING_MATERIAL')
        [UNIFORM(0,7,RANDOM())]::VARCHAR AS DOC_TYPE,
    ARRAY_CONSTRUCT(
        'Know Your Customer (KYC) Policy: All new customer accounts require identity verification through government-issued photo identification, proof of address dated within 90 days, and source of funds documentation for deposits exceeding $10,000. Enhanced due diligence is required for politically exposed persons (PEPs), high-risk jurisdictions, and customers with complex ownership structures. Annual reviews are mandatory for all high-risk accounts.',
        'Anti-Money Laundering (AML) Guidelines: Transaction monitoring systems must flag cash transactions over $10,000, structured transactions that appear designed to evade reporting thresholds, rapid movement of funds between accounts, and transactions involving sanctioned countries or individuals. Suspicious Activity Reports (SARs) must be filed within 30 days of detection. All employees must complete annual AML training.',
        'Enterprise Risk Management Framework: The risk management program encompasses credit risk, market risk, operational risk, and compliance risk. Risk appetite statements are reviewed quarterly by the Board Risk Committee. Key Risk Indicators (KRIs) are monitored daily with automated alerts for threshold breaches. Stress testing is conducted semi-annually under multiple economic scenarios.',
        'Data Privacy Policy: Customer personal data must be encrypted at rest (AES-256) and in transit (TLS 1.3). Data retention periods are 7 years for transaction records and 5 years after account closure for personal data. Customers have the right to access, correct, and delete their personal data under GDPR and CCPA regulations. Third-party data sharing requires explicit customer consent.',
        'Quarterly Regulatory Filing: This filing covers the period ending with comprehensive disclosures on capital adequacy ratios, liquidity coverage ratios, and net stable funding ratios. The institution maintains a Common Equity Tier 1 (CET1) ratio of 12.5%, exceeding the minimum regulatory requirement of 4.5%. Total risk-weighted assets amount to $2.8 billion.',
        'Internal Audit Report: The audit of the consumer lending division identified 3 high-priority findings related to loan origination documentation gaps, 2 medium-priority findings on appraisal independence, and 5 low-priority observations on process efficiency. Management has committed to remediation plans with target completion dates within 90 days for high-priority items.',
        'Cybersecurity Incident Report: On the date of detection, the security operations center identified a phishing campaign targeting employee email accounts. Two accounts were compromised before the attack was contained. No customer data was accessed. Immediate actions included password resets for all affected accounts, enhanced email filtering rules, and mandatory security awareness training for all staff.',
        'Compliance Training Material: This module covers the Bank Secrecy Act (BSA), USA PATRIOT Act requirements, OFAC sanctions compliance, and fair lending regulations. Employees must understand their obligation to identify and report suspicious activity, avoid tipping off subjects of investigations, and maintain the confidentiality of SAR filings. Completion of this training is required annually.'
    )[UNIFORM(0,7,RANDOM())]::TEXT AS DOC_CONTENT,
    OBJECT_CONSTRUCT(
        'regulatory_body',  ARRAY_CONSTRUCT('OCC','FDIC','SEC','CFPB','FinCEN','FRB')
                                [UNIFORM(0,5,RANDOM())]::VARCHAR,
        'status',           ARRAY_CONSTRUCT('ACTIVE','DRAFT','UNDER_REVIEW','ARCHIVED')
                                [UNIFORM(0,3,RANDOM())]::VARCHAR,
        'version',          UNIFORM(1,5,RANDOM())::TEXT || '.0',
        'effective_date',   DATEADD('day', -UNIFORM(0,365,RANDOM()), CURRENT_DATE())::TEXT,
        'review_cycle',     ARRAY_CONSTRUCT('QUARTERLY','SEMI_ANNUAL','ANNUAL')
                                [UNIFORM(0,2,RANDOM())]::VARCHAR,
        'classification',   ARRAY_CONSTRUCT('CONFIDENTIAL','INTERNAL','PUBLIC')
                                [UNIFORM(0,2,RANDOM())]::VARCHAR
    ) AS METADATA,
    DATEADD('day', -UNIFORM(0, 730, RANDOM()), CURRENT_TIMESTAMP()) AS CREATED_AT
FROM TABLE(GENERATOR(ROWCOUNT => 200));


-- ============================================================
-- 8. VERIFY TABLE COUNTS
-- ============================================================

SELECT 'CUSTOMERS'             AS TABLE_NAME, COUNT(*) AS ROW_COUNT FROM CUSTOMERS
UNION ALL SELECT 'ACCOUNTS',             COUNT(*) FROM ACCOUNTS
UNION ALL SELECT 'TRANSACTIONS',         COUNT(*) FROM TRANSACTIONS
UNION ALL SELECT 'RISK_ASSESSMENTS',     COUNT(*) FROM RISK_ASSESSMENTS
UNION ALL SELECT 'MARKET_DATA',          COUNT(*) FROM MARKET_DATA
UNION ALL SELECT 'SUPPORT_TICKETS',      COUNT(*) FROM SUPPORT_TICKETS
UNION ALL SELECT 'COMPLIANCE_DOCUMENTS', COUNT(*) FROM COMPLIANCE_DOCUMENTS
ORDER BY TABLE_NAME;
