/*=============================================================================
  FINSERV DEMO — Step 02: Base Tables & Synthetic Data
  Creates 7 tables in BASE schema. 4 tables are populated with GENERATOR()-based
  synthetic data here; 3 tables (TRANSACTIONS, RISK_ASSESSMENTS, SUPPORT_TICKETS)
  are populated via CSV → S3 → Snowpipe ingestion in file 04.
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
WITH BASE AS (
    SELECT
        ROW_NUMBER() OVER (ORDER BY SEQ4()) AS RN,
        -- 100 diverse international first names
        ARRAY_CONSTRUCT(
            'James','Michael','Robert','David','John','William','Daniel','Matthew',
            'Andrew','Christopher','Joseph','Ryan','Alexander','Benjamin','Samuel',
            'Nathan','Jacob','Ethan','Noah','Oliver','Lucas','Mason','Logan','Leo',
            'Felix','Liam','Aiden','Sebastian','Henry','Oscar',
            'Isaac','Gabriel','Julian','Adrian',
            'Mary','Jennifer','Sarah','Jessica','Emily','Amanda','Ashley','Elizabeth',
            'Sophia','Emma','Olivia','Isabella','Mia','Ava','Charlotte','Amelia',
            'Harper','Abigail','Ella','Grace','Chloe','Lily','Hannah','Zoe',
            'Ruby','Alice','Stella','Hazel','Aurora','Luna','Ellie','Violet',
            'Raj','Arjun','Vikram','Priya','Ananya','Neha','Aditya','Kavya',
            'Hiroshi','Kenji','Sakura','Yuki','Yuto','Hana',
            'Wei','Mei','Lin','Jing',
            'Mohammed','Omar','Fatima','Aisha',
            'Carlos','Miguel','Pedro','Maria','Ana','Sofia',
            'Marco','Luca','Lars','Astrid','Pierre','Claire'
        )[UNIFORM(0,99,RANDOM())]::VARCHAR AS FIRST_NAME,
        -- 80 diverse international last names
        ARRAY_CONSTRUCT(
            'Smith','Johnson','Williams','Brown','Jones','Garcia','Miller','Davis',
            'Rodriguez','Martinez','Anderson','Taylor','Thomas','Hernandez','Moore',
            'Martin','Jackson','Thompson','White','Harris','Clark','Lewis','Robinson',
            'Walker','Young','Allen','King','Wright','Scott','Green',
            'Patel','Shah','Kumar','Singh','Gupta','Sharma','Reddy','Nair',
            'Nakamura','Tanaka','Yamamoto','Suzuki','Sato','Watanabe',
            'Wang','Li','Zhang','Chen','Liu','Yang',
            'Kim','Park','Lee','Cho','Jung',
            'Ahmed','Ali','Khan','Hassan','Ibrahim',
            'Silva','Santos','Oliveira','Costa','Ferreira',
            'Mueller','Schmidt','Fischer','Weber','Schneider',
            'Dubois','Laurent','Moreau','Bernard','Petit',
            'Nielsen','Berg','Lindqvist','Johansson','Eriksson'
        )[UNIFORM(0,79,RANDOM())]::VARCHAR AS LAST_NAME,
        -- Location index (keeps city/state/country correlated)
        UNIFORM(0,21,RANDOM()) AS LOC_IDX,
        UNIFORM(0,11,RANDOM()) AS DOMAIN_IDX,
        UNIFORM(0,4,RANDOM()) AS EMAIL_STYLE,
        DATEADD('day', -UNIFORM(7300, 25550, RANDOM()), CURRENT_DATE()) AS DATE_OF_BIRTH,
        DATEADD('day', -UNIFORM(0, 730, RANDOM()), CURRENT_TIMESTAMP()) AS SIGNUP_DATE,
        -- Weighted employment: ~55% employed, 15% self-employed, 15% retired, 10% student, 5% unemployed
        ARRAY_CONSTRUCT('EMPLOYED','EMPLOYED','EMPLOYED','EMPLOYED','EMPLOYED',
                        'EMPLOYED','EMPLOYED','EMPLOYED','EMPLOYED','EMPLOYED','EMPLOYED',
                        'SELF_EMPLOYED','SELF_EMPLOYED','SELF_EMPLOYED',
                        'RETIRED','RETIRED','RETIRED',
                        'STUDENT','STUDENT',
                        'UNEMPLOYED')[UNIFORM(0,19,RANDOM())]::VARCHAR AS EMP_STATUS
    FROM TABLE(GENERATOR(ROWCOUNT => 2000))
)
SELECT
    FIRST_NAME,
    LAST_NAME,
    -- Realistic email built from name parts
    CASE EMAIL_STYLE
        WHEN 0 THEN LOWER(FIRST_NAME) || '.' || LOWER(LAST_NAME)
        WHEN 1 THEN LOWER(FIRST_NAME) || LOWER(LAST_NAME)
        WHEN 2 THEN LOWER(FIRST_NAME) || '_' || LOWER(LAST_NAME)
        WHEN 3 THEN LOWER(FIRST_NAME) || '.' || LOWER(LAST_NAME) || LPAD(MOD(RN*7,100)::TEXT,2,'0')
        ELSE LOWER(FIRST_NAME) || LOWER(LAST_NAME) || LPAD(MOD(RN*13,1000)::TEXT,3,'0')
    END || '@' ||
    ARRAY_CONSTRUCT('gmail.com','yahoo.com','outlook.com','icloud.com','hotmail.com',
                    'protonmail.com','aol.com','mail.com','zoho.com','fastmail.com',
                    'hey.com','gmx.com')[DOMAIN_IDX]::VARCHAR AS EMAIL,
    -- Phone with country-appropriate dial code
    CASE ARRAY_CONSTRUCT('USA','USA','USA','USA','USA',
                         'UK','UK','Singapore','Japan','Australia',
                         'Australia','Canada','Canada','India','UAE',
                         'Brazil','Germany','Germany','France','Switzerland',
                         'Hong Kong','South Korea')[LOC_IDX]::VARCHAR
        WHEN 'USA'         THEN '+1'
        WHEN 'UK'          THEN '+44'
        WHEN 'Canada'      THEN '+1'
        WHEN 'Australia'   THEN '+61'
        WHEN 'Japan'       THEN '+81'
        WHEN 'Germany'     THEN '+49'
        WHEN 'India'       THEN '+91'
        WHEN 'Singapore'   THEN '+65'
        WHEN 'UAE'         THEN '+971'
        WHEN 'Brazil'      THEN '+55'
        WHEN 'France'      THEN '+33'
        WHEN 'Switzerland' THEN '+41'
        WHEN 'Hong Kong'   THEN '+852'
        WHEN 'South Korea' THEN '+82'
    END || '-' ||
    LPAD(UNIFORM(200,999,RANDOM())::TEXT,3,'0') || '-' ||
    LPAD(UNIFORM(100,999,RANDOM())::TEXT,3,'0') || '-' ||
    LPAD(UNIFORM(1000,9999,RANDOM())::TEXT,4,'0') AS PHONE,
    DATE_OF_BIRTH,
    -- 22 international cities (correlated city/state/country via LOC_IDX)
    ARRAY_CONSTRUCT('New York','Los Angeles','Chicago','Houston','Miami',
                    'London','Manchester','Singapore','Tokyo','Sydney',
                    'Melbourne','Toronto','Vancouver','Mumbai','Dubai',
                    'Sao Paulo','Berlin','Frankfurt','Paris','Zurich',
                    'Hong Kong','Seoul')[LOC_IDX]::VARCHAR AS CITY,
    ARRAY_CONSTRUCT('NY','CA','IL','TX','FL',
                    'England','England','Central','Kanto','NSW',
                    'VIC','ON','BC','MH','Dubai',
                    'SP','Berlin','Hessen','IDF','ZH',
                    'HK','Seoul')[LOC_IDX]::VARCHAR AS STATE,
    ARRAY_CONSTRUCT('USA','USA','USA','USA','USA',
                    'UK','UK','Singapore','Japan','Australia',
                    'Australia','Canada','Canada','India','UAE',
                    'Brazil','Germany','Germany','France','Switzerland',
                    'Hong Kong','South Korea')[LOC_IDX]::VARCHAR AS COUNTRY,
    -- Income correlated with employment status
    ROUND(CASE EMP_STATUS
        WHEN 'EMPLOYED'      THEN UNIFORM(30000, 350000, RANDOM())
        WHEN 'SELF_EMPLOYED' THEN UNIFORM(20000, 500000, RANDOM())
        WHEN 'RETIRED'       THEN UNIFORM(25000, 250000, RANDOM())
        WHEN 'STUDENT'       THEN UNIFORM(5000, 45000, RANDOM())
        WHEN 'UNEMPLOYED'    THEN UNIFORM(0, 35000, RANDOM())
    END, 2) AS ANNUAL_INCOME,
    EMP_STATUS AS EMPLOYMENT_STATUS,
    -- Credit score correlated with employment status
    LEAST(850, GREATEST(300,
        CASE EMP_STATUS
            WHEN 'EMPLOYED'      THEN UNIFORM(580, 820, RANDOM())
            WHEN 'SELF_EMPLOYED' THEN UNIFORM(550, 800, RANDOM())
            WHEN 'RETIRED'       THEN UNIFORM(620, 840, RANDOM())
            WHEN 'STUDENT'       THEN UNIFORM(350, 700, RANDOM())
            WHEN 'UNEMPLOYED'    THEN UNIFORM(300, 650, RANDOM())
        END
    )) AS CREDIT_SCORE,
    SIGNUP_DATE
FROM BASE;


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
WITH ACCT_BASE AS (
    SELECT
        UNIFORM(1, 2000, RANDOM()) AS CUSTOMER_ID,
        -- Weighted: ~35% checking, ~30% savings, ~20% credit_card, ~15% investment
        ARRAY_CONSTRUCT('CHECKING','CHECKING','CHECKING','CHECKING','CHECKING','CHECKING','CHECKING',
                        'SAVINGS','SAVINGS','SAVINGS','SAVINGS','SAVINGS','SAVINGS',
                        'CREDIT_CARD','CREDIT_CARD','CREDIT_CARD','CREDIT_CARD',
                        'INVESTMENT','INVESTMENT','INVESTMENT')
            [UNIFORM(0,19,RANDOM())]::VARCHAR AS ACCOUNT_TYPE,
        DATEADD('day', -UNIFORM(30, 1825, RANDOM()), CURRENT_DATE()) AS OPENED_DATE,
        ARRAY_CONSTRUCT('ACTIVE','ACTIVE','ACTIVE','INACTIVE','CLOSED')
            [UNIFORM(0,4,RANDOM())]::VARCHAR AS STATUS,
        'BR-' || LPAD(UNIFORM(1,50,RANDOM())::TEXT, 3, '0') AS BRANCH_CODE
    FROM TABLE(GENERATOR(ROWCOUNT => 3000))
)
SELECT
    CUSTOMER_ID,
    ACCOUNT_TYPE,
    -- Balance varies by account type
    ROUND(CASE ACCOUNT_TYPE
        WHEN 'CHECKING'    THEN UNIFORM(100, 50000, RANDOM())
        WHEN 'SAVINGS'     THEN UNIFORM(500, 150000, RANDOM())
        WHEN 'CREDIT_CARD' THEN UNIFORM(0, 25000, RANDOM())
        WHEN 'INVESTMENT'  THEN UNIFORM(5000, 500000, RANDOM())
    END, 2) AS BALANCE,
    -- Credit limit only for credit cards
    CASE ACCOUNT_TYPE
        WHEN 'CREDIT_CARD' THEN UNIFORM(5000, 100000, RANDOM())
        ELSE 0
    END AS CREDIT_LIMIT,
    -- Interest rate varies by account type
    ROUND(CASE ACCOUNT_TYPE
        WHEN 'CHECKING'    THEN UNIFORM(1, 50, RANDOM()) / 10000.0
        WHEN 'SAVINGS'     THEN UNIFORM(150, 500, RANDOM()) / 10000.0
        WHEN 'CREDIT_CARD' THEN UNIFORM(1500, 2500, RANDOM()) / 10000.0
        WHEN 'INVESTMENT'  THEN UNIFORM(300, 1200, RANDOM()) / 10000.0
    END, 4) AS INTEREST_RATE,
    OPENED_DATE,
    STATUS,
    BRANCH_CODE
FROM ACCT_BASE;


-- ============================================================
-- 3. TRANSACTIONS — Table only (data loaded via Snowpipe, see file 04)
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
-- NOTE: ~10,000 rows loaded via CSV → S3 → Snowpipe (file 04)


-- ============================================================
-- 4. RISK_ASSESSMENTS — Table only (data loaded via Snowpipe, see file 04)
-- ============================================================

CREATE OR REPLACE TABLE RISK_ASSESSMENTS (
    ASSESSMENT_ID   NUMBER AUTOINCREMENT START 1 INCREMENT 1,
    CUSTOMER_ID     NUMBER NOT NULL,
    ASSESSED_AT     TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
    RISK_DATA       VARIANT,
    CONSTRAINT PK_RISK PRIMARY KEY (ASSESSMENT_ID),
    CONSTRAINT FK_RISK_CUSTOMER FOREIGN KEY (CUSTOMER_ID) REFERENCES CUSTOMERS(CUSTOMER_ID)
);
-- NOTE: ~2,000 rows loaded via CSV → S3 → Snowpipe (file 04)


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
-- 6. SUPPORT_TICKETS — Table only (data loaded via Snowpipe, see file 04)
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
-- NOTE: ~1,000 rows loaded via CSV → S3 → Snowpipe (file 04)


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
-- CUSTOMERS=2000, ACCOUNTS=3000, MARKET_DATA=5000, COMPLIANCE_DOCUMENTS=200
-- TRANSACTIONS, RISK_ASSESSMENTS, SUPPORT_TICKETS = 0 (populated via Snowpipe in file 04)

SELECT 'CUSTOMERS'             AS TABLE_NAME, COUNT(*) AS ROW_COUNT FROM CUSTOMERS
UNION ALL SELECT 'ACCOUNTS',             COUNT(*) FROM ACCOUNTS
UNION ALL SELECT 'TRANSACTIONS',         COUNT(*) FROM TRANSACTIONS
UNION ALL SELECT 'RISK_ASSESSMENTS',     COUNT(*) FROM RISK_ASSESSMENTS
UNION ALL SELECT 'MARKET_DATA',          COUNT(*) FROM MARKET_DATA
UNION ALL SELECT 'SUPPORT_TICKETS',      COUNT(*) FROM SUPPORT_TICKETS
UNION ALL SELECT 'COMPLIANCE_DOCUMENTS', COUNT(*) FROM COMPLIANCE_DOCUMENTS
ORDER BY TABLE_NAME;
