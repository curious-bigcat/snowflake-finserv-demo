/*=============================================================================
  FINSERV DEMO — Step 11b: Cortex AI Functions Showcase
  Demonstrates Snowflake's built-in AI/ML SQL functions and Python-based AI
  processing against the financial services data model.

  Sections 1-8:  Classic CORTEX functions (COMPLETE, SENTIMENT, SUMMARIZE,
                 EXTRACT_ANSWER, TRANSLATE, CLASSIFY_TEXT, EMBED_TEXT_768)
  Sections 9-16: Modern AI_ functions (AI_EXTRACT, AI_FILTER, AI_AGG,
                 AI_REDACT, AI_SIMILARITY, AI_SUMMARIZE_AGG,
                 ENTITY_SENTIMENT, AI_COMPLETE with named params)
  Sections 17-18: Python SP and UDF that leverage Cortex AI at scale

  Prerequisites:
    - Files 01-02 deployed (BASE tables populated)
    - Cortex AI functions available in the account region
=============================================================================*/

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE FINSERV_WH;
USE DATABASE FINSERV_DB;


-- ============================================================
-- 1. COMPLETE — Generate Risk Narratives for High-Risk Customers
--    Uses an LLM to produce a human-readable risk summary from
--    structured customer + risk assessment data.
-- ============================================================

SELECT
    c.CUSTOMER_ID,
    c.FIRST_NAME || ' ' || c.LAST_NAME AS CUSTOMER_NAME,
    r.RISK_DATA:risk_score::INT        AS RISK_SCORE,
    r.RISK_DATA:credit_history::VARCHAR AS CREDIT_HISTORY,
    r.RISK_DATA:debt_to_income::FLOAT  AS DEBT_TO_INCOME,
    SNOWFLAKE.CORTEX.COMPLETE(
        'mistral-large2',
        'You are a financial risk analyst. Given the following customer data, '
        || 'write a concise 2-3 sentence risk assessment narrative.\n\n'
        || 'Customer: ' || c.FIRST_NAME || ' ' || c.LAST_NAME || '\n'
        || 'Annual Income: $' || c.ANNUAL_INCOME::VARCHAR || '\n'
        || 'Credit Score: ' || c.CREDIT_SCORE::VARCHAR || '\n'
        || 'Employment: ' || c.EMPLOYMENT_STATUS || '\n'
        || 'Risk Score: ' || r.RISK_DATA:risk_score::VARCHAR || '/100\n'
        || 'Credit History: ' || r.RISK_DATA:credit_history::VARCHAR || '\n'
        || 'Debt-to-Income: ' || ROUND(r.RISK_DATA:debt_to_income::FLOAT, 2)::VARCHAR || '\n'
        || 'Risk Factors: ' || r.RISK_DATA:risk_factors::VARCHAR
    ) AS AI_RISK_NARRATIVE
FROM BASE.CUSTOMERS c
JOIN BASE.RISK_ASSESSMENTS r ON c.CUSTOMER_ID = r.CUSTOMER_ID
WHERE r.RISK_DATA:risk_score::INT > 75
ORDER BY r.RISK_DATA:risk_score::INT DESC
LIMIT 5;


-- ============================================================
-- 2. SENTIMENT — Score Support Ticket Sentiment
--    Identifies the most negative customer interactions for
--    prioritized follow-up by the support team.
-- ============================================================

SELECT
    t.TICKET_ID,
    c.FIRST_NAME || ' ' || c.LAST_NAME AS CUSTOMER_NAME,
    t.SUBJECT,
    t.PRIORITY,
    t.RESOLUTION_STATUS,
    SNOWFLAKE.CORTEX.SENTIMENT(t.BODY) AS SENTIMENT_SCORE,
    CASE
        WHEN SNOWFLAKE.CORTEX.SENTIMENT(t.BODY) < -0.3 THEN 'NEGATIVE'
        WHEN SNOWFLAKE.CORTEX.SENTIMENT(t.BODY) >  0.3 THEN 'POSITIVE'
        ELSE 'NEUTRAL'
    END AS SENTIMENT_LABEL,
    LEFT(t.BODY, 150) || '...' AS BODY_PREVIEW
FROM BASE.SUPPORT_TICKETS t
JOIN BASE.CUSTOMERS c ON t.CUSTOMER_ID = c.CUSTOMER_ID
ORDER BY SENTIMENT_SCORE ASC
LIMIT 15;


-- ============================================================
-- 3. SUMMARIZE — Auto-Abstract Compliance Documents
--    Produces concise summaries of lengthy regulatory documents
--    for quick executive review.
-- ============================================================

SELECT
    DOC_ID,
    DOC_TYPE,
    METADATA:regulatory_body::VARCHAR   AS REGULATORY_BODY,
    METADATA:status::VARCHAR            AS STATUS,
    LENGTH(DOC_CONTENT)                 AS ORIGINAL_LENGTH,
    SNOWFLAKE.CORTEX.SUMMARIZE(DOC_CONTENT) AS AI_SUMMARY
FROM BASE.COMPLIANCE_DOCUMENTS
WHERE METADATA:status::VARCHAR = 'ACTIVE'
ORDER BY DOC_ID
LIMIT 10;


-- ============================================================
-- 4. EXTRACT_ANSWER — Question Answering over Compliance Docs
--    Ask natural-language questions and get precise answers
--    extracted directly from document text.
-- ============================================================

-- Q1: What is the minimum CET1 capital ratio?
SELECT
    DOC_ID,
    DOC_TYPE,
    SNOWFLAKE.CORTEX.EXTRACT_ANSWER(
        DOC_CONTENT,
        'What is the minimum CET1 capital adequacy ratio?'
    ) AS ANSWER
FROM BASE.COMPLIANCE_DOCUMENTS
WHERE DOC_TYPE = 'REGULATORY_FILING';

-- Q2: How long do we retain transaction records?
SELECT
    DOC_ID,
    DOC_TYPE,
    SNOWFLAKE.CORTEX.EXTRACT_ANSWER(
        DOC_CONTENT,
        'How long are transaction records retained?'
    ) AS ANSWER
FROM BASE.COMPLIANCE_DOCUMENTS
WHERE DOC_TYPE = 'DATA_PRIVACY';

-- Q3: When must Suspicious Activity Reports be filed?
SELECT
    DOC_ID,
    DOC_TYPE,
    SNOWFLAKE.CORTEX.EXTRACT_ANSWER(
        DOC_CONTENT,
        'Within how many days must a Suspicious Activity Report be filed?'
    ) AS ANSWER
FROM BASE.COMPLIANCE_DOCUMENTS
WHERE DOC_TYPE = 'AML_GUIDELINE';

-- Q4: What encryption standards are required?
SELECT
    DOC_ID,
    DOC_TYPE,
    SNOWFLAKE.CORTEX.EXTRACT_ANSWER(
        DOC_CONTENT,
        'What encryption standard is required for data at rest?'
    ) AS ANSWER
FROM BASE.COMPLIANCE_DOCUMENTS
WHERE DOC_TYPE = 'DATA_PRIVACY';


-- ============================================================
-- 5. TRANSLATE — Multilingual Support Ticket Subjects
--    Translate ticket subjects to Spanish and French for
--    international support teams.
-- ============================================================

SELECT
    TICKET_ID,
    SUBJECT                                                    AS ORIGINAL_SUBJECT,
    SNOWFLAKE.CORTEX.TRANSLATE(SUBJECT, 'en', 'es')           AS SUBJECT_SPANISH,
    SNOWFLAKE.CORTEX.TRANSLATE(SUBJECT, 'en', 'fr')           AS SUBJECT_FRENCH,
    SNOWFLAKE.CORTEX.TRANSLATE(SUBJECT, 'en', 'de')           AS SUBJECT_GERMAN,
    PRIORITY
FROM BASE.SUPPORT_TICKETS
WHERE PRIORITY IN ('HIGH', 'CRITICAL')
ORDER BY TICKET_ID
LIMIT 10;


-- ============================================================
-- 6. CLASSIFY_TEXT — Auto-Categorize Support Tickets
--    Classify ticket text into business categories without
--    any pre-trained model — pure zero-shot classification.
-- ============================================================

SELECT
    t.TICKET_ID,
    t.SUBJECT,
    t.PRIORITY,
    SNOWFLAKE.CORTEX.CLASSIFY_TEXT(
        t.SUBJECT || ': ' || t.BODY,
        ['Billing & Payments', 'Fraud & Unauthorized Activity',
         'Account Access & Login', 'Technical Issue',
         'Card Services', 'Loan & Mortgage',
         'General Inquiry']
    ):"label"::VARCHAR AS AI_CATEGORY,
    ROUND(SNOWFLAKE.CORTEX.CLASSIFY_TEXT(
        t.SUBJECT || ': ' || t.BODY,
        ['Billing & Payments', 'Fraud & Unauthorized Activity',
         'Account Access & Login', 'Technical Issue',
         'Card Services', 'Loan & Mortgage',
         'General Inquiry']
    ):"confidence"::FLOAT, 3) AS CONFIDENCE,
    t.RESOLUTION_STATUS
FROM BASE.SUPPORT_TICKETS t
ORDER BY CONFIDENCE DESC
LIMIT 20;


-- ============================================================
-- 7. EMBED_TEXT_768 — Semantic Similarity Between Tickets
--    Generate vector embeddings for tickets and find the most
--    similar pairs using cosine distance. Useful for duplicate
--    detection and topic clustering.
-- ============================================================

-- 7a. Preview embeddings (first 5 dimensions shown)
SELECT
    TICKET_ID,
    SUBJECT,
    SNOWFLAKE.CORTEX.EMBED_TEXT_768(
        'snowflake-arctic-embed-m-v1.5',
        SUBJECT || ' ' || BODY
    ) AS EMBEDDING,
    ARRAY_SIZE(
        SNOWFLAKE.CORTEX.EMBED_TEXT_768(
            'snowflake-arctic-embed-m-v1.5',
            SUBJECT || ' ' || BODY
        )
    ) AS EMBEDDING_DIMS
FROM BASE.SUPPORT_TICKETS
LIMIT 5;

-- 7b. Find top 10 most similar ticket pairs
WITH ticket_embeddings AS (
    SELECT
        TICKET_ID,
        SUBJECT,
        PRIORITY,
        SNOWFLAKE.CORTEX.EMBED_TEXT_768(
            'snowflake-arctic-embed-m-v1.5',
            SUBJECT || ' ' || BODY
        ) AS EMBEDDING
    FROM BASE.SUPPORT_TICKETS
    WHERE TICKET_ID <= 100   -- limit scope for demo performance
)
SELECT
    a.TICKET_ID  AS TICKET_A,
    a.SUBJECT    AS SUBJECT_A,
    b.TICKET_ID  AS TICKET_B,
    b.SUBJECT    AS SUBJECT_B,
    ROUND(VECTOR_COSINE_SIMILARITY(
        a.EMBEDDING::VECTOR(FLOAT, 768),
        b.EMBEDDING::VECTOR(FLOAT, 768)
    ), 4) AS COSINE_SIMILARITY
FROM ticket_embeddings a
JOIN ticket_embeddings b
    ON a.TICKET_ID < b.TICKET_ID
ORDER BY COSINE_SIMILARITY DESC
LIMIT 10;


-- ============================================================
-- 8. COMPLETE (Structured JSON) — AI Fraud Risk Assessment
--    Use the LLM to produce structured JSON fraud assessments
--    for flagged transactions, combining transaction context
--    with customer profile data.
-- ============================================================

SELECT
    te.TXN_ID,
    te.CUSTOMER_NAME,
    te.AMOUNT,
    te.MERCHANT_NAME,
    te.CATEGORY,
    te.CHANNEL,
    TRY_PARSE_JSON(
        SNOWFLAKE.CORTEX.COMPLETE(
            'mistral-large2',
            'You are a fraud detection system. Analyze this transaction and return '
            || 'ONLY a JSON object with these fields: '
            || '"fraud_likelihood" (LOW/MEDIUM/HIGH), '
            || '"risk_factors" (array of strings), '
            || '"recommended_action" (string), '
            || '"explanation" (one sentence).\n\n'
            || 'Transaction:\n'
            || '  Amount: $' || te.AMOUNT::VARCHAR || '\n'
            || '  Merchant: ' || te.MERCHANT_NAME || '\n'
            || '  Category: ' || te.CATEGORY || '\n'
            || '  Channel: ' || te.CHANNEL || '\n'
            || '  Customer: ' || te.CUSTOMER_NAME || '\n'
            || '  Credit Score: ' || c.CREDIT_SCORE::VARCHAR || '\n'
            || '  Employment: ' || c.EMPLOYMENT_STATUS || '\n'
            || '  Country: ' || c.COUNTRY || '\n'
            || '  Flagged: TRUE\n\n'
            || 'Return only the JSON object, no other text.'
        )
    ) AS AI_FRAUD_ASSESSMENT
FROM CURATED.DT_TRANSACTION_ENRICHED te
JOIN BASE.CUSTOMERS c ON te.CUSTOMER_ID = c.CUSTOMER_ID
WHERE te.IS_FLAGGED = TRUE
ORDER BY te.AMOUNT DESC
LIMIT 5;


-- ============================================================
-- 9. AI_EXTRACT — Structured Entity Extraction
--    Pull structured fields from unstructured text using a
--    schema definition. No model training required.
-- ============================================================

-- 9a. Extract key compliance fields from document text
SELECT
    DOC_ID,
    DOC_TYPE,
    AI_EXTRACT(
        text  => DOC_CONTENT,
        responseFormat => {
            'regulatory_body':  'Which regulatory body is mentioned?',
            'key_requirement':  'What is the primary compliance requirement?',
            'penalty_or_threshold': 'What monetary thresholds or penalties are mentioned?',
            'review_frequency': 'How often must this be reviewed?'
        }
    ) AS EXTRACTED_FIELDS
FROM BASE.COMPLIANCE_DOCUMENTS
LIMIT 10;

-- 9b. Extract entities from support ticket text
SELECT
    TICKET_ID,
    SUBJECT,
    AI_EXTRACT(
        text  => BODY,
        responseFormat => {
            'product_mentioned': 'What financial product is the customer referring to?',
            'issue_type':        'What is the specific issue or complaint?',
            'urgency':           'How urgent does the customer sound? (low/medium/high)',
            'resolution_sought': 'What resolution is the customer asking for?'
        }
    ) AS EXTRACTED_ENTITIES
FROM BASE.SUPPORT_TICKETS
WHERE PRIORITY IN ('HIGH', 'CRITICAL')
LIMIT 10;


-- ============================================================
-- 10. AI_FILTER — Natural Language Boolean Filtering
--     Filter rows using plain-English conditions evaluated by
--     an LLM. Returns TRUE/FALSE per row.
-- ============================================================

-- 10a. Find tickets where the customer mentions legal action
SELECT
    TICKET_ID,
    SUBJECT,
    PRIORITY,
    LEFT(BODY, 200) || '...' AS BODY_PREVIEW
FROM BASE.SUPPORT_TICKETS
WHERE AI_FILTER(
    PROMPT('Does this support ticket mention or threaten legal action, lawsuits, or attorney involvement? "{0}"', BODY)
)
LIMIT 10;

-- 10b. Filter compliance docs for data-privacy-specific content
SELECT
    DOC_ID,
    DOC_TYPE,
    METADATA:regulatory_body::VARCHAR AS REGULATORY_BODY,
    LEFT(DOC_CONTENT, 200) || '...' AS CONTENT_PREVIEW
FROM BASE.COMPLIANCE_DOCUMENTS
WHERE AI_FILTER(
    PROMPT('Does this document contain requirements about personal data encryption or data retention periods? "{0}"', DOC_CONTENT)
);

-- 10c. AI_FILTER in a JOIN — match tickets to relevant compliance docs
SELECT
    t.TICKET_ID,
    t.SUBJECT AS TICKET_SUBJECT,
    d.DOC_TYPE,
    LEFT(d.DOC_CONTENT, 100) || '...' AS DOC_PREVIEW
FROM BASE.SUPPORT_TICKETS t
JOIN BASE.COMPLIANCE_DOCUMENTS d
    ON AI_FILTER(
        PROMPT('Is this compliance document "{0}" relevant to this customer complaint "{1}"?',
               d.DOC_CONTENT, t.SUBJECT || ': ' || t.BODY)
    )
WHERE t.PRIORITY = 'CRITICAL'
LIMIT 10;


-- ============================================================
-- 11. AI_AGG — Aggregate Insights Across Multiple Rows
--     Analyze patterns across many rows with a custom task
--     description. Not limited by LLM context window.
-- ============================================================

-- 11a. Summarize common themes across all high-priority tickets
SELECT AI_AGG(
    BODY,
    'Identify the top 5 most common complaint themes across these '
    || 'customer support tickets. For each theme, provide a brief '
    || 'description and estimate how many tickets mention it.'
) AS TICKET_THEMES
FROM BASE.SUPPORT_TICKETS
WHERE PRIORITY IN ('HIGH', 'CRITICAL');

-- 11b. Aggregate compliance requirements by regulatory body
SELECT
    METADATA:regulatory_body::VARCHAR AS REGULATORY_BODY,
    AI_AGG(
        DOC_CONTENT,
        'Summarize the key compliance obligations from these documents '
        || 'into a concise bullet-point list. Focus on actionable requirements.'
    ) AS AGGREGATED_REQUIREMENTS
FROM BASE.COMPLIANCE_DOCUMENTS
WHERE METADATA:status::VARCHAR = 'ACTIVE'
GROUP BY METADATA:regulatory_body::VARCHAR;

-- 11c. Analyze transaction patterns for flagged accounts
SELECT AI_AGG(
    'Customer ' || CUSTOMER_NAME || ' spent $' || AMOUNT::VARCHAR
    || ' at ' || MERCHANT_NAME || ' via ' || CHANNEL
    || ' on ' || TXN_DATE::VARCHAR,
    'Analyze these flagged transactions and identify suspicious patterns. '
    || 'What commonalities exist? Are there signs of fraud rings, '
    || 'unusual timing, or merchant concentration?'
) AS FRAUD_PATTERN_ANALYSIS
FROM CURATED.DT_TRANSACTION_ENRICHED
WHERE IS_FLAGGED = TRUE;


-- ============================================================
-- 12. AI_REDACT — PII Redaction from Text
--     Automatically detect and mask personally identifiable
--     information in unstructured text. Essential for
--     compliance (GDPR, CCPA) and safe data sharing.
-- ============================================================

-- 12a. Redact all PII from support ticket bodies
SELECT
    TICKET_ID,
    SUBJECT,
    BODY                AS ORIGINAL_BODY,
    AI_REDACT(BODY)     AS REDACTED_BODY
FROM BASE.SUPPORT_TICKETS
LIMIT 5;

-- 12b. Selective PII redaction — only names and phone numbers
SELECT
    TICKET_ID,
    SUBJECT,
    AI_REDACT(
        input      => BODY,
        categories => ['NAME', 'PHONE_NUMBER']
    ) AS REDACTED_BODY
FROM BASE.SUPPORT_TICKETS
LIMIT 5;

-- 12c. Redact PII then run sentiment (safe analytics pipeline)
SELECT
    TICKET_ID,
    AI_REDACT(BODY)                        AS SAFE_TEXT,
    AI_SENTIMENT(AI_REDACT(BODY))          AS SENTIMENT_SCORE
FROM BASE.SUPPORT_TICKETS
WHERE PRIORITY = 'HIGH'
LIMIT 10;


-- ============================================================
-- 13. AI_SIMILARITY — Semantic Similarity Scoring
--     Compute how semantically similar two texts are without
--     manually generating embeddings. Simplified alternative
--     to EMBED + VECTOR_COSINE_SIMILARITY.
-- ============================================================

-- 13a. Find tickets most similar to a known fraud complaint
SELECT
    TICKET_ID,
    SUBJECT,
    PRIORITY,
    AI_SIMILARITY(
        'I noticed unauthorized charges on my credit card statement '
        || 'that I did not make. Someone may have stolen my card information.',
        BODY
    ) AS SIMILARITY_SCORE
FROM BASE.SUPPORT_TICKETS
ORDER BY SIMILARITY_SCORE DESC
LIMIT 10;

-- 13b. Match compliance docs to a regulatory query
SELECT
    DOC_ID,
    DOC_TYPE,
    METADATA:regulatory_body::VARCHAR AS REGULATORY_BODY,
    AI_SIMILARITY(
        'What are the requirements for customer identity verification '
        || 'and know-your-customer due diligence procedures?',
        DOC_CONTENT
    ) AS RELEVANCE_SCORE
FROM BASE.COMPLIANCE_DOCUMENTS
ORDER BY RELEVANCE_SCORE DESC
LIMIT 5;

-- 13c. Cross-compare: find the compliance doc most relevant to each ticket
SELECT
    t.TICKET_ID,
    t.SUBJECT AS TICKET_SUBJECT,
    d.DOC_ID,
    d.DOC_TYPE,
    AI_SIMILARITY(t.BODY, d.DOC_CONTENT) AS RELEVANCE_SCORE
FROM BASE.SUPPORT_TICKETS t
CROSS JOIN BASE.COMPLIANCE_DOCUMENTS d
WHERE t.PRIORITY = 'CRITICAL'
QUALIFY ROW_NUMBER() OVER (PARTITION BY t.TICKET_ID ORDER BY RELEVANCE_SCORE DESC) = 1
ORDER BY RELEVANCE_SCORE DESC
LIMIT 10;


-- ============================================================
-- 14. AI_SUMMARIZE_AGG — Cross-Row Summarization
--     Summarize text spread across multiple rows without
--     context window limitations. Unlike SUMMARIZE (single-row),
--     this aggregates then summarizes.
-- ============================================================

-- 14a. Summarize all compliance docs into a single executive brief
SELECT AI_SUMMARIZE_AGG(DOC_CONTENT) AS COMPLIANCE_EXECUTIVE_SUMMARY
FROM BASE.COMPLIANCE_DOCUMENTS
WHERE METADATA:status::VARCHAR = 'ACTIVE';

-- 14b. Summarize support tickets per priority level
SELECT
    PRIORITY,
    COUNT(*) AS TICKET_COUNT,
    AI_SUMMARIZE_AGG(BODY) AS PRIORITY_SUMMARY
FROM BASE.SUPPORT_TICKETS
GROUP BY PRIORITY;

-- 14c. Summarize tickets per resolution status
SELECT
    RESOLUTION_STATUS,
    COUNT(*) AS TICKET_COUNT,
    AI_SUMMARIZE_AGG(SUBJECT || ': ' || BODY) AS STATUS_SUMMARY
FROM BASE.SUPPORT_TICKETS
GROUP BY RESOLUTION_STATUS;


-- ============================================================
-- 15. ENTITY_SENTIMENT — Per-Entity Sentiment Analysis
--     Unlike SENTIMENT (overall score), ENTITY_SENTIMENT
--     returns sentiment for each entity mentioned in the text.
--     Identifies which products, services, or people are viewed
--     positively or negatively.
-- ============================================================

SELECT
    TICKET_ID,
    SUBJECT,
    PRIORITY,
    SNOWFLAKE.CORTEX.ENTITY_SENTIMENT(BODY) AS ENTITY_SENTIMENTS
FROM BASE.SUPPORT_TICKETS
WHERE PRIORITY IN ('HIGH', 'CRITICAL')
LIMIT 10;


-- ============================================================
-- 16. AI_COMPLETE (Named Parameters) — Modern Syntax
--     The newer AI_COMPLETE function supports named parameters,
--     structured JSON output via response_format, temperature
--     control, and token usage visibility.
-- ============================================================

-- 16a. Basic named-parameter syntax
SELECT AI_COMPLETE(
    model   => 'mistral-large2',
    prompt  => 'List the top 3 financial risks for a retail bank in 2025. Be concise.'
) AS AI_RESPONSE;

-- 16b. Structured JSON output with response_format schema
SELECT AI_COMPLETE(
    model   => 'mistral-large2',
    prompt  => 'Analyze this transaction: $4,500 wire transfer to an overseas account '
               || 'from a customer with credit score 520 and UNEMPLOYED status.',
    response_format => {
        'type': 'json',
        'schema': {
            'type': 'object',
            'properties': {
                'risk_level':         {'type': 'string'},
                'risk_factors':       {'type': 'array', 'items': {'type': 'string'}},
                'recommended_action': {'type': 'string'},
                'confidence':         {'type': 'number'}
            }
        }
    }
) AS STRUCTURED_ASSESSMENT;

-- 16c. With temperature control and token details
SELECT AI_COMPLETE(
    model            => 'llama3.1-70b',
    prompt           => 'Write a one-paragraph customer retention offer for a PLATINUM '
                        || 'segment customer whose account balance dropped 40% this quarter.',
    model_parameters => {'temperature': 0.7, 'max_tokens': 200},
    show_details     => TRUE
) AS RETENTION_OFFER;


-- ============================================================
-- 17. PYTHON SP — Batch AI Ticket Analyzer
--    Processes support tickets in bulk: sentiment scoring,
--    zero-shot classification, and key phrase extraction.
--    Saves results to CONSUMPTION.AI_TICKET_ANALYSIS.
-- ============================================================

CREATE OR REPLACE PROCEDURE CONSUMPTION.SP_AI_TICKET_ANALYZER(BATCH_SIZE INT)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'
EXECUTE AS CALLER
AS
$$
def run(session, batch_size: int) -> str:
    """
    Batch-process support tickets with Cortex AI functions:
      1. Sentiment analysis on ticket body
      2. Zero-shot classification into business categories
      3. Stores enriched results in CONSUMPTION.AI_TICKET_ANALYSIS
    """
    from snowflake.snowpark.functions import col, lit, call_builtin, sql_expr

    # Create target table if not exists
    session.sql("""
        CREATE TABLE IF NOT EXISTS FINSERV_DB.CONSUMPTION.AI_TICKET_ANALYSIS (
            TICKET_ID          NUMBER,
            CUSTOMER_ID        NUMBER,
            SUBJECT            VARCHAR,
            PRIORITY           VARCHAR,
            RESOLUTION_STATUS  VARCHAR,
            SENTIMENT_SCORE    FLOAT,
            SENTIMENT_LABEL    VARCHAR,
            AI_CATEGORY        VARCHAR,
            AI_CONFIDENCE      FLOAT,
            ANALYZED_AT        TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
        )
    """).collect()

    # Truncate for fresh analysis run
    session.sql("TRUNCATE TABLE FINSERV_DB.CONSUMPTION.AI_TICKET_ANALYSIS").collect()

    # Process in batches using SQL for Cortex function access
    total_processed = 0
    offset = 0

    while True:
        result = session.sql(f"""
            INSERT INTO FINSERV_DB.CONSUMPTION.AI_TICKET_ANALYSIS
                (TICKET_ID, CUSTOMER_ID, SUBJECT, PRIORITY, RESOLUTION_STATUS,
                 SENTIMENT_SCORE, SENTIMENT_LABEL, AI_CATEGORY, AI_CONFIDENCE)
            SELECT
                TICKET_ID,
                CUSTOMER_ID,
                SUBJECT,
                PRIORITY,
                RESOLUTION_STATUS,
                SNOWFLAKE.CORTEX.SENTIMENT(BODY) AS SENTIMENT_SCORE,
                CASE
                    WHEN SNOWFLAKE.CORTEX.SENTIMENT(BODY) < -0.3 THEN 'NEGATIVE'
                    WHEN SNOWFLAKE.CORTEX.SENTIMENT(BODY) >  0.3 THEN 'POSITIVE'
                    ELSE 'NEUTRAL'
                END AS SENTIMENT_LABEL,
                SNOWFLAKE.CORTEX.CLASSIFY_TEXT(
                    SUBJECT || ': ' || BODY,
                    ['Billing & Payments', 'Fraud & Unauthorized Activity',
                     'Account Access & Login', 'Technical Issue',
                     'Card Services', 'Loan & Mortgage', 'General Inquiry']
                ):"label"::VARCHAR AS AI_CATEGORY,
                ROUND(SNOWFLAKE.CORTEX.CLASSIFY_TEXT(
                    SUBJECT || ': ' || BODY,
                    ['Billing & Payments', 'Fraud & Unauthorized Activity',
                     'Account Access & Login', 'Technical Issue',
                     'Card Services', 'Loan & Mortgage', 'General Inquiry']
                ):"confidence"::FLOAT, 3) AS AI_CONFIDENCE
            FROM FINSERV_DB.BASE.SUPPORT_TICKETS
            ORDER BY TICKET_ID
            LIMIT {batch_size} OFFSET {offset}
        """).collect()

        # Check how many rows were inserted
        count_result = session.sql(f"""
            SELECT COUNT(*) AS CNT
            FROM FINSERV_DB.BASE.SUPPORT_TICKETS
            ORDER BY TICKET_ID
            LIMIT {batch_size} OFFSET {offset}
        """).collect()

        batch_count = count_result[0]["CNT"]
        if batch_count == 0:
            break

        total_processed += batch_count
        offset += batch_size

        if batch_count < batch_size:
            break

    return (
        f"AI Ticket Analysis complete: {total_processed} tickets processed. "
        f"Results saved to CONSUMPTION.AI_TICKET_ANALYSIS"
    )
$$;

-- Run the analyzer (process 200 tickets per batch)
-- CALL CONSUMPTION.SP_AI_TICKET_ANALYZER(200);

-- Review results
-- SELECT AI_CATEGORY, SENTIMENT_LABEL,
--        COUNT(*) AS TICKET_COUNT,
--        ROUND(AVG(SENTIMENT_SCORE), 3) AS AVG_SENTIMENT,
--        ROUND(AVG(AI_CONFIDENCE), 3) AS AVG_CONFIDENCE
-- FROM CONSUMPTION.AI_TICKET_ANALYSIS
-- GROUP BY AI_CATEGORY, SENTIMENT_LABEL
-- ORDER BY AI_CATEGORY, SENTIMENT_LABEL;


-- ============================================================
-- 18. PYTHON UDF — Per-Customer Risk Narrative Generator
--     Wraps CORTEX.COMPLETE in a reusable UDF that can be
--     called from any SQL query to generate risk narratives.
-- ============================================================

CREATE OR REPLACE FUNCTION CONSUMPTION.UDF_RISK_NARRATIVE(
    CUSTOMER_NAME VARCHAR,
    ANNUAL_INCOME NUMBER,
    CREDIT_SCORE  NUMBER,
    EMPLOYMENT    VARCHAR,
    RISK_SCORE    NUMBER,
    CREDIT_HISTORY VARCHAR,
    DEBT_TO_INCOME FLOAT
)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'generate_narrative'
AS
$$
import _snowflake

def generate_narrative(customer_name, annual_income, credit_score,
                       employment, risk_score, credit_history, debt_to_income):
    """
    Generates a concise AI-powered risk narrative for a customer
    using Snowflake Cortex COMPLETE via the built-in API.
    """
    prompt = (
        f"You are a financial risk analyst. Write a concise 2-sentence "
        f"risk assessment for this customer.\n\n"
        f"Customer: {customer_name}\n"
        f"Income: ${annual_income:,.0f}\n"
        f"Credit Score: {credit_score}\n"
        f"Employment: {employment}\n"
        f"Risk Score: {risk_score}/100\n"
        f"Credit History: {credit_history}\n"
        f"Debt-to-Income: {debt_to_income:.2f}\n\n"
        f"Respond with only the assessment, no preamble."
    )
    return _snowflake.send_snow_api_request(
        "POST",
        "/api/v2/cortex/inference:complete",
        {},
        {},
        {
            "model": "mistral-large2",
            "messages": [{"role": "user", "content": prompt}],
            "max_tokens": 200
        },
        {},
        30000
    )
$$;

-- Usage example:
-- SELECT
--     c.CUSTOMER_ID,
--     c.FIRST_NAME || ' ' || c.LAST_NAME AS CUSTOMER_NAME,
--     r.RISK_DATA:risk_score::INT AS RISK_SCORE,
--     CONSUMPTION.UDF_RISK_NARRATIVE(
--         c.FIRST_NAME || ' ' || c.LAST_NAME,
--         c.ANNUAL_INCOME,
--         c.CREDIT_SCORE,
--         c.EMPLOYMENT_STATUS,
--         r.RISK_DATA:risk_score::INT,
--         r.RISK_DATA:credit_history::VARCHAR,
--         r.RISK_DATA:debt_to_income::FLOAT
--     ) AS AI_NARRATIVE
-- FROM BASE.CUSTOMERS c
-- JOIN BASE.RISK_ASSESSMENTS r ON c.CUSTOMER_ID = r.CUSTOMER_ID
-- WHERE r.RISK_DATA:risk_score::INT > 80
-- ORDER BY r.RISK_DATA:risk_score::INT DESC
-- LIMIT 3;
