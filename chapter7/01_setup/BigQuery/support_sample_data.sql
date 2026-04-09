-- Sample data for Chapter 7 running example tables.
-- BigQuery-style SQL; adjust dataset names as needed.

-- Product catalog entries
INSERT INTO `oreilly_support.product_catalog` (
  product_id,
  product_name,
  product_family,
  tier,
  is_active
)
VALUES
  ('prod_analytics_basic',    'Acme Analytics Basic',    'analytics', 'standard',   TRUE),
  ('prod_analytics_pro',      'Acme Analytics Pro',      'analytics', 'enterprise', TRUE),
  ('prod_messaging',          'Acme Messaging',          'messaging', 'standard',   TRUE),
  ('prod_messaging_premium',  'Acme Messaging Premium',  'messaging', 'enterprise', TRUE);

-- Knowledge base articles used by support agents
INSERT INTO `oreilly_support.support_kb_articles` (
  article_id,
  title,
  body,
  product_ids,
  language,
  updated_at
)
VALUES
  (
    'kb_login_timeout',
    'Troubleshooting login timeouts',
    'If users experience login timeouts, first check service status, then clear browser cache and confirm SSO configuration.',
    ['prod_analytics_basic', 'prod_analytics_pro'],
    'en',
    TIMESTAMP '2025-01-05 10:00:00+00'
  ),
  (
    'kb_slow_dashboards',
    'Improving slow dashboard performance',
    'Slow dashboards are often caused by unfiltered queries or large date ranges. Recommend adding filters and using aggregated tables.',
    ['prod_analytics_pro'],
    'en',
    TIMESTAMP '2025-01-06 14:30:00+00'
  ),
  (
    'kb_missing_notifications',
    'Missing notification emails',
    'When notification emails are missing, verify user notification settings, spam folders, and domain allowlists.',
    ['prod_messaging', 'prod_messaging_premium'],
    'en',
    TIMESTAMP '2025-01-07 09:15:00+00'
  );

-- Recent support tickets
INSERT INTO `oreilly_support.support_tickets` (
  ticket_id,
  customer_id,
  created_at,
  channel,
  subject,
  body,
  product_id,
  status,
  resolution_code
)
VALUES
  (
    'tkt_1001',
    'cust_acme_bank',
    TIMESTAMP '2025-01-08 09:05:00+00',
    'email',
    'Users cannot log in to analytics',
    'Several users are reporting that the analytics app times out on the login screen after 30 seconds.',
    'prod_analytics_basic',
    'open',
    NULL
  ),
  (
    'tkt_1002',
    'cust_acme_bank',
    TIMESTAMP '2025-01-08 11:22:00+00',
    'chat',
    'Intermittent login timeout errors',
    'Login works for some users, but others see a timeout error. This started after enabling SSO.',
    'prod_analytics_basic',
    'in_progress',
    NULL
  ),
  (
    'tkt_1003',
    'cust_northwind',
    TIMESTAMP '2025-01-09 15:40:00+00',
    'email',
    'Executive dashboard is very slow',
    'Our executive dashboard takes more than a minute to load, especially for last 12 months of data.',
    'prod_analytics_pro',
    'open',
    NULL
  ),
  (
    'tkt_1004',
    'cust_contoso',
    TIMESTAMP '2025-01-09 16:05:00+00',
    'chat',
    'Notification emails not being delivered',
    'Users stopped receiving notification emails from the messaging product yesterday, even though in-app notifications still appear.',
    'prod_messaging',
    'open',
    NULL
  ),
  (
    'tkt_1005',
    'cust_contoso',
    TIMESTAMP '2025-01-10 08:50:00+00',
    'email',
    'Some notifications go to spam',
    'A few users say that notifications from the premium messaging product are landing in spam folders.',
    'prod_messaging_premium',
    'resolved',
    'known_issue'
  );
----

