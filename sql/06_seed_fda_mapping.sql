-- ============================================================
-- 06_seed_fda_mapping.sql
-- Loads fda_category_mapping into peptide_radar.silver.fda_category_mapping
-- Safe to re-run (MERGE/idempotent)
-- ============================================================

MERGE INTO peptide_radar.silver.fda_category_mapping AS target
USING (
  SELECT raw_value, normalized_status, confidence, mapping_version, notes
  FROM VALUES
    -- approved
    ('Approved',                     'approved',        'high',   'v1', NULL),
    ('Approved/503A',                'approved',        'high',   'v1', NULL),
    ('Approved/OTC/503A',            'approved',        'high',   'v1', NULL),
    ('Approved (iPLEDGE)',           'approved',        'high',   'v1', 'REMS program'),
    ('Approved/Supplement',          'approved',        'high',   'v1', NULL),
    -- 503a_bulk
    ('503A Bulk',                    '503a_bulk',       'high',   'v1', NULL),
    ('503A',                         '503a_bulk',       'high',   'v1', NULL),
    ('503A (not FDA approved)',      '503a_bulk',       'high',   'v1', NULL),
    -- 503a_eval
    ('503A Eval.',                   '503a_eval',       'high',   'v1', 'Under evaluation for 503A list'),
    -- cosmetic_or_otc
    ('OTC/503A',                     'cosmetic_or_otc', 'medium', 'v1', NULL),
    ('OTC/Rx/503A',                  'cosmetic_or_otc', 'medium', 'v1', NULL),
    ('Cosmetic/503A',                'cosmetic_or_otc', 'medium', 'v1', NULL),
    ('503A/Cosmetic',                'cosmetic_or_otc', 'medium', 'v1', NULL),
    ('GRAS/503A',                    'cosmetic_or_otc', 'medium', 'v1', 'Generally recognized as safe'),
    -- supplement
    ('Supplement',                   'supplement',      'medium', 'v1', NULL),
    ('Supplement/503A',              'supplement',      'medium', 'v1', NULL),
    -- controlled
    ('Schedule II/503A',             'controlled',      'high',   'v1', 'DEA Schedule II'),
    ('Schedule III/503A',            'controlled',      'high',   'v1', 'DEA Schedule III'),
    ('Schedule IV/503A',             'controlled',      'high',   'v1', 'DEA Schedule IV'),
    -- investigational
    ('Investigational/503A',         'investigational', 'high',   'v1', 'IND filed or active')
  AS t(raw_value, normalized_status, confidence, mapping_version, notes)
) AS source
ON target.raw_value = source.raw_value
WHEN MATCHED THEN UPDATE SET
  target.normalized_status = source.normalized_status,
  target.confidence        = source.confidence,
  target.mapping_version   = source.mapping_version,
  target.notes             = source.notes
WHEN NOT MATCHED THEN INSERT (
  raw_value, normalized_status, confidence, mapping_version, notes
) VALUES (
  source.raw_value, source.normalized_status, source.confidence,
  source.mapping_version, source.notes
);

SELECT COUNT(*) AS mapping_rows FROM peptide_radar.silver.fda_category_mapping;
-- Expected: 20
