-- ============================================================
-- 05_seed_watchlist_FIXED.sql
-- Corrected 2026-05-21 — name mismatches resolved against
-- LOWER(peptide_name_generic) from elevanbio_dev.bronze.peptide_database_raw
-- pentagastrin and sincalide removed (not in source)
-- All 36 peptides have validated canonical_name values
-- ============================================================
-- SAFE TO RUN: uses MERGE, idempotent

MERGE INTO peptide_radar.silver.peptides AS target
USING (
  SELECT
    md5(canonical_name)                         AS peptide_id,
    canonical_name,
    seed_source,
    CAST(strategic_fit_score AS FLOAT)          AS strategic_fit_score,
    TRUE                                        AS watchlist_active,
    current_timestamp()                         AS last_updated,
    notes
  FROM VALUES
    -- 8 FDA PreCheck compounds (highest priority)
    ('vasopressin',                        'peptide_database_raw', 0.90, 'FDA PreCheck compound; 503A bulk; critical compounding candidate'),
    ('desmopressin',                       'peptide_database_raw', 0.88, 'FDA PreCheck compound; synthetic ADH analog'),
    ('oxytocin',                           'peptide_database_raw', 0.87, 'FDA PreCheck compound; 503A bulk'),
    ('glucagon',                           'peptide_database_raw', 0.85, 'FDA PreCheck compound; emergency hypoglycemia'),
    ('leuprolide',                         'peptide_database_raw', 0.83, 'FDA PreCheck compound; GnRH agonist'),
    ('octreotide',                         'peptide_database_raw', 0.82, 'FDA PreCheck compound; somatostatin analog'),
    ('bivalirudin',                        'peptide_database_raw', 0.80, 'FDA PreCheck compound; direct thrombin inhibitor'),
    ('liraglutide',                        'peptide_database_raw', 0.79, 'FDA PreCheck compound; GLP-1 agonist'),

    -- High-value research peptides — CORRECTED canonical_names matching source
    ('sermorelin',                         'peptide_database_raw', 0.75, 'GHRH analog; anti-aging/HGH secretagogue'),
    ('gonadorelin',                        'peptide_database_raw', 0.72, 'GnRH; fertility; veterinary cross-over'),
    ('thymosin alpha-1',                   'peptide_database_raw', 0.74, 'Immune modulation; oncology adjuvant'),
    ('ipamorelin',                         'peptide_database_raw', 0.71, 'Selective GH secretagogue; compounding demand high'),
    ('cjc-1295',                           'peptide_database_raw', 0.70, 'GHRH analog; often combined with ipamorelin'),
    ('bpc-157',                            'peptide_database_raw', 0.68, 'Tissue repair; high compounding interest; regulatory unclear'),
    ('tb-500 (thymosin beta-4 fragment)',   'peptide_database_raw', 0.67, 'Tissue repair; wound healing'),
    ('delta sleep inducing peptide (dsip)', 'peptide_database_raw', 0.55, 'Sleep peptide; niche compounding use'),
    ('selank',                             'peptide_database_raw', 0.60, 'Anxiolytic; Russian-origin; growing US interest'),
    ('semax',                              'peptide_database_raw', 0.58, 'Nootropic; ACTH analog; intranasal delivery'),
    ('epithalon',                          'peptide_database_raw', 0.62, 'Telomerase activator; longevity; high interest'),
    ('mots-c (mitochondrial orf peptide)', 'peptide_database_raw', 0.65, 'Mitochondrial peptide; metabolic; longevity'),
    ('collagen tripeptide ghk-cu',         'peptide_database_raw', 0.58, 'Copper peptide; skin/wound healing; cosmetic crossover'),
    ('kpv (lys-pro-val)',                  'peptide_database_raw', 0.60, 'Anti-inflammatory; IBD; skin conditions'),
    ('kisspeptin-54 / kisspeptin-10',      'peptide_database_raw', 0.63, 'Reproductive endocrinology; GnRH pulse modulator'),
    ('aod-9604',                           'peptide_database_raw', 0.66, 'Lipolytic fragment of HGH; metabolic'),
    ('melanotan ii',                       'peptide_database_raw', 0.50, 'Melanocyte stimulator; tanning; erectile; regulatory risk'),
    ('bremelanotide',                      'peptide_database_raw', 0.55, 'FDA-approved PT-141; HSDD; compounding post-approval dynamics'),
    ('ll-37 (cathelicidin)',               'peptide_database_raw', 0.62, 'Antimicrobial peptide; wound care; emerging'),
    ('secretin (human)',                   'peptide_database_raw', 0.52, 'GI peptide; diagnostic use; human form only'),
    ('somatostatin',                       'peptide_database_raw', 0.60, 'Hormone inhibitor; oncology; GI'),
    ('terlipressin',                       'peptide_database_raw', 0.65, 'Vasopressin analog; hepatorenal; ICU'),
    ('cetrorelix',                         'peptide_database_raw', 0.58, 'GnRH antagonist; fertility'),
    ('ganirelix',                          'peptide_database_raw', 0.57, 'GnRH antagonist; fertility'),
    ('alarelin',                           'peptide_database_raw', 0.54, 'GnRH analog; veterinary and fertility'),
    ('hexarelin',                          'peptide_database_raw', 0.60, 'GH secretagogue; cardiac effects'),
    ('tesamorelin',                        'peptide_database_raw', 0.69, 'FDA-approved GHRH analog; lipodystrophy')
  AS t(canonical_name, seed_source, strategic_fit_score, notes)
) AS source
ON target.canonical_name = source.canonical_name
WHEN MATCHED THEN UPDATE SET
  target.seed_source         = source.seed_source,
  target.strategic_fit_score = source.strategic_fit_score,
  target.watchlist_active    = TRUE,
  target.last_updated        = current_timestamp(),
  target.notes               = source.notes
WHEN NOT MATCHED THEN INSERT (
  peptide_id, canonical_name, seed_source,
  strategic_fit_score, watchlist_active, last_updated, notes
) VALUES (
  source.peptide_id, source.canonical_name, source.seed_source,
  source.strategic_fit_score, TRUE, current_timestamp(), source.notes
);

-- Verify
SELECT COUNT(*) AS total_seeded,
       SUM(CASE WHEN watchlist_active THEN 1 ELSE 0 END) AS active
FROM peptide_radar.silver.peptides;
-- Expected: total_seeded = 36, active = 36
