-- Peptide Radar -- Phase 0, Step 3: Seed watchlist into silver.peptides
-- Run in Databricks SQL Editor
-- Idempotent: MERGE avoids duplicates on re-run

-- peptide_id = SHA-256 of canonical_name (precomputed in watchlist_seed.csv).
-- canonical_name = LOWER(peptide_name_generic) from source, matched exactly.
-- Defaults that Delta DDL could not set are applied here:
--   watchlist_active = TRUE, last_updated = current_timestamp()

MERGE INTO peptide_radar.silver.peptides AS target
USING (
  VALUES
    ('0717fd030271b7c1f7eac2e81c57f82a6b664a1d527991a2061c99ea6e97f700', 'vasopressin', 'peptide_database_raw', 0.95),
    ('5b34c410a1475413467dda93e89e9e942e2524670330a462476a8739a261b324', 'desmopressin', 'peptide_database_raw', 0.8),
    ('ddd789a86b90b2f448e89ac8bb4b6a2c19d0e493d4ecfa3280ab8ddaa933f979', 'oxytocin', 'peptide_database_raw', 0.95),
    ('5663be1dbfd72b949b8faa44bcf5c844eaab71ad37fc26db7e720a0e1d002e00', 'glucagon', 'peptide_database_raw', 0.75),
    ('7f87c0f3cead37163aeab4962deb75af64693aa27604efcb39e8eae80d95ffda', 'leuprolide', 'peptide_database_raw', 0.8),
    ('b585c67b7e3b7d1f58c7765cd356b200f5b8514bfdcad0d6864f484a456a4eb1', 'octreotide', 'peptide_database_raw', 0.8),
    ('50100f40fffc16d4eb8e5d729f2e91ee8f7ff0a28c7c83095a4831052896a44b', 'bivalirudin', 'peptide_database_raw', 0.7),
    ('f24132a6414bb496c999b58a4cd83227b9eb0c567e86103264324baa891a273c', 'liraglutide', 'peptide_database_raw', 0.65),
    ('0ca8dc9d9998af9b652627cc9a960b88c81b70962a75bc9c47321065522d42c3', 'sermorelin', 'peptide_database_raw', 0.9),
    ('e9ec54abf1ae4d9986b0f95e44081d4c39d802606a3a2c9befc3a3916dcb2750', 'gonadorelin', 'peptide_database_raw', 0.9),
    ('c4df17660cf2dc7c12ef29c7069866f7b328571e86a8ab697aa377172ac2c195', 'thymosin alpha-1', 'peptide_database_raw', 0.85),
    ('4ef5b2f0f4b5eec660773ea56277216fe711c1d170603adc7a80253579aba910', 'ipamorelin', 'peptide_database_raw', 0.8),
    ('b9e53052dc2d15c819621f366b77019b196806c532ad958112f85efcdefbbdc8', 'cjc-1295', 'peptide_database_raw', 0.75),
    ('d278f6c97bb2c4af4c524adb8ff90082576be657a6f658ff078a52be29b94a6d', 'bpc-157', 'peptide_database_raw', 0.85),
    ('ed4a5ceb8d783f70610762f1e3070942e072fc66e1d147539684feda8a8f6723', 'tb-500 (thymosin beta-4 fragment)', 'peptide_database_raw', 0.8),
    ('fc994e7e3547a402b97351ec384c82069a90c5f21a48a5826b498a3f94c31441', 'delta sleep inducing peptide (dsip)', 'peptide_database_raw', 0.55),
    ('daa9a4e2445149cb354265fb9fb502c765442cca33be2abde1fb5cd22b5623e3', 'selank', 'peptide_database_raw', 0.6),
    ('331b97d4097f61d7aed9a26478bc657d8c66ba238a0ced53734291a975bbf904', 'semax', 'peptide_database_raw', 0.6),
    ('b5888a82cab9fe94debf4794db13d04505b2672775053b0940471b077ebeff3f', 'epithalon', 'peptide_database_raw', 0.55),
    ('c7c4d604f3ca77911d12a053479ae7bbd9cb92f20c2b46eb46dd80a0c041c56d', 'mots-c (mitochondrial orf peptide)', 'peptide_database_raw', 0.7),
    ('f61022cc1a4bfefb078c68886f1426fbbe07828d099a4b2bc5a4dcf30cab133f', 'collagen tripeptide ghk-cu', 'peptide_database_raw', 0.5),
    ('70827ec311962ebcce33d7915c1dbf2082fec570d3e280d4f06e8b81e07d62fa', 'kpv (lys-pro-val)', 'peptide_database_raw', 0.65),
    ('8d2bcf52c687b44735c0392dd817135389cd4f53d4d50488a02eaeb848fccb6c', 'kisspeptin-54 / kisspeptin-10', 'peptide_database_raw', 0.7),
    ('abd135f778302b2d91c4db40054b9d0a76c4649a027f89a163b74ae611e7ecff', 'aod-9604', 'peptide_database_raw', 0.6),
    ('f47ca14cd23c27fcc43f74e7911b8f065b37d76f5d8ef35c422793842ecd1d4c', 'melanotan ii', 'peptide_database_raw', 0.45),
    ('1b5ec216d421184b10c4f341f8bfa3cb2ea327dfe7e4e2ef7d9b31b6dc7c4fc1', 'bremelanotide', 'peptide_database_raw', 0.65),
    ('6263c8c744d8926eefdbe6cd0bb13c33588624c9d32041ab27e13748cf77e3b6', 'semaglutide', 'peptide_database_raw', 0.5),
    ('1cf21fcb7ecce355bf843c47c431178ff36dcf549ea00fc501b4b94cb0f5115e', 'tirzepatide', 'peptide_database_raw', 0.45),
    ('162d60b60e81c001984543090a0abddeba3a21f3504e205a5f1d8faa0f6664bc', 'tesamorelin', 'peptide_database_raw', 0.75),
    ('86e32ba420277c2d22cf0dfc81a736b2f418f9a4092610970fec00ddd06d41a5', 'teriparatide', 'peptide_database_raw', 0.7),
    ('ffd3b333377238a493e199d321554a930c953325df11fdaacbb26d8a83585380', 'exenatide', 'peptide_database_raw', 0.6),
    ('7ed9e6ae423f80b390c030b592e2f74365cee3e9cbda79c5da6ae5694615fed7', 'll-37 (cathelicidin)', 'peptide_database_raw', 0.65),
    ('08fe299c3d71c8ca3da454262119961603e62ecc6bb1805507375444c2dbc8c2', 'aviptadil', 'peptide_database_raw', 0.6),
    ('bb033597e4871a63951726b8a5df082f901c154e249f138996bbd873853916f8', 'pentagastrin', 'manual', 0.85),
    ('1cee77648a25946bd7bc543ab000d9c16b64110bb581e112656e018316a53857', 'sincalide', 'manual', 0.85),
    ('0d680028a80e049191ecd2001e970c5624aea38ecb1899b98ba6f1b2433e5fc6', 'secretin (human)', 'peptide_database_raw', 0.85)
) AS src(peptide_id, canonical_name, seed_source, strategic_fit_score)
ON target.peptide_id = src.peptide_id
WHEN MATCHED THEN UPDATE SET
  target.canonical_name      = src.canonical_name,
  target.seed_source         = src.seed_source,
  target.strategic_fit_score = src.strategic_fit_score,
  target.last_updated        = current_timestamp()
WHEN NOT MATCHED THEN INSERT (
  peptide_id, canonical_name, seed_source, strategic_fit_score,
  watchlist_active, last_updated
) VALUES (
  src.peptide_id, src.canonical_name, src.seed_source, src.strategic_fit_score,
  TRUE, current_timestamp()
);
