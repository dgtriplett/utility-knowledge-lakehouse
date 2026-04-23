# Row-level security pattern

Utilities typically partition access along region (transmission territory, distribution operating district) and department (protection, operations, planning). Unity Catalog's row filters let you declare that once on the table and have every downstream consumer — the Vector Search index, Genie, the agent — inherit it.

## Pattern

```sql
-- 1. A user→region mapping table, kept in sync with your identity provider.
CREATE TABLE IF NOT EXISTS utility_knowledge.security.user_regions (
    user_email STRING,
    region_code STRING
);

-- 2. A filter function.
CREATE OR REPLACE FUNCTION utility_knowledge.security.region_filter(region STRING)
RETURN region IN (
  SELECT region_code
  FROM utility_knowledge.security.user_regions
  WHERE user_email = current_user()
) OR is_account_group_member('utility-knowledge-admins');

-- 3. Apply it to every customer-facing table.
ALTER TABLE utility_knowledge.curated.documents
  SET ROW FILTER utility_knowledge.security.region_filter ON (region);

ALTER TABLE utility_knowledge.curated.document_chunks
  SET ROW FILTER utility_knowledge.security.region_filter ON (region);

ALTER TABLE utility_knowledge.curated.sme_debriefs
  SET ROW FILTER utility_knowledge.security.region_filter ON (region);
```

## Why this is the right layer

- The Vector Search delta-sync index inherits the row filter automatically.
- The agent, running as the querying user's identity via the Databricks App, only sees chunks that user is allowed to see.
- No application-level permission code. No duplicated rules across Genie, the app, and any future integrations.

## Propagating region through the pipeline

The current extraction schema doesn't populate a `region` column on chunks. Two options:

1. **Add region extraction to Layer 1.** Update the `ai_extract` field list in `02_extract_fields.py` to include a `region` field, then propagate it through consolidation and chunking.
2. **Join at consolidation time.** Keep a `substation_regions` table mapping substation names to regions, join it into `curated.documents` during Layer 2, carry through to chunks.

Option 2 is easier if you already have an asset registry.

## Column masks for sensitive fields

Protection settings, customer commercial relationships, or incident postmortems may warrant masking for most users:

```sql
CREATE OR REPLACE FUNCTION utility_knowledge.security.mask_sensitive(value STRING)
RETURN CASE
  WHEN is_account_group_member('protection-engineers') THEN value
  ELSE '[redacted — request access]'
END;

ALTER TABLE utility_knowledge.curated.document_fields
  ALTER COLUMN approving_engineer
  SET MASK utility_knowledge.security.mask_sensitive;
```
