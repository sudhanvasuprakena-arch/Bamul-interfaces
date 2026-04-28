-- =============================================================================
-- BAMUL OM to AR Interface - AR Setup (Transaction Types & Batch Source)
-- Run on: TARGET (New EBS) instance
-- NOTE: These should ideally be created via the EBS UI (Receivables Manager),
--       but the SQL below documents what needs to be set up.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 1. Create Batch Source: BAMUL_OM_IMPORT
-- Navigation: Receivables > Setup > Transactions > Sources
-- -----------------------------------------------------------------------------
-- Name:           BAMUL_OM_IMPORT
-- Type:           Imported
-- Auto Trx Numbering: Yes
-- Last Number:    0
-- Description:    BAMUL OM to AR Interface Import Source

-- -----------------------------------------------------------------------------
-- 2. Create AR Transaction Types
-- Navigation: Receivables > Setup > Transactions > Transaction Types
-- -----------------------------------------------------------------------------

-- Transaction Type 1: Bamul Route Sales
-- Name:               Bamul Route Sales
-- Class:              Invoice (INV)
-- Open Receivable:    Yes
-- Post to GL:         Yes
-- Allow Freight:      No
-- Creation Sign:      Positive
-- Natural Application Only: No

-- Transaction Type 2: Bamul P&I Sales
-- Name:               Bamul P&I Sales
-- Class:              Invoice (INV)
-- Open Receivable:    Yes
-- Post to GL:         Yes
-- Allow Freight:      No
-- Creation Sign:      Positive
-- Natural Application Only: No

-- -----------------------------------------------------------------------------
-- 3. Verify Setup
-- Run these queries after manual setup to confirm:
-- -----------------------------------------------------------------------------

-- Verify Batch Source
SELECT batch_source_id, name, status, auto_trx_numbering_flag
FROM ar.ra_batch_sources_all
WHERE name = 'BAMUL_OM_IMPORT';

-- Verify Transaction Types
SELECT cust_trx_type_id, name, type, post_to_gl, open_receivable
FROM ar.ra_cust_trx_types_all
WHERE name IN ('Bamul Route Sales', 'Bamul P&I Sales');
