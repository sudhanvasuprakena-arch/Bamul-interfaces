/* ============================================================================
   FILE:         02_Populate_COGS_Details.sql
   INTERFACE:    IF-02: OM to GL (COGS Journal Creation)
   VERSION:      1.0 PRODUCTION
   
   DESCRIPTION:
   ------------
   Populates XX_COGS_DETAILS table with product costing data.
   Based on "Costing Details (1).xlsx" provided by user.
   
   Material Account: 121001 (Inventory)
   COGS Account: 511002 (Cost of Goods Sold)
   
   ============================================================================ */

-- Clear existing data (if re-running)
DELETE FROM apps.xx_cogs_details;
COMMIT;

-- ============================================================================
-- Toned Milk Products
-- ============================================================================
INSERT INTO apps.xx_cogs_details (product_code, description, cost, unit_of_measurement, material_account, cogs_account)
VALUES ('MITK0200', 'Toned Milk 200 ml', 45.61, 'PKT', '121001', '511002');

INSERT INTO apps.xx_cogs_details (product_code, description, cost, unit_of_measurement, material_account, cogs_account)
VALUES ('MITK0500', 'Toned Milk 500 ml', 45.10, 'PKT', '121001', '511002');

INSERT INTO apps.xx_cogs_details (product_code, description, cost, unit_of_measurement, material_account, cogs_account)
VALUES ('MITK1000', 'Toned Milk 1000 ml', 44.95, 'PKT', '121001', '511002');

INSERT INTO apps.xx_cogs_details (product_code, description, cost, unit_of_measurement, material_account, cogs_account)
VALUES ('MITK6000', 'Toned Milk 6000 ml', 44.98, 'PKT', '121001', '511002');

-- ============================================================================
-- Shubham Milk Products
-- ============================================================================
INSERT INTO apps.xx_cogs_details (product_code, description, cost, unit_of_measurement, material_account, cogs_account)
VALUES ('MISH0200', 'Shubham Milk 200 ml', 48.51, 'PKT', '121001', '511002');


INSERT INTO apps.xx_cogs_details (product_code, description, cost, unit_of_measurement, material_account, cogs_account)
VALUES ('MISH1000', 'Shubham Milk 1000 ml', 49.22, 'PKT', '121001', '511002');

INSERT INTO apps.xx_cogs_details (product_code, description, cost, unit_of_measurement, material_account, cogs_account)
VALUES ('MISH1000A', 'Shubham Milk 1000 ml to Army', 48.51, 'PKT', '121001', '511002');

-- ============================================================================
-- Nandini Special Milk Products
-- ============================================================================
INSERT INTO apps.xx_cogs_details (product_code, description, cost, unit_of_measurement, material_account, cogs_account)
VALUES ('MISP0200', 'Nandini Special Milk 200 ml', 49.22, 'PKT', '121001', '511002');

INSERT INTO apps.xx_cogs_details (product_code, description, cost, unit_of_measurement, material_account, cogs_account)
VALUES ('MISP0500', 'Nandini Special Milk 500 ml', 48.70, 'PKT', '121001', '511002');

INSERT INTO apps.xx_cogs_details (product_code, description, cost, unit_of_measurement, material_account, cogs_account)
VALUES ('MISP1000', 'Nandini Special Milk 1000 ml', 48.54, 'PKT', '121001', '511002');

-- ============================================================================
-- HCM Milk Products
-- ============================================================================
INSERT INTO apps.xx_cogs_details (product_code, description, cost, unit_of_measurement, material_account, cogs_account)
VALUES ('MIHC0500', 'HCM Milk 500 ml', 46.29, 'PKT', '121001', '511002');

-- ============================================================================
-- Samrudhi Milk Products
-- ============================================================================
INSERT INTO apps.xx_cogs_details (product_code, description, cost, unit_of_measurement, material_account, cogs_account)
VALUES ('MISA0500', 'Samrudhi Milk 500 ml', 51.93, 'PKT', '121001', '511002');

-- ============================================================================
-- Desi Milk Products
-- ============================================================================
INSERT INTO apps.xx_cogs_details (product_code, description, cost, unit_of_measurement, material_account, cogs_account)
VALUES ('MIDE0500', 'Desi Milk 500 ml', 69.62, 'PKT', '121001', '511002');

INSERT INTO apps.xx_cogs_details (product_code, description, cost, unit_of_measurement, material_account, cogs_account)
VALUES ('MIDE0500I', 'Desi Milk 500 ml Inter Union Sale', 69.62, 'PKT', '121001', '511002');

COMMIT;

-- ============================================================================
-- Verification Query
-- ============================================================================

--  XX_COGS_DETAILS Population Complete


SELECT 
    COUNT(*) as total_products,
    COUNT(CASE WHEN cost IS NOT NULL THEN 1 END) as products_with_cost,
    COUNT(CASE WHEN cost IS NULL THEN 1 END) as products_missing_cost,
    MIN(cost) as min_cost,
    MAX(cost) as max_cost,
    AVG(cost) as avg_cost
FROM apps.xx_cogs_details;



SELECT 
    product_code,
    description,
    TO_CHAR(cost, '999.99') as unit_cost,
    unit_of_measurement as uom,
    material_account,
    cogs_account,
    enabled_flag
FROM apps.xx_cogs_details
ORDER BY product_code;

