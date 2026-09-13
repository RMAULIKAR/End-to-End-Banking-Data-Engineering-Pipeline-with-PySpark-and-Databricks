CREATE OR REPLACE VIEW bankingprojectpyspark_cata.man_schema.vw_customer_kpis AS
SELECT *
FROM delta.`/Volumes/bankingprojectpyspark_cata/man_schema/project_storage/gold/customer_kpis`;


CREATE OR REPLACE VIEW bankingprojectpyspark_cata.man_schema.vw_account_kpis AS
SELECT *
FROM delta.`/Volumes/bankingprojectpyspark_cata/man_schema/project_storage/gold/account_kpis`;


CREATE OR REPLACE VIEW bankingprojectpyspark_cata.man_schema.vw_monthly_transaction_kpis AS
SELECT *
FROM delta.`/Volumes/bankingprojectpyspark_cata/man_schema/project_storage/gold/monthly_transaction_kpis`;