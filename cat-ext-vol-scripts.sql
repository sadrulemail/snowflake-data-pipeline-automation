create database demo_db;

create schema if not exists common_sch;


CREATE OR REPLACE CATALOG INTEGRATION glueCatalogInt
CATALOG_SOURCE=GLUE
TABLE_FORMAT=ICEBERG
GLUE_AWS_ROLE_ARN='arn:aws:iam::054973743628:role/glueCatalogInt_Role'
GLUE_CATALOG_ID='054973743628'
GLUE_REGION='us-east-1'
CATALOG_NAMESPACE = 'iceberg_demo_db'
ENABLED=TRUE;


DESCRIBE CATALOG INTEGRATION glueCatalogInt;

-- arn:aws:iam::445985103706:user/btv71000-s
-- SP99341_SFCRole=3_I1q/Wc7LR40B0KGAtumCwJeT1r8=

CREATE OR REPLACE EXTERNAL VOLUME glueCatalog_extvol
STORAGE_LOCATIONS =
(
(
NAME = 'glueCatalog_extvol'
STORAGE_PROVIDER = 'S3'
STORAGE_BASE_URL = 's3://iceberg-s3-bucket-demo-3486386/tables-data/'
STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::054973743628:role/gluecatalog_extvol_role'
)
);

DESC EXTERNAL VOLUME glueCatalog_extvol;

-- arn:aws:iam::445985103706:user/btv71000-s
-- SP99341_SFCRole=3_M40m/a1pfkDvxKW3yY1H3byTaSQ=

