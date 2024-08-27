CREATE DATABASE SP500_DB;

USE SP500_DB;

DROP TABLE Companies;

CREATE TABLE Companies (
	Date varchar(255),
	Symbol varchar(255),
	ClosePrice float
	);

DROP TABLE CompanyProfiles;

CREATE TABLE CompanyProfiles (
	Symbol varchar(255),
	Company varchar(255),
	Sector varchar(255),
	Headquarters varchar(255),
	Established varchar(255)
	);

SELECT * FROM Companies;

SELECT * FROM CompanyProfiles;

TRUNCATE TABLE CompanyProfiles;

ALTER TABLE CompanyProfiles
ALTER COLUMN Symbol VARCHAR(255) NOT NULL

ALTER TABLE Companies
DROP COLUMN Sector;

ALTER TABLE Companies
ALTER COLUMN Date VARCHAR(255) NOT NULL

ALTER TABLE Companies
ALTER COLUMN Symbol VARCHAR(255) NOT NULL

ALTER TABLE Companies
ALTER COLUMN ClosePrice VARCHAR(255) NOT NULL

