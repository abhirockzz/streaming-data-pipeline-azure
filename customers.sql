CREATE SCHEMA Crm;
GO

CREATE TABLE Crm.Customers (
  cust_id       INT NOT NULL PRIMARY KEY,
  cust_name     NVARCHAR(30),
  city NVARCHAR(20)
);
GO


SELECT * FROM Crm.Customers;
GO