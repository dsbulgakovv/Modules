This module was written for specific tasks and is not applicable for huge datasets.

It is good at combining working processes in Jupyter Notebook with pandas dataframes and merging it with data
located on the MS SQL Server in your organization. You can use it to have fast access for the data without additional
.csv files and huge filters in the SQL queries in other apps.

To work with this module you will need Microsoft SQL Server driver:
https://learn.microsoft.com/ru-ru/sql/connect/odbc/download-odbc-driver-for-sql-server?view=sql-server-ver16

Also, it is built in the way to verify your identity through the Microsoft Windows authentication "Trusted_Connection".
To change the authentication method you should change the db_connect() function.

If you see any details to be improved, please, contact me.
Let us make it better together!