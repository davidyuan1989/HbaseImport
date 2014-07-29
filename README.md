HbaseImport
===========

ETL Bulk Load for Hbase

bulk approach to import data, which load all the files in the same folder into one table to avoid write “hot region” issue. 
Then parse the data based on table and store them into different folders, easy to load large files.

