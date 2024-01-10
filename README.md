# mysql-crawl-dumper

Script for creating a partial dump of a MySQL database
A starting table is selected, and data retrieval conditions from it are specified
Then, the script collects, in the manner of a search engine, through foreign keys all related tables and data.
The result of the script's work will be a .sh file, which contains instructions for mysqldump. Running it will create a dump of all necessary tables and their data.
