# Crawl-dumper collects, in the manner of a search engine, through foreign keys all related tables and data

Script for creating a partial dump of a MySQL database<br>
Set table name and data retrieval conditions from it.<br><br>
Crawl-dumper collects, in the manner of a search engine, through foreign keys all related tables and data. <br>
The result of the script's work will be a .sh file, which contains instructions for mysqldump. <br>
Running it will create a dump of all necessary tables and their data.<br><br>

Thanks to working through mysqldump, it does not take up much memory space while running.
