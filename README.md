# SparkEtlSbt
ETL process between source and destination tables using Spark (in Scala)

This SBT project is used to read table data from one database and write to another using Spark JDBC.

The arguments required to run the spark job are as follows:
1) Location of JDBC .properties file:
The .properties file should contain the following properties:
source.database.driver:             Driver name for source database
source.database.url:                JDBC url for source database
source.database.username:           Username for source database authentication
source.database.password:           Password for source database authentication
destination.database.driver:        Driver name for destination database
destination.database.url:           JDBC url for destination database
destination.database.username:      Username for destination database authentication
destination.database.password:      Password for destination database authentication

Example (SQL Server to MySQL):
source.database.driver=com.microsoft.sqlserver.jdbc.SQLServerDriver
source.database.url=jdbc:sqlserver://localhost:1433;database=MySampleDB
source.database.username=sa
source.database.password=p@ssw0rd
destination.database.driver=com.mysql.jdbc.Driver
destination.database.url=jdbc:mysql://localhost:3306/MySQLDB
destination.database.username=admin
destination.database.password=admin

2) Source database table name:
The name of the database table where the data is to be read from.
To read data from other than a single table, a SELECT query can be formulated within " "

3) Destination database table name:
The name of the database table where the data is to be written to.

Syntax:
spark-submit --master <master_url> --jars <comma_separated_jdbc_jars> <JAR_file_build> <properties_file> <source_table> <destination_table>

Example:
spark-submit --master localhost:7077 --jars mssql-jdbc-8.4.0.jre8.jar,mysql-connector-java-8.0.21.jar /path/to/spark-jdbc.properties source_table destination_table

spark-submit --master localhost:7077 --jars mssql-jdbc-8.4.0.jre8.jar,mysql-connector-java-8.0.21.jar /path/to/spark-jdbc.properties "select col1, col2, col3 from table_name where col2 = 'foo'" destination_table

Note:
This project was built on top of Spark 3.0.0 for Hadoop 2.7, with Scala version 2.12.11, Java 8 (u251) and sbt version 1.3.13.
This project uses 'FAIR' spark.scheduler.mode, which requires spark.executor.instances and spark.executor.cores to be configured before-hand.
This project does not configure the number of executors, assigned memory per executor, etc., assuming it will be done in the respective configuration files on the machine(s) the Spark job will be executed on.
This project does not support CSV files and other sources of data other than JDBC, yet.
