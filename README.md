# Readme

To start, clone the repo into an IDE that supports docker (I've used pycharm) and enter this into your terminal:

>docker-compose up


**This can take 10-15mins when building the container for the first time.
Comments on how I would have done this differently to make this much quicker & user friendly can be found in main.py**

When docker-compose is completed leave the container running, open a separate terminal then enter: 

>docker exec -it eza-data-eng-assessment_mysql_1 bash -l

This will open a bash instance. then use the following command to access the mysql server:

>mysql -uroot -pgroot
> 
You'll then be able to query the data.

Here's a few example queries:

>SHOW TABLES

This will give you a list of tables in the database, pick a database and enter this command:

>SELECT COUNT(*) FROM 'YOUR_SELECTED_DATABASE'

(be sure to remove the '' quotes)

##Table Structure & function

This script will explode the json files in /data/ and write them via dataframe directly into a mysql database.

Many to Many relationship on 'resource_id' field. The tables have been seperated by 'resourceTYPE' so each resource type table can be quickly queried.


