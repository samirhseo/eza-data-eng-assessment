# Readme

To start, clone the repo into an IDE that supports docker (I've used pycharm) and enter this into your terminal:

>docker-compose up

When the docker-compose is completed 

(this can take 10-15mins. Comments on how I would have done this differently under different circumstances can be found in main.py)

leave the container running & open a separate terminal then enter: 

>docker exec -it eza-data-eng-assessment_mysql_1 bash -l

this will open a bash instance. then use the following command to access the mysql server:

>mysql -uroot -pgroot
> 
You'll then be able to query the data.

here's a few example queries:

>SHOW TABLES

This will give you a list of tables in the database, pick a database and enter this command:

>SELECT COUNT(*) FROM 'YOUR_SELECTED_DATABASE'

(be sure to remove the '' quotes)


