# Readme

## Install

To start, clone the repo into an IDE that supports docker (I've used pycharm) and enter this into the terminal in your IDE Environment:

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

## Table Structure & function

This script will explode the json files in /data/ and write them via dataframe directly into a mysql database.

Many to Many relationship on 'resource_id' field. The tables have been seperated by 'resourceTYPE' so each resource type table can be quickly queried.

Table names:

- flattened_json_allergyintolerance_table

- flattened_json_careplan_table

- flattened_json_careteam_table

- flattened_json_claim_table

- flattened_json_condition_table

- flattened_json_device_table

- flattened_json_diagnosticreport_table

- flattened_json_documentreference_table

- flattened_json_encounter_table

- flattened_json_explanationofbenefit_table

- flattened_json_imagingstudy_table

- flattened_json_immunization_table

- flattened_json_medication_table

- flattened_json_medicationadministration_table

- flattened_json_medicationrequest_table

- flattened_json_observation_table

- flattened_json_patient_table

- flattened_json_procedure_table

- flattened_json_provenance_table

- flattened_json_supplydelivery_table




