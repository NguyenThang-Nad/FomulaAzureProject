# Azure DataBricks Formula Data
## Problem Statement
Designing and implementing a Data Pipeline using Azure DataBricks (ADB) to analyze and visualize the results from the Formula 1 race.
## Dataset
In this project,we will use the Formula One dataset from:
http://ergast.com/mrd/
The data source is available through API/JSON/XML format or dowloaded CSV files.ER diagram of source data here:
![image](https://github.com/NguyenThang-Nad/FomulaAzureProject/assets/136436998/9507e394-1738-4029-ac85-4f54733a2142)
# Solution Archirecture
![image](https://github.com/NguyenThang-Nad/FomulaAzureProject/assets/136436998/71812f92-2b36-40d8-9615-ba3d88cc10a0)

Architecture for this project is below: raw data will be read from Ergast via API method using Azure Data Factory and imported into ADLS Raw containers in csv format. Then Databricks will be used to perform data ingestion to ADLS Ingestion Layer, and transformation to the ADLS Presentation layer. Later data will be analyzed and visualized through Databricks and Power BI. Azure Data Factory will be an orchestration tool to monitor and schedule the pipeline. On the Databricks website, 3 different layers processed through DataBricks notebooks are described as Bronze (where raw data is loaded), Silver (where data is filtered and cleaned), and Gold (where data is transformed through business logic).
## Set up
Create Azure storage and databricks workspace.Enable ADLS Gen 2 and create 3 containers as in the picture,coresponding to 3 layer of the Databricks data pipeline model:Bronze,Silve and Gold.
![image](https://github.com/NguyenThang-Nad/FomulaAzureProject/assets/136436998/cf244e48-2a6a-4f85-b826-46ef01ca82a9)

Create a DataBricks service on Azure and a standard cluster,and a notebook to setup credentials to Azure datalake.Next grant access for DataBricks to Azure DataLake using SAS token.Then mount DataBricks DBFS to Azure Data Lake.


![image](https://github.com/NguyenThang-Nad/FomulaAzureProject/assets/136436998/8d0ebe1d-bb6f-4ec6-89f4-509bdfc24327)

## Data ingestion
After mount Databricks to ADLS and loading source data to the raw container.Transformations such as chageing column name to makes them moew meaningful for data consumers.Drop column URL because we wont use it,also add the ingestion_date column with the current timestamp to track the data loading time.
![image](https://github.com/NguyenThang-Nad/FomulaAzureProject/assets/136436998/84c39624-9332-48d0-aba6-97c22965c42b)

The final step is to load tranformed table to Processed foder as Parquet format.
![image](https://github.com/NguyenThang-Nad/FomulaAzureProject/assets/136436998/f9a7784a-2a5c-43d1-a4c6-98f14ada44e0)
![image](https://github.com/NguyenThang-Nad/FomulaAzureProject/assets/136436998/6023fb45-cb0b-4e3d-bab5-015e56202838)
## Data tranformation for Presentation Layer
in this project,business partner want to have the information of the 2023 year race.The information should contain the results,drivers and team.So what i'm going to do next is to extract relevant tables and columns and join them into reace_result table:
![image](https://github.com/NguyenThang-Nad/FomulaAzureProject/assets/136436998/84820c77-945f-42f1-a939-4d02f2780e69)
Do another tranformation required by business partners is to extract Drivers and Contructors standing/rankings.
## Create Data Pipelines in Azure Data Factory to orchestrate Databricks Notebooks
Create if to check raw container conutains any files,if yes,we trigger Databricks notebooks.In order to do that,we can create 2 other activity,create a Linked service to Azure Data Lake Storage and set container as raw.
![image](https://github.com/NguyenThang-Nad/FomulaAzureProject/assets/136436998/f644ac3c-6edd-4a3c-bbf8-0715ce95abb9)
Create 3 Databricks activities to execute 3 tranformation notebook.
![image](https://github.com/NguyenThang-Nad/FomulaAzureProject/assets/136436998/1d369203-79e4-4ccd-acac-7e474e710974)
This pipeline also triggers the Ingestion Pipeline as the first step in the process.
## Data Analysis and Data Visualization
Create database and tables in Databricks stored in Hive Meta Store so they can visualize data in Power BI/Tableau/Looker.
![image](https://github.com/NguyenThang-Nad/FomulaAzureProject/assets/136436998/1a8294a7-54b5-438d-b839-59403ca15b9e)
## Data visualization 
Power BI:
Driver points in season 2023
![image](https://github.com/NguyenThang-Nad/FomulaAzureProject/assets/136436998/f10b11c4-273b-4038-939f-b7859d102ebf)
![image](https://github.com/NguyenThang-Nad/FomulaAzureProject/assets/136436998/f5290789-7ab1-4666-a13b-7fc1ef931832)

## Technologies Used
DataBricks
ADF
Python
PySpark
Power BI
Azure
