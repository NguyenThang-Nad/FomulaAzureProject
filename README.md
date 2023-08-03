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

![image](https://github.com/NguyenThang-Nad/FomulaAzureProject/assets/136436998/f10b11c4-273b-4038-939f-b7859d102ebf)
![image](https://github.com/NguyenThang-Nad/FomulaAzureProject/assets/136436998/f5290789-7ab1-4666-a13b-7fc1ef931832)

