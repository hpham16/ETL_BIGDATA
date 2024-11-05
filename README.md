# ETL_BIGDATA
This is my project, where I process customer data, specifically log_search and log_content. The goal of the project is to transform complex data into simpler data, enabling the Data Analysis (DA) teams to easily conduct analyses and generate reports. The data will be stored in a PostgreSQL database using Apache Spark

![image](https://github.com/user-attachments/assets/c5c2368f-2c97-4dd8-9f60-a1302ac582ed)

1. Processing content_data.
  1.1. Treatment direction
   First, I will read the data from the CSV file and remove any unnecessary information. Then, I will classify the applications into categories based on AppName, which includes durations such as Television, Movies, Entertainment, Children, and Sports. Next, I will pivot the data according to these durations to track customer preferences. This will allow me to analyze what customers have watched, which genres they prefer the most, and to compile statistics for each customer. Finally, I will aggregate this data and store it in the database in the content_data table.
   1.2. Data
    a. Before process.
   ![image](https://github.com/user-attachments/assets/2578daca-6c82-458c-933d-441d07488f91)
   ![image](https://github.com/user-attachments/assets/4d6f95d8-4f0b-4dd3-a1fa-e7fe65944931)
    b. After process.
   ![image](https://github.com/user-attachments/assets/72b9f50e-3f6b-4c4a-bd64-48c9c0d081cd)
2. Processing search_data.
   2.1. Treatment direction
   To analyze this data, I will start by counting the number of searches for each title by customers. The goal is to identify which content is of the most interest to customers using the partition method. Next, I will label the titles and execute this process for the months of June and July. Finally, I will aggregate the data into a DataFrame and save the results.
   2.2. Data

