# AWS-Data-Lakehouse
- Project completed as part of Udacity's Data Engineering with AWS Nanodegree Program
- Project delivered in July 2023
- Repository includes 11 files:
    * A `accelerometer_landing.sql` script
    * A `customer_landing.sql` script
    * A `step_trainer_landing.sql` script
    * A `Customer_Landing_to_Trusted.py` script
    * An `Accelerometer_Landing_to_Trusted.py` script
    * A `Customer_Trusted_to_Curated.py` script
    * A `Step_Trainer_Landing_to_Curated.py` script
    * A `Machine_Learning_Curated.py` script
    * .jpg screenshots of the customer_landing, accelerometer_landing, and customer_trusted tables in Athena

## Problem Statement
The STEDI Team has been hard at work developing a hardware STEDI Step Trainer that:
- trains the user to do a STEDI balance exercise
- has sensors on the device that collect data to train a machine-learning algorithm to detect steps
- has a companion mobile app that collects customer data and interacts with the device sensors

STEDI has heard from millions of early adopters who are willing to purchase the STEDI Step Trainers and use them.

Several customers have already received their Step Trainers, installed the mobile application, and begun using them together to test their balance. The Step Trainer is just a motion sensor that records the distance of the object detected. The app uses a mobile phone accelerometer to detect motion in the X, Y, and Z directions.

The STEDI team wants to use the motion sensor data to train a machine learning model to detect steps accurately in real-time. Privacy will be a primary consideration in deciding what data can be used.

Some of the early adopters have agreed to share their data for research purposes. Only these customers’ Step Trainer and accelerometer data should be used in the training data for the machine learning model.

### The Prompt
As a data engineer on the STEDI Step Trainer team, extract the data produced by the STEDI Step Trainer sensors and the mobile app, and curate them into a data lakehouse solution on AWS. The intent is for the company's Data Scientists to use the solution to train machine learning models.

### The Device
There are sensors on the device that collect data to train a machine learning algorithm to detect steps. It also has a companion mobile app that collects customer data and interacts with the device sensors. The step trainer is just a motion sensor that records the distance of the object detected.

### The Data
1. Customer Records (from fulfillment and the STEDI website):
    * Location: AWS S3 Bucket URI - s3://cd0030bucket/customers/
    * Contains the following fields:
        - serialnumber
        - sharewithpublicasofdate
        - birthday
        - registrationdate
        - sharewithresearchasofdate
        - customername
        - email
        - lastupdatedate
        - phone
        - sharewithfriendsasofdate

2. Step Trainer Records (data from the motion sensor):
    * Location: AWS S3 Bucket URI - s3://cd0030bucket/step_trainer/
    * Contains the following fields:
        - sensorReadingTime
        - serialNumber
        - distanceFromObject

3. Accelerometer Records (from the mobile app):
    * Location: AWS S3 Bucket URI - s3://cd0030bucket/accelerometer/
    * Contains the following fields:
        - timeStamp
        - user
        - x
        - y
        - z

## Usage
### Prerequisites
- An S3 bucket to store data categorized into either the landing, trusted, or curated zone
- Landing zone S3 buckets to ingest raw customer, step trainer, and accelerometer JSON files
- IAM permissions for S3, Glue, and Athena
- Database specific for project's Glue tables, e.g. `project`

### Outline
The solution is built on AWS and uses the following services:
- S3 for data storage
- Glue for data processing
- Athena for querying data

My data lakehouse solution is comprised of five Python scripts which are run in AWS Glue. The scripts are run in the following order:
1. `Customer_Landing_to_Trusted.py`: This script transfers customer data from the 'landing' to 'trusted' zones. It filters for customers who have agreed to share data with researchers.
2. `Accelerometer_Landing_to_Trusted.py`: This script transfers accelerometer data from the 'landing' to 'trusted' zones. It filters for Accelerometer readings from customers who have agreed to share data with researchers.
3. `Customer_Trusted_to_Curated.py`: This script transfers customer data from the 'trusted' to 'curated' zones. It filters for customers with Accelerometer readings and have agreed to share data with researchers.
4. `Step_Trainer_Landing_to_Curated.py`: This script transfers step trainer data from the 'landing' to 'curated' zones. It filters for curated customers with Step Trainer readings.
5. `Machine_Learning_Curated.py`: This script combines Step Trainer and Accelerometer data from the 'curated' zone into a single table to train a machine learning model.

### Directions
#### To create Customer Landing Zone
1. a. Run `customer_landing.sql` script in Athena to create `customer_landing` table

#### To create Accelerometer Landing Zone
1. b. Run `accelerometer_landing.sql` script in Athena to create `accelerometer_landing` table

#### To create Step Trainer Landing Zone
1. c. Run `step_trainer_landing.sql` script in Athena to create `step_trainer_landing` table

#### To create Customer Trusted Zone
2. Run `Customer_Landing_to_Trusted.py` script in Glue to create `customer_trusted` table

#### To create Accelerometer Trusted Zone
3. Run `Accelerometer_Landing_to_Trusted.py` script in Glue to create `accelerometer_trusted` table

#### To create Customer Curated Zone
4. Run `Customer_Trusted_to_Curated.py` script in Glue to create `customer_curated` table

#### To create Step Trainer Curated Zone
5. Run `Step_Trainer_Landing_to_Curated.py` script in Glue to create `step_trainer_curated` table

#### To create Machine Learning Curated Zone
6. Run `Machine_Learning_Curated.py` script in Glue to create `machine_learning_curated` table

## Solution

### Technical Discussion
In a data lake architecture, the use of landing, trusted, and curated zones serves specific purposes that can significantly enhance the quality, reliability, and usability.

- Landing Zone: The landing zone is often the first point of contact for raw data as it enters the data lake. It serves as a staging area where data from various sources is collected, often in its original format. This zone provides a place to accumulate data before any substantial processing occurs. This allows for flexibility, as the original raw data remains intact and available for different types of analysis or processing in the future.

- Trusted Zone: After data has been landed, it may then be processed and moved to the trusted zone. In this zone, data is cleansed, validated, and often transformed into a structured format. This can include operations like deduplication, handling missing or incorrect data, and ensuring our customers have approved their data to be used for research purposes. The trusted zone is designed to be a source of reliable data for further analysis.

- Curated Zone: The curated zone is where data is further transformed, often to meet the specific needs of a particular analysis, application, or group of users. This may involve operations like aggregating data, creating derived metrics, or combining multiple datasets. The curated zone should provide data that's ready-to-use for your specific data-driven applications and analyses.

In essence, these three zones facilitate a layered approach to data management and preparation. Each stage adds value to the data, making it increasingly reliable and useful for our specific needs. This strategy also aids in maintaining data quality, tracking data lineage, and enabling efficient and versatile data exploration and analysis.

### Business Discussion
Our data lakehouse solution is designed to give STEDI a robust and flexible data infrastructure that allows us to store, clean, and transform vast amounts of data.

Firstly, my solution provides scalability and cost efficiency by leveraging Amazon S3 for storage, enabling us to store large amounts of diverse data cost-effectively. We can scale our storage up or down based on our needs, and we only pay for what we use.

Secondly, by using AWS Glue, a fully managed extract, transform, and load (ETL) service, we are able to clean, normalize, and relocate our data. This step is crucial for preparing our data for high-quality analytics and machine learning.

The structured data is then ready for downstream use by our data scientists for exploratory data analysis or to train machine learning models.

Moreover, my solution provides a single source of truth for our data, improving data quality and consistency. This is particularly beneficial when dealing with complex datasets as it simplifies the process of data management and increases efficiency.

Overall, my data lakehouse solution gives us the power to make data-driven decisions, enhancing STEDI's competitive advantage in the market, improving our products, and delivering a superior customer experience.
