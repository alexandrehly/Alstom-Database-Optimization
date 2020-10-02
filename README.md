# Alstom-Database-Optimization

Work done for my internship at Alstom in 2020.

The objective was to code the queries several queries :
- Reconstruct a message by fetching its individual fields in the database
- Fetch a set number of fields
- Estbalish the update history of a set number of fields
The difficulty lies in the fact that only the updates of the fields are kept in memory.

There are 3 steps in the development:
1. First proof of concept connecting Spark and Cassandra and coded in Scala
2. Changed the language from Scala to Java to better integrate with the existing software coded in Java and facilitate support for the company.
3. Setting a proper was not an option for the company so I changed the data scheme which allowed me to process the data in reasonable times without distributed methods and I then rewrote the existing SQL queries to  fit the new architecture.

I have separated each part in a folder.
