# BigQuery support for EDC

### Objective
Support exchange of data to and from BigQuery tables.

### Extensions
* BigQueryDataAddressValidatorExtension (common): registers two validators, one for source data address and one for destination
* BigQueryProvisionExtension (control plane): registers the provisioner for BigQuery. Currently only checks that destination table exists and provides access token to the sink in the data plane
* DataPlaneBigQueryExtension (data plane): registers source and sink factories, data exchanged is formatted as JSON

### Data Source
Source data address is defined by a query statement. The source data address is taken from the asset requested from the catalog and includes the query as parameter. Named parameters are supported.
Factory creates the source which executes the query on the provider side using parameters found in the DataFlowStartMessage (found in the sink address)
Two solutions:
1. Synchronous (current implementation): fetch all rows, prepare a single part with the data obtained formatted as JSON and then return the part 
2. Asynchronous (soon to replace synchronous): prepare and return a single part with a piped input stream connected to piped output stream used in another (source) thread: in this thread, paginated results (e.g. 10 rows at a time)  of the query are fetched and, each page is serialized to the stream in JSON format 

### Data Sink
Destination data address defined by dataset and destination table. 
Factory creates the sink which receives the JSON data from the source. Using the access token created by the provisioner the sink stores the data using the Storage Write API. In case data transfer fails, currently there is no retry, data is then not committed (i.e. treated as a transaction)

### Future Development
- Support table name as source instead of query
- Support table creation in the provisioner
- Support DTS for BigQuery to BigQuery
