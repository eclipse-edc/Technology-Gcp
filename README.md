# Technology GCP

This repository contains GCP-specific implementations for several SPIs of
the [Eclipse Dataspace Components Connector](https://github.com/eclipse-edc/Connector)

## Documentation

Developer documentation can be found under [docs/developer](docs/developer/), where the main concepts and decisions are
captured as [decision records](docs/developer/decision-records/).
## Configure and usage of GCP Extensions
### Authentication
Google storage data plane and provision support two different approaches for authentication:
* Default authentication:
    * Authenticates against the Google Cloud API using the [Application Default Credentials](https://cloud.google.com/docs/authentication#adc).
    * These will automatically be provided if the connector is deployed with the correct service account attached.
* Service Account key file
    * Authentication using a Service Account key file exported from Google Cloud Platform
    * Service Account key file can be stored in a vault or encoded as base64 and provided in the dataAddress
  
### Enabled APIs and Services
To have the full support of GCP extension, it is important to enable `Identity and Access Management (IAM) API` for each project.

1. Click on Navigation Menu
2. Select `Enabled APIs and Services` under APIs and Services
3. Click on `+ ENABLE APIS AND SERVICES` (top center)
4. Search for `Identity and Access Management (IAM) API` and Enable it

### Service account setup
#### GCP Default authentication Setup

For the provisioning to work we will need to run it with a service account with the correct permissions. The permissions are:

- creating buckets
- creating service accounts
- setting permissions
- creating access tokens

##### Set project variable

```
PROJECT_ID={PROJECT_ID}
```

##### Create service account that will be used when interacting with the Google Cloud API.

```
gcloud iam service-accounts create dataspace-connector \
    --description="Service account used for running eclipse dataspace connector" \
    --display-name="EDC service account"
```

##### Assign required roles to service account by choosing in the filter by Google CLI or by Google Console (see below)


```
# Service Account Admin

gcloud projects add-iam-policy-binding $PROJECT_ID \
--member="serviceAccount:dataspace-connector@$PROJECT_ID.iam.gserviceaccount.com" \
--role="roles/iam.serviceAccountAdmin"

# Storage Admin

gcloud projects add-iam-policy-binding $PROJECT_ID \
--member="serviceAccount:dataspace-connector@$PROJECT_ID.iam.gserviceaccount.com" \
--role="roles/storage.admin"

# Service Account Token Creator

gcloud projects add-iam-policy-binding $PROJECT_ID \
--member="serviceAccount:dataspace-connector@$PROJECT_ID.iam.gserviceaccount.com" \
--role="roles/iam.serviceAccountTokenCreator"
```

#### 2. By Google Console
* Select `IAM and Admin` under Navigation Menu
* Select `Service accounts`
* click on `+ CREATE SERVICE ACCOUNT`
* Give any random account name and click `CREATE AND CONTINUE`
* Add Roles
    1. Service Account Admin
    2. Storage Admin
    3. Service Account Token Creator
* Click on Done
* Select the created Service account and go to `KEYS`
* Select `ADD KEY` and choose JSON format
* The JSON could be saved into your vault or encode in base64 to be further used in the data address as `service_account_value` : `YOUR_ENCODED_JSON`

## Configuration

| Key                      | Description                                                                                                       | Mandatory |
|:-------------------------|:------------------------------------------------------------------------------------------------------------------|-----------|
| edc.gcp.project.id       | ID of the GCP projcet to be used for bucket and token creation. This can be difined in the DataAddress properties |  |

## DataAddress properties

| Key                      | Description                                                                                                                                                                      | Mandatory as source| Mandatory as destination without provision | Mandatory as destination with provision |
|:-------------------------|:---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-------|--------------------------------------------|--------|
| type                     | GoogleCloudStorage                                                                                                                                                               | X | X                                          |X| 
| project_id               | ID of the GCP projcet to be used for bucket and toekn creation. If the project_id has not been defined here, the projectId in the connector configurations will be used instead! |  |                                            |X|
| storage_class            | STANDARD/ NEARLINE/ COLDLINE/ ARCHIVE / [More info](https://cloud.google.com/storage/docs/storage-classes)                                                                       |  |                                            |X|
| location                 | [Available regions](https://cloud.google.com/storage/docs/locations#location-r)                                                                                                  | X |                                            |  |
| service_account_key_name | It should reference a vault entry containing a [Service Account Key File](https://cloud.google.com/iam/docs/creating-managing-service-account-keys#creating) in json.            |  |                                            |  |
| service_account_value    | It should contain a [Service Account Key File](https://cloud.google.com/iam/docs/creating-managing-service-account-keys#creating) in json encoded with base64                    |  |                                            |  |
| bucket_name              | It should contain the name of the bucket where an Asset is located to be transferred                                                                                             |X| X                                          |  |
| blob_name                | It should contains the name of file. For an example `test.json`                                                                                                                  |X| X                                          |  |



## Contributing

See [how to contribute](https://github.com/eclipse-edc/Connector/blob/main/CONTRIBUTING.md) for details.