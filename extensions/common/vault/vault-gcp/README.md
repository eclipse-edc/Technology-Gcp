# GCP Secret Manager Vault

The vault-gcp extension is an implementation of the Vault interface based on GCP Secret Manager.
Arbitrary key names are possible through the key sanitation feature. 

## Settingss

- edc.vault.gcp.project : optional setting to specify the GCP project hosting the Secret Manager API. If not specified, the default project is used ( see ServiceOptions.getDefaultProjectId() )
- edc.vault.gcp.saccount_file : optional setting providing the path to a service account (JSON) file, to be used for credentials. If not specified, default credentials are used (not recommended)
- edc.vault.gcp.region : mandatory setting to specify secret replica location

## Decisions
- Secrets will not be overwritten if they exist to prevent potential leakage of credentials to third parties.
- Keys strings are sanitized to comply with key requirements of AWS Secrets Manager. Sanitizing replaces all illegal characters with '-' and appends the hash code of the original key to minimize the risk of key collision after the transformation, because the replacement operation is a many-to-one function. A warning will be logged if the key contains illegal characters.
