variable "project" {
  description = "Project ID"
  default     = "de-capstone-project"
}

variable "location" {
  description = "Project Location"
  default     = "US"
}

variable "region" {
  description = "Region"
  default     = "US"
}

variable "bq_dataset_name" {
  description = "My big query dataset name"
  default     = "raw"
}

variable "gcs_bucket_name" {
  description = "My storage bucket name"
  default     = "raw-bucket-911"
}

variable "gcs_storage_class" {
  description = "Bucket storage class"
  default     = "STANDARD"
}

variable "credentials" {
  description = "My credentials"
  default     = "~/.google/credentials/creds.json"
}
