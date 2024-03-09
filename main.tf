terraform {
  required_providers {
    minio = {
      source = "aminueza/minio"
      version = "2.0.1"
    }
  }
}

locals {
  envs = { for tuple in regexall("(.*)=(.*)", file(".env")) : tuple[0] => sensitive(tuple[1]) }
}

provider "minio" {
  minio_server = "localhost:9000"
  minio_user = local.envs["MINIO_ROOT_USER"]
  minio_password = local.envs["MINIO_ROOT_PASSWORD"]
}

resource "minio_s3_bucket" "artifacts_bucket" {
  bucket = "datalake-artifacts"
  acl    = "public"
}

resource "minio_s3_bucket" "landing_bucket" {
  bucket = "datalake-landing"
  acl    = "public"
}

resource "minio_s3_bucket" "raw_bucket" {
  bucket = "datalake-raw"
  acl    = "public"
}

resource "minio_s3_bucket" "trusted_bucket" {
  bucket = "datalake-trusted"
  acl    = "public"
}

resource "minio_s3_bucket" "refined_bucket" {
  bucket = "datalake-refined"
  acl    = "public"
}

resource "minio_s3_object" "ipca2023_sample_file" {
  depends_on = [minio_s3_bucket.landing_bucket]
  bucket_name = minio_s3_bucket.landing_bucket.bucket
  object_name = "ipca/ipca2023.tsv"
  content = file("samples/ipca2023.tsv")
  content_type = "text/plain"
}
