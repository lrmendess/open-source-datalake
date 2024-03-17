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
  bucket = local.envs["BUCKET_DATALAKE_ARTIFACTS"]
  acl    = "public"
}

resource "minio_s3_bucket" "landing_bucket" {
  bucket = local.envs["BUCKET_DATALAKE_LANDING"]
  acl    = "public"
}

resource "minio_s3_bucket" "raw_bucket" {
  bucket = local.envs["BUCKET_DATALAKE_RAW"]
  acl    = "public"
}

resource "minio_s3_bucket" "trusted_bucket" {
  bucket = local.envs["BUCKET_DATALAKE_TRUSTED"]
  acl    = "public"
}

resource "minio_s3_bucket" "refined_bucket" {
  bucket = local.envs["BUCKET_DATALAKE_REFINED"]
  acl    = "public"
}

resource "minio_s3_bucket" "spark_logs_bucket" {
  bucket = local.envs["BUCKET_SPARK_LOGS"]
  acl    = "public"
}

resource "minio_s3_object" "upload_ipca_samples" {
  for_each = fileset("${path.module}/minio/samples/ipca", "*.tsv")
  depends_on = [minio_s3_bucket.landing_bucket]
  bucket_name = minio_s3_bucket.landing_bucket.bucket
  object_name = "ipca/${each.value}"
  content = file("minio/samples/ipca/${each.value}")
  content_type = "text/plain"
}

resource "minio_s3_object" "spark_logs_padding" {
  depends_on = [minio_s3_bucket.spark_logs_bucket]
  bucket_name = minio_s3_bucket.spark_logs_bucket.bucket
  object_name = "logs/.keep"
  content = "I shouldn't even exist"
  content_type = "text/plain"
}
