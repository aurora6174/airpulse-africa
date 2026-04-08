resource "aws_s3_bucket" "datalake" {
  bucket        = var.bucket_name
  force_destroy = true
  tags = { Project = var.project_tag }
}

resource "aws_s3_bucket_versioning" "datalake" {
  bucket = aws_s3_bucket.datalake.id
  versioning_configuration { status = "Enabled" }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "datalake" {
  bucket = aws_s3_bucket.datalake.id
  rule {
    apply_server_side_encryption_by_default { sse_algorithm = "AES256" }
  }
}

resource "aws_iam_user" "airpulse" {
  name = "airpulse-pipeline"
  tags = { Project = var.project_tag }
}

resource "aws_iam_access_key" "airpulse" {
  user = aws_iam_user.airpulse.name
}

resource "aws_iam_user_policy" "s3_access" {
  name = "airpulse-s3-policy"
  user = aws_iam_user.airpulse.name
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect   = "Allow"
      Action   = ["s3:PutObject","s3:GetObject","s3:ListBucket","s3:DeleteObject"]
      Resource = [
        aws_s3_bucket.datalake.arn,
        "${aws_s3_bucket.datalake.arn}/*"
      ]
    }]
  })
}
