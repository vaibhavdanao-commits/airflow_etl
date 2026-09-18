import boto3

from config import (
    AWS_ACCESS_KEY_ID,
    AWS_SECRET_ACCESS_KEY,
    AWS_REGION,
    S3_BUCKET
)


# ============================================================
# CREATE S3 CLIENT
# ============================================================

s3 = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_REGION
)


# ============================================================
# CREATE S3 ZONES
# ============================================================

def create_s3_zones():

    zones = [
        "raw/",
        "processed/",
        "curated/"
    ]

    print("=" * 70)
    print("TASK 2 - S3 DATA LAKE ZONES")
    print("=" * 70)

    print(f"\nBucket : {S3_BUCKET}")
    print(f"Region : {AWS_REGION}")

    for zone in zones:

        s3.put_object(
            Bucket=S3_BUCKET,
            Key=zone
        )

        print(
            f"Created: s3://{S3_BUCKET}/{zone}"
        )

    print("\n" + "=" * 70)
    print("S3 DATA LAKE ZONES CREATED SUCCESSFULLY")
    print("=" * 70)


# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":
    create_s3_zones()