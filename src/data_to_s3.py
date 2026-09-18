import sys
from pathlib import Path

import boto3
from botocore.exceptions import NoCredentialsError, ClientError


# ============================================================
# ADD PROJECT ROOT TO PYTHON PATH
# ============================================================

PROJECT_ROOT = Path(__file__).resolve().parent.parent

sys.path.append(str(PROJECT_ROOT))


# ============================================================
# IMPORT CONFIGURATION
# ============================================================

from config import (
    AWS_ACCESS_KEY_ID,
    AWS_SECRET_ACCESS_KEY,
    AWS_REGION,
    S3_BUCKET,
    S3_PREFIX,
    LOCAL_FOLDER
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
# CHECK LOCAL FOLDER
# ============================================================

if not LOCAL_FOLDER.exists():

    print("=" * 70)
    print("ERROR")
    print("=" * 70)

    print(f"Local folder does not exist:")
    print(LOCAL_FOLDER)

    raise SystemExit(1)


# ============================================================
# UPLOAD FILES
# ============================================================

print("=" * 70)
print("UPLOADING LOCAL DATA TO S3")
print("=" * 70)

uploaded_count = 0


for local_file in LOCAL_FOLDER.rglob("*"):

    # Ignore directories
    if not local_file.is_file():
        continue

    # Get path relative to local data folder
    relative_path = local_file.relative_to(LOCAL_FOLDER)

    # Convert Windows path to S3-compatible path
    relative_path = relative_path.as_posix()

    # Create S3 object key
    s3_key = f"{S3_PREFIX}/{relative_path}"

    print()
    print(f"Uploading: {local_file}")
    print(f"To       : s3://{S3_BUCKET}/{s3_key}")

    try:

        s3.upload_file(
            str(local_file),
            S3_BUCKET,
            s3_key
        )

        print("SUCCESS")

        uploaded_count += 1

    except NoCredentialsError:

        print()
        print("ERROR: AWS credentials are missing or invalid.")

        raise SystemExit(1)

    except ClientError as e:

        print()
        print("ERROR: S3 upload failed.")
        print(e)

        raise SystemExit(1)


# ============================================================
# COMPLETION
# ============================================================

print()
print("=" * 70)
print("UPLOAD COMPLETED")
print("=" * 70)

print(f"Files uploaded: {uploaded_count}")

print()
print("S3 location:")
print(f"s3://{S3_BUCKET}/{S3_PREFIX}/")
