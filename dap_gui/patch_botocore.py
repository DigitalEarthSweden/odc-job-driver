import re

import botocore.handlers


def patch_valid_bucket():
    # Define the new regex pattern
    new_valid_bucket = re.compile(r"^[a-zA-Z0-9.\-_:]{1,255}$")

    # Patch the VALID_BUCKET variable in the botocore.handlers module
    botocore.handlers.VALID_BUCKET = new_valid_bucket


# Apply the patch
patch_valid_bucket()
