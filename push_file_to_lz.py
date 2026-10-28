import os
import time
from dotenv import load_dotenv
import requests
import json
import logging
from datetime import datetime
import utils
import constants

logger = logging.getLogger(__name__)

# Fix 3: HTTP timeout constants (connect_timeout, read_timeout) in seconds
HTTP_CONNECT_TIMEOUT = int(os.getenv("HTTP_CONNECT_TIMEOUT_SEC", "10"))
HTTP_READ_TIMEOUT = int(os.getenv("HTTP_READ_TIMEOUT_SEC", "60"))
HTTP_TIMEOUT = (HTTP_CONNECT_TIMEOUT, HTTP_READ_TIMEOUT)

# Fix 1: Retry constants
MAX_UPLOAD_RETRIES = int(os.getenv("MAX_UPLOAD_RETRIES", "3"))
UPLOAD_RETRY_BASE_DELAY_SEC = float(os.getenv("UPLOAD_RETRY_BASE_DELAY_SEC", "2"))


def _lz_folder_url(lz_url: str, table_name: str = "") -> str:
    """Landing zone folder URL; empty table_name = mirrored database root (LandingZone/)."""
    base = lz_url.rstrip("/")
    if not table_name:
        return base + "/"
    return f"{base}/{table_name}/"


def push_file_to_lz(
    filepath: str,
    table_name: str,
):
    logger.info(f"pushing file to lz. table_name={table_name}, filepath={filepath}")
    try:
        if os.getenv("DEBUG__SKIP_PUSH_TO_LZ"):
            logger.info("Push to LZ skipped by environment variable DEBUG__SKIP_PUSH_TO_LZ")
        else:
            access_token = __get_access_token(
                os.getenv("APP_ID"), os.getenv("SECRET"), os.getenv("TENANT_ID")
            )
            # Fix 1: retry with exponential backoff
            __patch_file_with_retry(access_token, filepath, os.getenv("LZ_URL"), table_name)
        # identify if any other parquet files in the dir, and remove them, leaving only the last one we just pushed
        __clean_up_old_parquet_files(filepath)
    except Exception as e:
        logger.error(f"Error pushing file to lz: {str(e)}")
        raise


def __clean_up_old_parquet_files(filepath: str):
    if os.getenv("DEBUG__SKIP_PARQUET_FILES_CLEAN_UP"):
        return
    filename_stem = os.path.splitext(os.path.basename(filepath))[0]
    filename_ext = os.path.splitext(os.path.basename(filepath))[1]
    # do nothing if it's a parquet file with prefix, or it's not a parquet file
    if not filename_stem.isnumeric() or filename_ext != ".parquet":
        return
    logger.debug(f"Cleaning up old parquet files. Current file: {filepath}")
    dir = os.path.dirname(filepath)
    current_filename = os.path.basename(filepath)
    old_parquet_filename_list = [
        filename
        for filename in os.listdir(dir)
        if os.path.splitext(filename)[1] == ".parquet"
        and os.path.splitext(filename)[0].isnumeric()
        and int(os.path.splitext(filename)[0]) < int(filename_stem)
    ]
    for old_parquet_filename in old_parquet_filename_list:
        logger.debug(f"Deleting old parquet file {old_parquet_filename}")
        os.remove(os.path.join(dir, old_parquet_filename))


def __get_access_token(app_id, client_secret, directory_id):
    """It will create a access token to access the mail apis"""
    app_id = app_id  # Application Id - on the azure app overview page
    client_secret = client_secret
    directory_id = directory_id
    token_url = (
        "https://login.microsoftonline.com/" + directory_id + "/oauth2/v2.0/token"
    )
    token_data = {
        "grant_type": "client_credentials",
        "client_id": app_id,
        "client_secret": client_secret,
        "scope": "https://storage.azure.com/.default",
    }
    token_headers = {"Content-Type": "application/x-www-form-urlencoded"}
    # Fix 3: add timeout
    token_response = requests.post(token_url, data=token_data, headers=token_headers, timeout=HTTP_TIMEOUT)
    token_response_dict = json.loads(token_response.text)

    token = token_response_dict.get("access_token")

    if token == None:
        logger.debug("Unable to get access token")
        logger.debug(str(token_response_dict))
        raise Exception("Error in getting in access token")
    else:
        return token


def __patch_file_with_retry(access_token, file_path, lz_url, table_name):
    """Fix 1: Retry wrapper around __patch_file with exponential backoff."""
    last_exception = None
    for attempt in range(1, MAX_UPLOAD_RETRIES + 1):
        try:
            __patch_file(access_token, file_path, lz_url, table_name)
            return  # success
        except Exception as e:
            last_exception = e
            delay = UPLOAD_RETRY_BASE_DELAY_SEC * (2 ** (attempt - 1))
            logger.warning(
                "Upload attempt %d/%d failed for %s: %s. Retrying in %.1fs...",
                attempt, MAX_UPLOAD_RETRIES, file_path, e, delay,
            )
            if attempt < MAX_UPLOAD_RETRIES:
                time.sleep(delay)
    logger.error(
        "All %d upload attempts failed for %s: %s",
        MAX_UPLOAD_RETRIES, file_path, last_exception,
    )
    raise last_exception


def __delete_temp_file(access_token, base_url, file_name_temp):
    """Fix 2: Best-effort cleanup of orphaned *_TEMP file in OneLake."""
    try:
        temp_url = base_url + file_name_temp + '_TEMP'
        headers = {"Authorization": "Bearer " + access_token, "content-length": "0"}
        response = requests.delete(temp_url, headers=headers, timeout=HTTP_TIMEOUT)
        if response.status_code in (200, 204):
            logger.info("Cleaned up orphaned TEMP file: %s", file_name_temp + '_TEMP')
        else:
            logger.warning(
                "Failed to clean up TEMP file %s_TEMP (status %d)",
                file_name_temp, response.status_code,
            )
    except Exception as cleanup_err:
        logger.warning("Error during TEMP file cleanup: %s", cleanup_err)


def __patch_file(access_token, file_path, lz_url, table_name):
    file_name = os.path.basename(file_path)
    file_name_temp = file_name
    base_url = _lz_folder_url(lz_url, table_name)
    if not file_name.startswith('_'):
        file_name_temp = '_' + file_name
    else:
        file_name_temp = file_name

    temp_created = False
    try:
        token_url_temp = base_url + file_name_temp + '_TEMP' + "?resource=file"
        token_url = base_url + file_name

        token_headers = {"Authorization": "Bearer " + access_token, "content-length": "0"}
        logger.debug("creating file in lake")

        # Step 1: Create temp file in lakehouse
        # Fix 3: add timeout
        response = requests.put(token_url_temp, data={}, headers=token_headers, timeout=HTTP_TIMEOUT)
        response.raise_for_status()
        temp_created = True
        logger.debug(response)

        token_url_temp = base_url + file_name_temp + '_TEMP' + "?position=0&action=append&flush=true"
        token_headers = {
            "Authorization": "Bearer " + access_token,
            "x-ms-file-name": file_name,
        }

        logger.debug(token_url_temp)
        logger.debug("pushing data to file in lake")

        # Step 2: Append data to temp file
        # Fix 3: add timeout
        with open(file_path, "rb") as file:
            file_contents = file.read()
            response = requests.patch(token_url_temp, data=file_contents, headers=token_headers, timeout=HTTP_TIMEOUT)
        response.raise_for_status()
        logger.debug(response)

        # Step 3: Rename temp to final name
        token_headers = {
            "Authorization": "Bearer " + access_token,
            "x-ms-rename-source": base_url + file_name_temp + '_TEMP' + "?resource=file",
            "x-ms-version": "2020-06-12"
        }
        # Fix 3: add timeout
        response = requests.put(token_url, headers=token_headers, timeout=HTTP_TIMEOUT)
        response.raise_for_status()
        temp_created = False  # rename succeeded, no orphan to clean
        logger.debug(response)
    except Exception as e:
        logger.error(f"Error patching file to landing zone: {str(e)}")
        # Fix 2: clean up orphaned TEMP file
        if temp_created:
            __delete_temp_file(access_token, base_url, file_name_temp)
        raise


def get_file_from_lz(table_name, file_name):
    logger.info(
        f"trying to get file from lz. table_name={table_name}, file_name={file_name}"
    )
    access_token = __get_access_token(
        os.getenv("APP_ID"), os.getenv("SECRET"), os.getenv("TENANT_ID")
    )
    token_headers = {"Authorization": "Bearer " + access_token, "content-length": "0"}
    url = _lz_folder_url(os.getenv("LZ_URL"), table_name) + file_name
    # Fix 3: add timeout
    response = requests.get(url, headers=token_headers, timeout=HTTP_TIMEOUT)
    response_status_code = response.status_code
    if response_status_code != 200:
        logger.warning(
            f"failed to get file from Landing Zone. Server responded with code {response_status_code}"
        )
        return None, None
    local_file_path = os.path.join(utils.get_table_dir(table_name), file_name)
    return (response_status_code, response)


def get_file_from_lz_root(file_name: str):
    """Read a file from the landing zone root (mirrored database level, not per table)."""
    return get_file_from_lz("", file_name)


def push_file_to_lz_root(filepath: str):
    """Push a file to the landing zone root (mirrored database level, not per table)."""
    push_file_to_lz(filepath, "")


def delete_file_from_lz(table_name, file_name):
    logger.info(
        f"trying to delete file from lz. table_name={table_name}, file_name={file_name}"
    )
    access_token = __get_access_token(
        os.getenv("APP_ID"), os.getenv("SECRET"), os.getenv("TENANT_ID")
    )
    token_headers = {"Authorization": "Bearer " + access_token, "content-length": "0"}
    url = _lz_folder_url(os.getenv("LZ_URL"), table_name) + file_name
    # Fix 3: add timeout
    response = requests.delete(url, headers=token_headers, timeout=HTTP_TIMEOUT)
    logger.debug(f"delete response: {response}")
    return response.status_code if response.status_code == 200 else None
