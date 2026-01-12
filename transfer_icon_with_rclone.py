# /// script
# requires-python = ">=3.14"
# dependencies = [
#     "marimo>=0.19.2",
#     "ruff==0.14.11",
# ]
# ///

import marimo

__generated_with = "0.19.2"
app = marimo.App(width="full")


@app.cell
def _():
    import subprocess
    import json
    import re
    import os
    import tempfile
    from collections import defaultdict
    from datetime import datetime
    from pathlib import PurePosixPath
    import logging

    # Configure your standard python logger
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    log = logging.getLogger("rclone_sync")
    return PurePosixPath, json, log, logging, re, subprocess


@app.cell
def _(PurePosixPath, re):
    FTP_HOST = "opendata.dwd.de"
    FTP_ROOT_PATH = PurePosixPath("/weather/nwp/icon-eu/grib/")

    # Regex to find the date in the filename (YYYYMMDDHH)
    DATE_REGEX = re.compile(r"_(\d{10})_")
    return FTP_HOST, FTP_ROOT_PATH


@app.cell
def _(log, logging):
    def log_rclone_output(stderr: str):
        # Read stderr line by line as it is produced
        for line in stderr.splitlines():
            clean_line = line.strip()
            if not clean_line:
                continue

            # rclone uses "Error", "Warning", "Info" in its output.
            line_upper = clean_line.upper()
            if "ERROR" in line_upper or "FAILED" in line_upper:
                log_level = logging.ERROR
            elif "WARNING" in line_upper:
                log_level = logging.WARNING
            else:
                log_level = logging.INFO

            log.log(level=log_level, msg=f"Rclone: {clean_line}")
    return (log_rclone_output,)


@app.cell
def _(
    FTP_HOST,
    FTP_ROOT_PATH,
    PurePosixPath,
    json,
    log,
    log_rclone_output,
    subprocess,
):
    def list(ftp_host: str, path: PurePosixPath):
        """
        Uses rclone lsjson to get a full recursive list of files very quickly.
        Returns a list of file dictionaries.
        """
        log.info(f"Listing ftp://{ftp_host}{path} ...")
        cmd = [
            "rclone",
            "lsjson",
            f"--ftp-host={ftp_host}",
            "--ftp-user=anonymous",
            # rclone requires passwords to be obscured by encrypting & encoding them in base64.
            # The base64 string below was created with the command `rclone obscure guest`.
            "--ftp-pass=JUznDm8DV5bQBCnXNVtpK3dN1qHB",
            f":ftp:{path}",
            "--recursive",
            "--fast-list",  # Optimizes listing for some remotes.
            "--no-mimetype",  # Don't read the mime type (can speed things up).
            "--no-modtime",  # Don't read the modification time (can speed things up).
            "--quiet",
        ]

        log.info("Command: %s", " ".join(cmd))
        # Rclone sends its progress and status messages to stderr.
        process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        # The subprocess docs say we can't use `process.wait` if the process returns lots of data in a PIPE,
        # instead we have to use `process.communicate`.
        stdout_str, stderr_str = process.communicate(timeout=90)
        log_rclone_output(stderr_str)

        if process.returncode != 0:
            error_msg = f"rclone return code is {process.returncode}"
            log.error(error_msg)
            raise RuntimeError(error_msg)

        try:
            return json.loads(stdout_str)
        except json.decoder.JSONDecodeError as e:
            log.exception("Failed to decode stdout of rclone as json. Stdout of rclone='%s'.", stdout_str)
            raise


    listing = list(FTP_HOST, FTP_ROOT_PATH / "00")
    return (listing,)


@app.cell
def _(listing):
    listing[-10:]
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
