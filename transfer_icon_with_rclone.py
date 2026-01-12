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
    from typing import TypedDict, ReadOnly

    # Configure your standard python logger
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    log = logging.getLogger("rclone_sync")
    return (
        PurePosixPath,
        ReadOnly,
        TypedDict,
        datetime,
        defaultdict,
        json,
        log,
        logging,
        os,
        re,
        subprocess,
        tempfile,
    )


@app.cell
def _(PurePosixPath, re):
    FTP_HOST = "opendata.dwd.de"
    FTP_ROOT_PATH = PurePosixPath("/weather/nwp/icon-eu/grib/")
    NWP_RUN = "00"

    # Regex to find the date in the filename (YYYYMMDDHH)
    DATE_REGEX = re.compile(r"_(\d{10})_")
    return DATE_REGEX, FTP_HOST, FTP_ROOT_PATH, NWP_RUN


@app.cell
def _(log, logging):
    def log_rclone_output(stderr: str):
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
    NWP_RUN,
    PurePosixPath,
    ReadOnly,
    TypedDict,
    json,
    log,
    log_rclone_output,
    subprocess,
):
    class ListItem(TypedDict):
        Path: ReadOnly[str]
        Name: ReadOnly[str]
        Size: ReadOnly[int]
        ModTime: ReadOnly[str]
        IsDir: ReadOnly[bool]


    def ftp_list(ftp_host: str, path: PurePosixPath) -> list[ListItem]:
        """
        Uses rclone lsjson to get a full recursive list of files very quickly.
        Returns a list of ListItem dictionaries. Note that the `Path` attribute in the returned dict does not
        include the `path` input to this function. For example, a returned `Path` might look like
        "aswdifd_s/icon-eu_europe_regular-lat-lon_single-level_2026011200_004_ASWDIFD_S.grib2.bz2"
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


    listing = ftp_list(FTP_HOST, FTP_ROOT_PATH / NWP_RUN)
    return ListItem, listing


@app.cell
def _(listing):
    listing[100]
    return


@app.cell
def _(
    DATE_REGEX,
    FTP_ROOT_PATH,
    ListItem,
    NWP_RUN,
    PurePosixPath,
    datetime,
    defaultdict,
    listing,
    log,
):
    DST_ROOT_PATH = PurePosixPath("/home/jack/data/ICON-EU/grib/rsync_and_python")


    def generate_batches(file_list: list[ListItem]) -> dict[tuple[str, PurePosixPath], list[str]]:
        """
        Groups files by their transfer logic:
        (Source Directory) -> (Destination Directory)

        Returns a dict which maps from (src_directory, dst_directory) to a list of filenames.
        For example:
        (
            'opendata.dwd.de/weather/nwp/icon-eu/grib/00/alb_rad',
            PurePosixPath('/home/jack/data/ICON-EU/grib/rsync_and_python/2026-01-12T00Z/alb_rad')
        ): [
            "icon-eu_europe_regular-lat-lon_single-level_2026011200_000_ALB_RAD.grib2.bz2",
            "icon-eu_europe_regular-lat-lon_single-level_2026011200_001_ALB_RAD.grib2.bz2",
            ...
        ]
        """
        batches = defaultdict(list)

        for file_info in file_list:
            if file_info["IsDir"]:
                continue

            file_path = PurePosixPath(file_info["Path"])  # e.g., "alb_rad/filename.grib2.bz2"

            # --- FILTERING ---
            if "pressure-level" in file_path.name:
                continue

            # --- PARSING ---
            # Extract Date
            match = DATE_REGEX.search(file_path.name)
            if not match:
                log.warn("Skipping (no date found): %s", file_path.name)
                continue

            raw_date = match.group(1)
            dt = datetime.strptime(raw_date, "%Y%m%d%H")
            date_dir = dt.strftime("%Y-%m-%dT%HZ")

            # Extract the NWP parameter name from path (e.g., 'alb_rad' from 'alb_rad/file')
            if len(file_path.parts) != 2:
                continue
            param_name = file_path.parts[0]  # e.g. 'alb_rad'

            # Define Source and Dest Bases for this specific file
            # We group by the directory, not the file, so rclone can move lists of files
            src_dir_url = FTP_ROOT_PATH / NWP_RUN / file_path.parent
            dest_dir_url = DST_ROOT_PATH / date_dir / param_name

            # Add filename to this specific batch
            batch_key = (src_dir_url, dest_dir_url)
            batches[batch_key].append(file_path.name)

        return batches


    batches = generate_batches(listing)
    return (batches,)


@app.cell
def _(batches):
    batches
    return


@app.cell
def _(log, os, subprocess, tempfile):
    def run_transfers(batches):
        """
        Iterates through batches, creates a temp file list, and runs rclone.
        """
        total_batches = len(batches)
        log.info("Processing %s batch groups...", total_batches)

        for i, ((src_url, dest_url), filenames) in enumerate(batches.items(), 1):
            log.info(f"[{i}/{total_batches}] Syncing {len(filenames)} files to {dest_url}...")

            # Create a temporary file to hold the list of filenames for this batch
            with tempfile.NamedTemporaryFile(mode="w+", delete=False) as tmp:
                tmp.write("\n".join(filenames))
                tmp_path = tmp.name

            cmd = [
                "rclone",
                "copy",
                "--ftp-host=opendata.dwd.de",
                "--ftp-user=anonymous",
                # rclone requires passwords to be obscured by encrypting & encoding them in base64.
                # The base64 string below was created with the command `rclone obscure guest`.
                "--ftp-pass=JUznDm8DV5bQBCnXNVtpK3dN1qHB",
                f":ftp:{src_url}",
                str(dest_url),
                "--files-from",
                tmp_path,
                "--transfers",
                "10",  # Increase parallelism within the batch
                "--no-check-certificate",
            ]
            log.info("Command: %s", " ".join(cmd))

            try:
                # Run rclone (suppress stdout to keep logs clean, show stderr on error)
                subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL)
            except subprocess.CalledProcessError as e:
                log.exception(f"Error syncing batch {src_url}: {e}")
            finally:
                os.remove(tmp_path)

            if i == 20:
                break
    return (run_transfers,)


@app.cell
def _(batches, run_transfers):
    run_transfers(batches)
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
