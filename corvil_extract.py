#!/usr/bin/env python3
"""
CPM Corvil Extract Tools (reconstructed from screenshots)

NOTES:
- This script references project-specific helpers/classes that are not shown in the photos:
  - event_log(...)
  - Market(...)
  - cpm_connect(...)
  - run_command(...)
  - csv-comma2soh helper (external binary in Linux branch)
  - CorvilApiStreamingClient.py (external utility)
- Some branches/pipelines (especially Linux) were partially truncated across photos; I preserved
  what was visible and kept the intent consistent.
"""

import argparse
import datetime
import gzip
import os
import re
import smtplib
import sys
import yaml

from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText


# -------------------------
# Argument parsing
# -------------------------
def get_arg_parser(valid_extracts):
    description = "CPM Corvil Extract Tools"
    parser = argparse.ArgumentParser(description=description)

    subparsers = parser.add_subparsers(help="Commands", dest="execution_mode")
    subparsers.required = True

    # LIST
    list_parser = subparsers.add_parser(name="list", help="Extracts available for a mic")
    list_parser.add_argument(
        "-m", "--mic",
        choices=valid_extracts.keys(),
        required=True,
        action="store",
        type=str,
        help="MIC to list the extracts for"
    )

    # EXTRACT
    extract_parser = subparsers.add_parser(name="extract", help="Generate an extract")
    extract_parser.add_argument(
        "-m", "--mic",
        choices=valid_extracts.keys(),
        required=True,
        action="store",
        type=str,
        help="Target MIC for the extracts"
    )

    extract_parser.add_argument(
        "-x", "--extract_name",
        required=True,
        action="store",
        type=str,
        help="Name of the extract to pull"
    )

    extract_parser.add_argument(
        "--start_time",
        required=True,
        action="store",
        type=lambda s: datetime.datetime.strptime(s, "%Y-%m-%d %H:%M:%S"),
        help="Start Date in YYYY-MM-DD HH:MI:SS format."
    )

    extract_parser.add_argument(
        "--end_time",
        required=True,
        action="store",
        type=lambda s: datetime.datetime.strptime(s, "%Y-%m-%d %H:%M:%S"),
        help="End Date in YYYY-MM-DD HH:MI:SS format."
    )

    extract_parser.add_argument(
        "-f", "--filename",
        required=False,
        action="store",
        type=str,
        help=(
            "Filename. Defaults to "
            "<mic>_<extract_name>_<start_time YYYYMMDD_H_M_S>"
            "_to_<end_time YYYYMMDD_H_M_S>.csv"
        )
    )

    extract_parser.add_argument(
        "-c", "--compress",
        action="store_true",
        required=False,
        default=False,
        help="Compress file"
    )

    extract_parser.add_argument(
        "-o", "--overwrite",
        action="store_true",
        required=False,
        default=False,
        help="Overwrite output files if they already exist"
    )

    extract_parser.add_argument(
        "--console",
        action="store_true",
        required=False,
        default=False,
        help="Output to console only"
    )

    extract_parser.add_argument(
        "--human",
        action="store_true",
        required=False,
        default=False,
        help="Database formatted file readable by humans"
    )

    extract_parser.add_argument(
        "--manifest",
        action="store_true",
        required=False,
        default=False,
        help="Create manifest file"
    )

    extract_parser.add_argument(
        "--mnemonic",
        required=False,
        action="store",
        type=str,
        help="mnemonic used in manifest"
    )

    # NOTE: Screenshots show this as required=True with default False (odd, but preserved)
    extract_parser.add_argument(
        "--testing",
        action="store",
        required=True,
        default=False,
        help="Set to True when testing"
    )

    extract_parser.add_argument(
        "--wildcard",
        action="store_true",
        required=False,
        default=False,
        help="Set the column list to wildcard"
    )

    extract_parser.add_argument(
        "--no_verify",
        action="store_true",
        required=False,
        default=False,
        help="Do not verify the column output against the config"
    )

    return parser.parse_args()


# -------------------------
# Config -> valid extracts
# -------------------------
def get_valid_extracts(corvilConfig):
    valid_extracts = {}

    for key, value in corvilConfig["markets"].items():
        if "extracts" in value.keys():
            tmp_dict = {}

            # Consider and extract valid if it has cne, rt-class and decoder_extract value
            for extract, properties in value["extracts"].items():
                entry_complete = True

                if ("cne" not in properties.keys() or
                        ("cne" in properties.keys() and len(properties["cne"]) == 0)):
                    entry_complete = False

                if ("rt-class" not in properties.keys() or
                        ("rt-class" in properties.keys() and len(properties["rt-class"]) == 0)):
                    entry_complete = False

                if ("decoder_extracts" not in properties.keys() or
                        ("decoder_extracts" in properties.keys() and len(properties["decoder_extracts"]) == 0)):
                    entry_complete = False

                if entry_complete is True:
                    tmp_dict[extract] = properties

            valid_extracts[key] = tmp_dict
            del tmp_dict

    return valid_extracts


def list_extracts(valid_extracts, mic):
    msg = "List of available extracts for mic: {mic}\n".format(mic=mic)
    print(msg)

    for key, value in valid_extracts[mic].items():
        msg = "{extract_name}: {extract_info}".format(extract_name=key, extract_info=str(value))
        print(msg)


# -------------------------
# Email
# -------------------------
def send_mail(lf, rt_class, start_time, end_time):
    send_from = "CPM-US@theice.com"
    send_to = "CPM-US@theice.com"

    msg = MIMEMultipart("alternative")
    msg["Subject"] = "CORVIL EXTRACT ERROR"
    msg["From"] = send_from
    msg["To"] = send_to
    msg["X-Priority"] = "1"

    body = MIMEText(
        "ERROR while extracting:\n{rt} {stime}-{etime}".format(
            rt=rt_class,
            stime=start_time,
            etime=end_time
        )
    )
    msg.attach(body)

    s = smtplib.SMTP("localhost")
    try:
        s.sendmail(send_from, send_to, msg.as_string())
    except Exception as e:
        lf.write("Error sending email: {}".format(e))
    s.quit()


# -------------------------
# Verify columns
# -------------------------
def verify_cols(lf, verify_header_filename, verify_field_list, rt_class, start_time, end_time):
    lf.write("Running column verification")

    with open(verify_header_filename) as f:
        header_row = f.readline().strip("\n")

    col_list = header_row.split(",")

    verified = True

    # Column count checks
    if len(verify_field_list) != len(col_list):
        lf.write("Verification failed. Column count mismatch")
        lf.write("Expected Column Count: " + str(len(verify_field_list)))
        lf.write("Actual Column Count: " + str(len(col_list)))
        verified = False
    else:
        # Verify identical column ordering
        i = 0
        while i < len(verify_field_list):
            if col_list[i] != verify_field_list[i]:
                lf.write("Column mismatch at output file column position " + str(i + 1))
                lf.write("Expected column name: " + verify_field_list[i])
                lf.write("Got column name: " + col_list[i])
                verified = False
                break
            i += 1

    if verified is False:
        lf.write("Expected Columns:")
        lf.write(str(verify_field_list))
        lf.write("Actual Columns:")
        lf.write(str(col_list))

        # TODO in screenshot: Send a high priority splunk alert and email
        send_mail(lf, rt_class, start_time, end_time)
        lf.write("Script terminated with ERROR - exit code 1")
        # In screenshots, there are also commented lines about killing the process

    else:
        lf.write("Column verification passed")
        lf.write("Deleting verification file: " + verify_header_filename)
        os.remove(verify_header_filename)

    return verified


# -------------------------
# Utility: line count for gz
# -------------------------
def file_lcount(fname):
    with gzip.open(fname, mode="rb") as f:
        for i, _ in enumerate(f):
            pass
    return i + 1


# -------------------------
# Main
# -------------------------
def main():
    # Config files
    configPath = "../../Configurations/"
    corvil_file_str = configPath + "ref_corvil.yaml"
    config_file_str = configPath + "ref_market_db.yaml"
    accounts_file_str = configPath + "ref_accounts.yaml"
    connections_file_str = configPath + "ref_connections.yaml"

    with open(corvil_file_str) as f:
        ref_corvil_config = yaml.safe_load(f)

    corvilConfig = ref_corvil_config

    # Build valid extracts
    valid_extracts = get_valid_extracts(corvilConfig)

    # Parse args (requires valid_extracts)
    args = get_arg_parser(valid_extracts)

    mic = args.mic
    execution_mode = args.execution_mode

    if execution_mode == "list":
        list_extracts(valid_extracts, mic)
        return

    # execution_mode == "extract"
    extract_name = args.extract_name

    start_time_dt = args.start_time
    start_time = start_time_dt.strftime("%Y-%m-%d %H:%M:%S")
    start_time_logfile = start_time.replace(" ", "-").replace(":", "-")
    start_time_filename = start_time_dt.strftime("%Y%m%d_%H-%M-%S")

    end_time_dt = args.end_time
    end_time = end_time_dt.strftime("%Y-%m-%d %H:%M:%S")
    end_time_logfile = end_time.replace(" ", "-").replace(":", "-")
    end_time_filename = end_time_dt.strftime("%Y%m%d_%H-%M-%S")

    # Create an end time for the test file used for column verification
    end_time_verify_dt = start_time_dt + datetime.timedelta(seconds=1)
    end_time_verify_str = end_time_verify_dt.strftime("%Y-%m-%d %H:%M:%S")

    # Logging (external helper)
    lf = event_log(
        strLogDirPrefix="corvil_extract_utility",
        strLogFilePrefix=mic + "_" + extract_name,
        strStartDate=start_time_logfile,
        strEndDate=end_time_logfile,
        boolConsoleOutput=True
    )

    lf.write("Command line arguments used to invoke the script: " + str(args))

    # Market (external helper)
    market = Market(config_file_str, mic)

    if os.name.lower() == "nt":
        operating_system = "windows"
    else:
        operating_system = "linux"

    if extract_name not in valid_extracts[mic].keys():
        lf.write("Invalid extract name specified. Please choose one of the valid values below")
        list_extracts(valid_extracts, mic)
        return

    database = market.m2_dbms
    environment = "prod"

    # Connect (external helper)
    connections = cpm_connect(
        connectionFile=connections_file_str,
        accountsFile=accounts_file_str,
        database=database,
        environment=environment
    )

    corvil_credentials = connections.get_stored_credentials("corvil-" + environment)

    python = "python"
    corvil_utility = r"CorvilApiStreamingClient.py"

    username = corvil_credentials["username"]
    password = corvil_credentials["password"]

    cne = valid_extracts[mic][extract_name]["cne"]
    ip_address = ref_corvil_config["corvil"][environment]["cne"][cne]["ip"]
    rt_class = valid_extracts[mic][extract_name]["rt-class"]
    decoder_extract = valid_extracts[mic][extract_name]["decoder_extracts"]

    # Create a list of fields to send to the extract utility
    field_list = ",".join(
        corvilConfig["decoder_extracts"][decoder_extract]["extract_fields"]
    )

    # Get Corvil added fields for the extract
    corvil_added_fields = corvilConfig["decoder_extracts"][decoder_extract]["corvil_added_fields"]

    # Create expected column list
    temp_field_list = field_list.replace('"', "")
    verify_field_list = corvil_added_fields.copy() + temp_field_list.split(",")

    compress = args.compress
    human = args.human
    manifest = args.manifest
    mnemonic = args.mnemonic
    testing = args.testing

    # Filename
    if args.filename is not None:
        filename = args.filename
    else:
        filename = "{mic}_{extract}_{start}_to_{end}".format(
            mic=mic,
            extract=extract_name,
            start=start_time_filename,
            end=end_time_filename
        )

    test_filename = filename + "_test"
    verify_filename = filename + "_temp_verify.csv"
    verify_test_filename = test_filename + "_temp_verify.csv"

    # Check if output exists
    if (os.path.isfile(filename + ".csv") or
            (compress is True and os.path.isfile(filename + ".csv.gz"))):

        if args.overwrite is True:
            msg = "Deleting {filename}"
            if os.path.isfile(filename + ".csv"):
                lf.write(msg.format(filename=filename))
                os.remove(filename + ".csv")

            if os.path.isfile(filename + ".csv.gz"):
                lf.write(msg.format(filename=filename + ".csv.gz"))
                os.remove(filename + ".csv.gz")
        else:
            msg = "{filename} found. Please delete or rerun with the overwrite flag."
            lf.write(msg.format(filename=filename))
            lf.write("Exiting")
            sys.exit(1)

    # Base command strings
    if args.wildcard is True:
        base_cmd = (
            "{python} ./{corvil_utility} -c -b -n {username} -p {password} "
            "message-csv {ip_address} {rt_class} \"{start_time}\" \"{end_time}\""
        )
        base_cmd_test = base_cmd
    else:
        base_cmd = (
            "{python} ./{corvil_utility} -c -b -n {username} -p {password} "
            "message-csv {ip_address} {rt_class} \"{start_time}\" \"{end_time}\" {field_list}"
        )
        base_cmd_test = base_cmd

    # Substitute placeholder values
    base_cmd = base_cmd.format(
        python=python,
        corvil_utility=corvil_utility,
        username=username,
        password=password,
        ip_address=ip_address,
        rt_class=rt_class,
        start_time=start_time,
        end_time=end_time,
        field_list=field_list
    )

    base_cmd_test_file = base_cmd_test.format(
        python=python,
        corvil_utility=corvil_utility,
        username=username,
        password=password,
        ip_address=ip_address,
        rt_class=rt_class,
        start_time=start_time,
        end_time=end_time_verify_str,
        field_list=field_list
    )

    # ---------------------------------------------------------
    # Linux commands (as shown)
    # ---------------------------------------------------------
    if operating_system == "linux":
        # separates the file_path from the file_name
        f_name = re.split(r"/", filename)[-1]
        f_path = re.split(f_name, filename)[0]

        # Build suffix pipelines
        if args.console is True and human is True:
            cmd_suffix = " | ./csv-comma2soh | tr '\\001' ','"

        elif args.console is True:
            cmd_suffix = " | ./csv-comma2soh"

        elif args.console is False and compress is False and human is True:
            test_file_cmd_suffix = (
                " | ./csv-comma2soh | tr '\\001' ',' | /bin/gzip > {filename}.csv.gz"
            ).format(filename=test_filename)

            test_filename_remove = test_filename + ".csv.gz"

            # header commands (as shown; two variants appeared in photos)
            get_csv_header = "zcat {filename}.csv.gz | sed -n '6p' > " + verify_filename
            get_csv_header_temp = "zcat {filename}.csv.gz | head -1 > {verify_filename}"
            get_test_file_csv_header = get_csv_header_temp.format(
                filename=test_filename,
                verify_filename=verify_test_filename
            )

            cmd_suffix = (
                " | ./csv-comma2soh | tr '\\001' ',' > {filename}.csv"
            ).format(filename=filename)

        elif args.console is False and compress is True and human is True:
            # In one screenshot this branch includes a split/filter= gzip pipeline that was truncated.
            test_file_cmd_suffix = (
                " | ./csv-comma2soh | tr '\\001' ',' | /bin/gzip > {filename}.csv.gz"
            ).format(filename=test_filename)

            test_filename_remove = test_filename + ".csv.gz"

            get_csv_header = "zcat {filename}.csv.gz | sed -n '6p' > " + verify_filename
            get_csv_header_temp = "zcat {filename}.csv.gz | head -1 > {verify_filename}"
            get_test_file_csv_header = get_csv_header_temp.format(
                filename=test_filename,
                verify_filename=verify_test_filename
            )

            # Truncated in photo; keeping simplest consistent intent:
            cmd_suffix = (
                " | ./csv-comma2soh | tr '\\001' ',' | /bin/gzip > {filename}.csv.gz"
            ).format(filename=filename)

        elif args.console is False and compress is True:
            test_file_cmd_suffix = (
                " | ./csv-comma2soh | tr '\\001' ',' | /bin/gzip > {filename}.csv.gz"
            ).format(filename=test_filename)

            test_filename_remove = test_filename + ".csv.gz"

            get_csv_header = "zcat {filename}.csv.gz | sed -n '6p' > " + verify_filename
            get_csv_header_temp = "zcat {filename}.csv.gz | head -1 > {verify_filename}"
            get_test_file_csv_header = get_csv_header_temp.format(
                filename=test_filename,
                verify_filename=verify_test_filename
            )

            cmd_suffix = (
                " | ./csv-comma2soh | /bin/gzip > {filename}.csv.gz"
            ).format(filename=filename)

        else:
            # args.console False and compress False (plain)
            test_file_cmd_suffix = (
                " | ./csv-comma2soh | tr '\\001' ',' | /bin/gzip > {filename}.csv.gz"
            ).format(filename=test_filename)

            test_filename_remove = test_filename + ".csv.gz"

            get_csv_header = "zcat {filename}.csv.gz | sed -n '6p' > " + verify_filename
            get_csv_header_temp = "zcat {filename}.csv.gz | head -1 > {verify_filename}"
            get_test_file_csv_header = get_csv_header_temp.format(
                filename=test_filename,
                verify_filename=verify_test_filename
            )

            cmd_suffix = (
                " | ./csv-comma2soh > {filename}.csv"
            ).format(filename=filename)

        # SET TEST FILE and ACTUAL FILE COMMANDS
        final_test_file_command = base_cmd_test_file + " " + test_file_cmd_suffix
        final_command = base_cmd + " " + cmd_suffix

        # Run a test extract and verify columns
        if args.console is False and args.wildcard is False and args.no_verify is False:
            msg = "Generating test file for column verification"
            run_command(final_test_file_command, msg)

            lf.write("Generating verification file")

            # Copy the header row (shown as: get the 6th line)
            first_line = None
            with open(test_filename) as f:
                for i, line in enumerate(f):
                    if i == 5:
                        first_line = line
                        break

            with open(verify_test_filename, "w") as f:
                if first_line is not None:
                    f.write(first_line)

            verify_cols(
                lf=lf,
                verify_header_filename=verify_test_filename,
                verify_field_list=verify_field_list,
                rt_class=rt_class,
                start_time=start_time,
                end_time=end_time
            )

            lf.write("Deleting test extract file: " + test_filename + ".csv")
            os.remove(test_filename + ".csv")

            lf.write("---------------------------------------------------------------")

        msg = "Running extract"
        run_command(final_command, msg)

        # Compress, if needed
        if compress is True:
            compress_cmd = "tar -czf {filename}.csv.gz {filename}.csv".format(filename=filename)
            msg = "Compressing file"
            run_command(compress_cmd, msg)

            msg = "Deleting uncompressed version"
            lf.write(msg)
            os.remove(filename + ".csv")

        # Make manifest file (shown only under Linux, compress True, manifest True, mnemonic set)
        if (args.mnemonic is not None and
                args.console is False and
                compress is True and
                manifest is True):

            file_list = [
                f for f in os.listdir(f_path)
                if os.path.isfile(os.path.join(f_path, f))
            ]

            rx = r"^{fn}.*.gz".format(fn=f_name)

            for file_names in file_list:
                if re.match(rx, file_names) is not None:
                    manifestfile = re.sub(r".gz", ".manifest", file_names)
                    line_count = file_lcount(os.path.join(f_path, file_names))
                    size = os.path.getsize(os.path.join(f_path, file_names))

                    if size < 5000 and testing == "False":
                        manifestfile = manifestfile + ".error"

                    with open(os.path.join(f_path, manifestfile), "w") as mf:
                        mf.write(mnemonic + "|" + file_names + "|" + str(size) + "|" + "0\n")

    # ---------------------------------------------------------
    # Windows commands (only partially shown; keeping what was visible)
    # ---------------------------------------------------------
    elif operating_system == "windows":
        if args.console is True:
            cmd_suffix = ""
        else:
            cmd_suffix = " > {filename}.csv".format(filename=filename)
            final_test_file_command = base_cmd_test_file + " > " + test_filename + ".csv"

        final_command = base_cmd + cmd_suffix

        # Verify the extract with a test file
        if args.console is False and args.wildcard is False and args.no_verify is False:
            msg = "-Generating test file for column verification----------------"
            run_command(final_test_file_command, msg)

        msg = "Running extract"
        run_command(final_command, msg)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        # Screenshot ends with print(myerror); keeping intent
        print(e)
