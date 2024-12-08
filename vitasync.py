import argparse
import os
import shutil
import time
from ftplib import FTP, error_perm
from tqdm import tqdm
import signal


def list_directory_with_details(ftp):
    """List directory contents with details from the LIST command."""
    items = {}

    def parse_line(line):
        parts = line.split()
        if len(parts) >= 9:
            filename = parts[-1]
            try:
                mtime = time.mktime(time.strptime(" ".join(parts[5:8]), "%b %d %H:%M"))
                items[filename] = mtime
            except ValueError:
                items[filename] = 0  # Default timestamp if parsing fails

    try:
        ftp.retrlines('LIST', parse_line)
    except error_perm as e:
        print(f"Failed to list directory: {e}")
    return items


def is_directory(ftp, item):
    """Check if an item is a directory by attempting to change into it."""
    current_dir = ftp.pwd()
    try:
        ftp.cwd(item)
        ftp.cwd(current_dir)  # Return to the original directory
        return True
    except error_perm:
        return False


def count_files(ftp, remote_dir):
    """Recursively count all files in a remote directory."""
    total_files = 0
    try:
        ftp.cwd(remote_dir)
    except error_perm as e:
        print(f"Failed to access directory {remote_dir}: {e}")
        return 0  # Return 0 if the directory is inaccessible

    remote_items = list_directory_with_details(ftp)
    for item in remote_items.keys():
        if is_directory(ftp, item):
            total_files += count_files(ftp, item)  # Recursively count files in subdirectories
        else:
            total_files += 1  # Count the file itself

    ftp.cwd("..")
    return total_files


def download_directory(ftp, remote_dir, local_dir, merged_dir, verbose, progress, pbar=None, total_files=None):
    """Download files from the remote FTP directory to a local directory only if they differ."""
    os.makedirs(local_dir, exist_ok=True)

    try:
        ftp.cwd(remote_dir)
    except error_perm as e:
        print(f"Failed to change directory to {remote_dir}: {e}")
        return  # Skip this directory if inaccessible

    remote_items = list_directory_with_details(ftp)

    # If progress bar is not initialized, calculate total files and initialize it
    if pbar is None and total_files is None:
        total_files = sum(1 for item in remote_items.values() if not is_directory(item))
        pbar = tqdm(total=total_files, disable=not progress, desc="Downloading all files")

    for item, details in remote_items.items():
        local_path = os.path.join(local_dir, item)
        merged_path = os.path.join(merged_dir, item)

        if is_directory(details):
            if verbose:
                print(f"Entering directory: {item}")
            download_directory(ftp, item, local_path, merged_path, verbose, progress, pbar, total_files)
        else:
            ftp_mtime = details["mtime"]
            if os.path.exists(merged_path) and os.path.getmtime(merged_path) == ftp_mtime:
                if verbose:
                    print(f"Skipping file: {item} (identical to merged directory)")
                continue

            if verbose:
                print(f"Downloading file: {item}")
            try:
                with open(local_path, 'wb') as f:
                    ftp.retrbinary(f"RETR {item}", f.write)

                # Update the timestamp to match the FTP server's file
                os.utime(local_path, (ftp_mtime, ftp_mtime))
            except error_perm as e:
                print(f"Failed to download file {item}: {e}")

            pbar.update(1)

    ftp.cwd("..")

    # Close progress bar only at the topmost level
    if pbar is not None and remote_dir == ftp.pwd():
        pbar.close()


class TimeoutException(Exception):
    """Custom exception for signaling a timeout."""
    pass


def timeout_handler(signum, frame):
    """Handler function to raise a timeout exception."""
    raise TimeoutException("Operation timed out")


def upload_directory(ftp, local_dir, remote_dir, verbose, progress, pbar=None):
    """Upload files to the remote FTP directory only if they differ."""
    try:
        ftp.cwd("/")
        ftp.cwd(remote_dir)
    except error_perm:
        if verbose:
            print(f"Creating remote directory: {remote_dir}")
        try:
            ftp.mkd(remote_dir)
            ftp.cwd(remote_dir)
        except error_perm as e:
            print(f"Failed to create or access remote directory {remote_dir}: {e}")
            return  # Skip this directory if it cannot be created

    local_items = os.listdir(local_dir)

    # Initialize progress bar if not already initialized
    if pbar is None:
        pbar = tqdm(total=len(local_items), disable=not progress, desc=f"Uploading to {remote_dir}")

    # Get remote file details
    remote_files = list_directory_with_details(ftp)

    for item in local_items:
        local_path = os.path.join(local_dir, item)

        if os.path.isdir(local_path):
            if verbose:
                print(f"Creating and entering directory: {item}")
            upload_directory(ftp, local_path, os.path.join(remote_dir, item), verbose, progress, pbar)
        else:
            local_mtime = os.path.getmtime(local_path)
            remote_mtime = remote_files.get(item, {}).get("mtime", 0)

            # Skip uploading if the file is identical on the remote server
            if item in remote_files and local_mtime == remote_mtime:
                if verbose:
                    print(f"Skipping file: {item} (identical on remote server)")
                continue

            if verbose:
                print(f"Uploading file: {item} (local is newer)")
            try:
                with open(local_path, 'rb') as f:
                    ftp.storbinary(f"STOR {item}", f)
                ftp.voidcmd(f"MFMT {time.strftime('%Y%m%d%H%M%S', time.gmtime(local_mtime))} {item}")
            except error_perm as e:
                print(f"Failed to upload file {item}: {e}")

        pbar.update(1)

    if pbar is not None and remote_dir == ftp.pwd():
        pbar.close()


def merge_directories(source, target, progress):
    """Merge source directory into the target directory."""
    files = {os.path.join(root, file) for root, _, files in os.walk(source) for file in files}
    pbar = tqdm(total=len(files), disable=not progress, desc="Merging files")

    for root, _, files in os.walk(source):
        relative_root = os.path.relpath(root, source)
        target_root = os.path.join(target, relative_root)
        os.makedirs(target_root, exist_ok=True)

        for file in files:
            source_path = os.path.join(root, file)
            target_path = os.path.join(target_root, file)

            if not os.path.exists(target_path) or os.path.getmtime(source_path) > os.path.getmtime(target_path):
                shutil.copy2(source_path, target_path)
            pbar.update(1)

    pbar.close()


def ftp_connect(server, port):
    """Establish a connection to an FTP server."""
    ftp = FTP()
    ftp.connect(server, port)
    ftp.login()
    return ftp


def main():
    parser = argparse.ArgumentParser(description="Sync and merge FTP directories.")
    parser.add_argument("ftp_servers", nargs="+", help="IP addresses of the FTP servers (one or two)")
    parser.add_argument("merged_folder", help="Path to the permanent merged folder")
    parser.add_argument("-v", "--verbose", action="store_true", help="Enable verbose output")
    parser.add_argument("-p", "--progress", action="store_true", help="Show progress bar")

    args = parser.parse_args()

    if len(args.ftp_servers) not in [1, 2]:
        print("Error: Provide one or two FTP server IPs.")
        return

    ftp_port = 1337
    remote_dir = "ux0:/user/00/savedata"
    temp_dir_1 = "/tmp/ftp_sync_server1"
    temp_dir_2 = "/tmp/ftp_sync_server2"
    merged_folder = args.merged_folder

    if len(args.ftp_servers) == 1:
        ftp_server = args.ftp_servers[0]
        with ftp_connect(ftp_server, ftp_port) as ftp:
            if args.verbose:
                print(f"Connected to {ftp_server}")
            download_directory(ftp, remote_dir, temp_dir_1, merged_folder, args.verbose, args.progress)
            merge_directories(temp_dir_1, merged_folder, args.progress)
            upload_directory(ftp, merged_folder, remote_dir, args.verbose, args.progress)

    elif len(args.ftp_servers) == 2:
        ftp_server_1, ftp_server_2 = args.ftp_servers
        with ftp_connect(ftp_server_1, ftp_port) as ftp1:
            if args.verbose:
                print(f"Connected to {ftp_server_1}")
            download_directory(ftp1, remote_dir, temp_dir_1, merged_folder, args.verbose, args.progress)

        with ftp_connect(ftp_server_2, ftp_port) as ftp2:
            if args.verbose:
                print(f"Connected to {ftp_server_2}")
            download_directory(ftp2, remote_dir, temp_dir_2, merged_folder, args.verbose, args.progress)

        merge_directories(temp_dir_1, merged_folder, args.progress)
        merge_directories(temp_dir_2, merged_folder, args.progress)

        with ftp_connect(ftp_server_1, ftp_port) as ftp1:
            upload_directory(ftp1, merged_folder, remote_dir, args.verbose, args.progress)

        with ftp_connect(ftp_server_2, ftp_port) as ftp2:
            upload_directory(ftp2, merged_folder, remote_dir, args.verbose, args.progress)

    print("Synchronization complete.")

    shutil.rmtree(temp_dir_1, ignore_errors=True)
    shutil.rmtree(temp_dir_2, ignore_errors=True)


if __name__ == "__main__":
    main()
