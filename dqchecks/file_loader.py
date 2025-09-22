"""
file_loader.py

This module provides the `FileLoader` class for loading data files
(template or bronze) from a specified directory structure. It supports
filtering based on metadata embedded in file names, validation of
expected file counts, MD5 checksum calculation, and logging of file metadata.

Typical directory structure:
- /Files/templates
- /Files/data collections

Files are filtered by values such as organisation_cd, process_cd, submission_period_cd, and status.

"""

import os
import glob
import datetime
import logging
import hashlib
from dataclasses import dataclass

TEMPLATE_KEYS = {"submission_period_cd", "process_cd"}
BRONZE_KEYS = {"submission_period_cd", "process_cd", "status"}

@dataclass(frozen=True)
class FileMetadata:
    """
    Data structure to store metadata about a file.

    Attributes:
        path (str): Full path to the file.
        filename (str): Name of the file.
        last_modified (datetime): Last modified timestamp.
        md5_hash (str | None): MD5 hash of the file contents.
        template_version (str | None): Version identifier, typically from template filename.
        template_path (str | None): Absolute path to the template.
    """
    path: str
    filename: str
    last_modified: datetime.datetime
    md5_hash: str | None
    template_version: str | None = None
    template_path: str | None = None

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

class FileLoader:
    # pylint: disable=C0301
    """
    Loads and filters data files (template or bronze) from a directory structure
    based on filename metadata.

    Attributes:
        source_data_path (str): Base directory containing "Files/templates" and "Files/data collections".
        load_template (bool): Whether to load template or bronze file.
        one_company_one_template (bool): If True, limits templates to those matching the organisation_cd.
        strict (bool): Enforces exact one-file match requirement.
        filters (dict): Dictionary of filter criteria like process_cd, organisation_cd, etc.
    """

    def __init__(self,
                 source_data_path: str,
                 load_template: bool,
                 one_company_one_template: bool = False,
                 strict: bool = True,
                 **filters: str):
        # pylint: disable=C0301
        """
        Initializes the FileLoader instance.

        Args:
            source_data_path (str): Path to the root directory containing files.
            load_template (bool): Load template if True, otherwise bronze.
            one_company_one_template (bool): If True, restrict template filtering to organisation_cd.
            strict (bool): If True, expects exactly one matching file or raises error.
            filters (dict): Filtering parameters like organisation_cd, process_cd, etc.
        """
        self.source_data_path = source_data_path
        self.load_template = load_template
        self.one_company_one_template = one_company_one_template
        self.strict = strict

        self.filters = {
            k: v.lower().strip()
            for k, v in filters.items()
            if v is not None
        }

        self.organisation_cd = self.filters.get("organisation_cd", "")
        self.process_cd = self.filters.get("process_cd", "")

    def match_all_conditions(self, filename: str, conditions: dict) -> bool:
        """
        Checks if all key=value filters are present in the filename.

        Args:
            filename (str): The filename to check.
            conditions (dict): Filter key-value pairs.

        Returns:
            bool: True if all conditions are matched, False otherwise.
        """
        filename_lower = filename.lower()
        # pylint: disable=C0301
        return all(f"{key.lower()}={value.lower()}" in filename_lower for key, value in conditions.items())

    def log_file_stats(self, file_list, label):
        """
        Logs size and modification time for a list of files.

        Args:
            file_list (list[str]): List of file paths.
            label (str): Description label for logging.
        """
        logging.info("%s (%d files):", label, len(file_list))
        for f in file_list:
            try:
                stat = os.stat(f)
                size_mb = stat.st_size / (1024 * 1024)
                last_modified = datetime.datetime.fromtimestamp(stat.st_mtime)
                # pylint: disable=C0301
                logging.info("- %s | Size: %.2f MB | Last Modified: %s", os.path.basename(f), size_mb, last_modified)
            # pylint: disable=W0718
            except Exception as e:
                logging.warning("- %s | Could not get file stats: %s", os.path.basename(f), e)

    def find_files(self, base_path, pattern="*.xlsx"):
        """
        Finds all files matching the pattern recursively under the base path.

        Args:
            base_path (str): Directory to search in.
            pattern (str): Glob pattern to match (default: '*.xlsx').

        Returns:
            list[str]: List of matching file paths.
        """
        search_path = os.path.join(base_path, "**", pattern)
        return glob.glob(search_path, recursive=True)

    def filter_files_by_org(self, files, restrict_to_org=False):
        """
        Filters files to include only those that contain the organisation code.

        Args:
            files (list[str]): List of file paths.
            restrict_to_org (bool): If True, apply filtering.

        Returns:
            list[str]: Filtered list of file paths.
        """
        if restrict_to_org and self.organisation_cd:
            return [f for f in files if self.organisation_cd in os.path.basename(f).lower()]
        return files

    def filter_files_by_prefix(self, files):
        """
        Filters files where filename starts with the organisation code.

        Args:
            files (list[str]): List of file paths.

        Returns:
            list[str]: Filtered list or all files if no organisation_cd set.
        """
        if self.organisation_cd:
            return [
                f for f in files
                    if os.path.basename(f).lower().startswith(self.organisation_cd)]
        return files

    def filter_files_by_conditions(self, files, conditions):
        """
        Applies filtering based on filename content and given conditions.

        Args:
            files (list[str]): List of file paths.
            conditions (dict): Filtering conditions.

        Returns:
            list[str]: Files that meet all conditions.
        """
        if not conditions:
            return files
        return [f for f in files if self.match_all_conditions(f, conditions)]

    def validate_file_count(self, files, expected_count, file_type):
        """
        Validates the number of files found against the expected count.

        Args:
            files (list[str]): List of file paths.
            expected_count (int): Number of files expected.
            file_type (str): Type of file (e.g., "template", "bronze").

        Raises:
            ValueError: If number of files is not equal to expected.
        """
        if len(files) != expected_count:
            raise ValueError(
                f"❌ Expected exactly {expected_count} {file_type} file(s)"
                # pylint: disable=C0301
                f"{f' for [{self.organisation_cd}]' if self.organisation_cd else ''}, found {len(files)}.\n"
                f"Files found:\n" + "\n".join(files)
            )

    def calculate_md5(self, filepath, chunk_size=8192):
        """
        Calculates the MD5 checksum of a file.

        Args:
            filepath (str): Path to the file.
            chunk_size (int): Size of each chunk read (default 8192 bytes).

        Returns:
            str | None: MD5 hash, or None if an error occurred.
        """
        hash_md5 = hashlib.md5()
        try:
            with open(filepath, "rb") as f:
                for chunk in iter(lambda: f.read(chunk_size), b""):
                    hash_md5.update(chunk)
            return hash_md5.hexdigest()
        # pylint: disable=W0718
        except Exception as e:
            logging.warning("Could not calculate MD5 for %s: %s", filepath, e)
            return None

    def load_file_info(self, filepath):
        """
        Loads metadata for a given file.

        Args:
            filepath (str): Path to the file.

        Returns:
            tuple: (filename, last_modified datetime, md5_hash)
        """
        filename = os.path.basename(filepath)
        stat = os.stat(filepath)
        last_modified = datetime.datetime.fromtimestamp(stat.st_mtime)
        md5_hash = self.calculate_md5(filepath)
        return filename, last_modified, md5_hash

    def run(self) -> FileMetadata:
        """
        Main method to load the matching file (template or bronze),
        validate it, extract metadata, and return it.

        Returns:
            FileMetadata: Object containing file metadata.

        Raises:
            FileNotFoundError: If no matching file found.
            ValueError: If strict mode and unexpected file count found.
        """
        restrict_template_to_org = self.one_company_one_template

        # Define template and data paths
        template_base = os.path.join(self.source_data_path, "Files", "templates")
        data_base = os.path.join(self.source_data_path, "Files", "data collections")

        # Find all matching files
        all_templates = self.find_files(template_base)
        all_templates = self.filter_files_by_org(
            all_templates,
            restrict_to_org=restrict_template_to_org)

        all_bronze = self.find_files(data_base)
        all_bronze = self.filter_files_by_prefix(all_bronze)

        # Filter files by supplied conditions
        template_conditions = {k: v for k, v in self.filters.items() if k in TEMPLATE_KEYS}
        bronze_conditions = {k: v for k, v in self.filters.items() if k in BRONZE_KEYS}

        if not template_conditions:
            logging.warning("⚠️ No filters applied to template file search.")
        if not bronze_conditions:
            logging.warning("⚠️ No filters applied to bronze file search.")

        matched_templates = self.filter_files_by_conditions(all_templates, template_conditions)
        matched_bronze = self.filter_files_by_conditions(all_bronze, bronze_conditions)

        if len(matched_templates) > 1 and not self.strict:
            logging.warning("Multiple matching templates found; using the first one.")

        # Log matched files
        self.log_file_stats(matched_templates, "Matched Template Files")
        self.log_file_stats(matched_bronze, "Matched Bronze Files")

        # Raise early if nothing found
        if self.load_template:
            if not matched_templates:
                raise FileNotFoundError("No matching template files found.")
        else:
            if not matched_bronze:
                raise FileNotFoundError("No matching bronze files found.")

        # Enforce strict count
        if self.strict:
            self.validate_file_count(matched_templates, 1, "template")
            if not self.load_template:
                self.validate_file_count(matched_bronze, 1, "bronze")

        # Final file selection and metadata
        if self.load_template:
            chosen_file = matched_templates[0]
            filename, last_modified, md5_hash = self.load_file_info(chosen_file)
            template_version = filename
        else:
            chosen_file = matched_bronze[0]
            filename, last_modified, md5_hash = self.load_file_info(chosen_file)
            if matched_templates:
                template_version = os.path.basename(matched_templates[0])
            else:
                raise FileNotFoundError("No matching template files found for the bronze file.")

        # Log final selection
        logging.info("\nLoaded file: %s", chosen_file)
        logging.info("Template Version: %s", template_version)
        logging.info("Last Modified: %s", last_modified)
        logging.info("MD5 Hash: %s", md5_hash if md5_hash else "Unavailable")


        return FileMetadata(
            path=chosen_file,
            filename=filename,
            last_modified=last_modified,
            md5_hash=md5_hash,
            template_version=template_version,
            template_path = matched_templates[0],
        )
