"""Unit tests for file_utils module."""
import os
import tempfile
import unittest
from pathlib import Path

from utils.file_utils import (
    get_file_type,
    is_supported_file_type,
    list_files_with_extensions,
    read_file_as_text,
    get_file_size,
)


class TestFileUtils(unittest.TestCase):
    """Test cases for file_utils module."""

    def test_get_file_type(self):
        """Test get_file_type function."""
        self.assertEqual(get_file_type("test.csv"), "CSV")
        self.assertEqual(get_file_type("test.json"), "JSON")
        self.assertEqual(get_file_type("test.xlsx"), "Excel")
        self.assertEqual(get_file_type("test.xls"), "Excel")
        self.assertEqual(get_file_type("test.txt"), "Text")
        self.assertEqual(get_file_type("test.md"), "Markdown")
        self.assertEqual(get_file_type("test.pdf"), "PDF")
        self.assertEqual(get_file_type("test.unknown"), "Unknown")

    def test_is_supported_file_type(self):
        """Test is_supported_file_type function."""
        self.assertTrue(is_supported_file_type("test.csv"))
        self.assertTrue(is_supported_file_type("test.json"))
        self.assertTrue(is_supported_file_type("test.xlsx"))
        self.assertTrue(is_supported_file_type("test.xls"))
        self.assertTrue(is_supported_file_type("test.txt"))
        self.assertTrue(is_supported_file_type("test.md"))
        self.assertTrue(is_supported_file_type("test.pdf"))
        self.assertFalse(is_supported_file_type("test.unknown"))

    def test_list_files_with_extensions(self):
        """Test list_files_with_extensions function."""
        with tempfile.TemporaryDirectory() as tmp_dir:
            # Create some test files
            for ext in [".csv", ".json", ".txt"]:
                with open(os.path.join(tmp_dir, f"test{ext}"), "w") as f:
                    f.write("test content")

            # Test with default extensions
            files = list_files_with_extensions(tmp_dir)
            self.assertEqual(len(files), 3)
            self.assertTrue(any(f.endswith("test.csv") for f in files))
            self.assertTrue(any(f.endswith("test.json") for f in files))
            self.assertTrue(any(f.endswith("test.txt") for f in files))

            # Test with specific extension
            files = list_files_with_extensions(tmp_dir, ["csv"])
            self.assertEqual(len(files), 1)
            self.assertTrue(files[0].endswith("test.csv"))

            # Test with non-existent directory
            non_existent_dir = os.path.join(tmp_dir, "non_existent")
            files = list_files_with_extensions(non_existent_dir)
            self.assertEqual(files, [])

    def test_read_file_as_text(self):
        """Test read_file_as_text function."""
        with tempfile.NamedTemporaryFile(mode="w", delete=False) as tmp_file:
            tmp_file.write("test content")
            tmp_file_path = tmp_file.name

        try:
            content = read_file_as_text(tmp_file_path)
            self.assertEqual(content, "test content")

            # Test with non-existent file
            content = read_file_as_text("non_existent_file.txt")
            self.assertIsNone(content)
        finally:
            os.unlink(tmp_file_path)

    def test_get_file_size(self):
        """Test get_file_size function."""
        with tempfile.NamedTemporaryFile(mode="w", delete=False) as tmp_file:
            tmp_file.write("test content")
            tmp_file_path = tmp_file.name

        try:
            size = get_file_size(tmp_file_path)
            self.assertEqual(size, len("test content"))

            # Test with non-existent file
            size = get_file_size("non_existent_file.txt")
            self.assertEqual(size, 0)
        finally:
            os.unlink(tmp_file_path)


if __name__ == "__main__":
    unittest.main() 