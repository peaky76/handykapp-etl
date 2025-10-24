#!/usr/bin/env python3
"""
Interactive JSON Editor for Digital Ocean Spaces

Usage:
    python json_editor.py <file_path>

Example:
    python json_editor.py races/2024/race_001.json
"""

import json
import subprocess
import sys
import tempfile
from pathlib import Path

# Add src to path for imports
sys.path.append(str(Path(__file__).parent / "src"))

from clients import SpacesClient


def edit_json_interactively(file_path):
    """
    Download JSON file, open in editor, save back to Spaces
    """
    print(f"Loading JSON file: {file_path}")

    try:
        # Download the file
        data = SpacesClient.read_file(file_path)
        print("✓ File loaded successfully")

        # Create a temporary file
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".json", delete=False
        ) as tmp_file:
            json.dump(data, tmp_file, indent=2, ensure_ascii=False)
            tmp_path = tmp_file.name

        print(f"✓ Created temporary file: {tmp_path}")
        print("✓ Opening in editor...")

        # Open in user's default editor
        # This will work on Windows, Mac, and Linux
        try:
            # Try VSCode first - use shell=True to find it in PATH
            result = subprocess.run(
                ["code", "--wait", tmp_path], check=True, shell=True
            )
        except (subprocess.CalledProcessError, FileNotFoundError):
            try:
                # Try notepad on Windows
                result = subprocess.run(["notepad", tmp_path], check=True, shell=True)
            except (subprocess.CalledProcessError, FileNotFoundError):
                try:
                    # Try nano on Unix-like systems
                    result = subprocess.run(["nano", tmp_path], check=True, shell=True)
                except (subprocess.CalledProcessError, FileNotFoundError):
                    print(
                        "Could not find a suitable editor. Please edit the file manually:"
                    )
                    print(f"File path: {tmp_path}")
                    input("Press Enter when you've finished editing...")

        # Read the edited file - try different encodings
        edited_data = None
        for encoding in ["utf-8", "utf-8-sig", "latin-1", "cp1252"]:
            try:
                with open(tmp_path, encoding=encoding) as f:
                    edited_data = json.load(f)
                    print("✓ JSON validation passed")
                    break
            except (UnicodeDecodeError, json.JSONDecodeError):
                continue

        if edited_data is None:
            try:
                with open(tmp_path, encoding="utf-8") as f:
                    edited_data = json.load(f)
                    print("✓ JSON validation passed")
            except json.JSONDecodeError as e:
                print(f"✗ JSON validation failed: {e}")
                retry = input("Would you like to edit again? (y/n): ")
                if retry.lower() == "y":
                    return edit_json_interactively(file_path)
                print("✗ Aborting without saving changes")
                return False

        # Ask for confirmation
        print("\nChanges made:")
        print("=" * 50)

        # Simple diff - just show if data changed
        if edited_data != data:
            print("✓ File has been modified")

            # Show a preview of changes
            print(
                f"Original keys: {len(data.keys()) if isinstance(data, dict) else 'N/A'}"
            )
            print(
                f"New keys: {len(edited_data.keys()) if isinstance(edited_data, dict) else 'N/A'}"
            )

            confirm = input("\nSave changes to Digital Ocean Spaces? (y/n): ")
            if confirm.lower() == "y":
                # Save back to Spaces
                json_content = json.dumps(edited_data, indent=2, ensure_ascii=False)
                SpacesClient.write_file(json_content, file_path)
                print(f"✓ Changes saved to {file_path}")
                return True
            print("✗ Changes discarded")
            return False

        print("No changes made")
        return True

    except Exception as e:
        print(f"✗ Error: {e}")
        return False
    finally:
        # Clean up temp file
        try:
            Path(tmp_path).unlink()
            print("✓ Temporary file cleaned up")
        except Exception:
            pass


def list_json_files(prefix=""):
    """
    List available JSON files in the space
    """
    print(f"Listing JSON files with prefix: '{prefix}'")
    print("=" * 50)

    try:
        files = [f for f in SpacesClient.get_files(prefix) if f.endswith(".json")]

        if not files:
            print("No JSON files found")
            return

        for i, file in enumerate(files[:20], 1):  # Show first 20
            print(f"{i:2d}. {file}")

        if len(files) > 20:
            print(f"... and {len(files) - 20} more files")

    except Exception as e:
        print(f"✗ Error listing files: {e}")


def main():
    if len(sys.argv) < 2:
        print("Usage:")
        print("  python json_editor.py <file_path>          # Edit specific file")
        print("  python json_editor.py --list [prefix]      # List JSON files")
        print()
        print("Examples:")
        print("  python json_editor.py races/2024/race_001.json")
        print("  python json_editor.py --list races/2024/")
        sys.exit(1)

    if sys.argv[1] == "--list":
        prefix = sys.argv[2] if len(sys.argv) > 2 else ""
        list_json_files(prefix)
    else:
        file_path = sys.argv[1]
        success = edit_json_interactively(file_path)
        sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
