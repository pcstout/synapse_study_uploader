# Synapse Study Uploader

Upload a Large Amount of Files to Synapse

- Obeys Synapse's 10,000 file/folder limit.
- Groups files into Folders by filename.
- Extracts DICOM data into Synapse annotations.
- Uploads multiple files at once.
- Can create a synapseclient manifest file.

## Installation

- Install python3
- `pip install -r requirements.txt`

## Usage

```
usage: synapse_study_uploader.py [-h] [-r REMOTE_FOLDER_PATH] [-u USERNAME]
                                 [-p PASSWORD] [-d DEPTH] [-t THREADS] [-cmo]
                                 [-dr] [-v] [-l LOG_LEVEL]
                                 project-id local-folder-path

positional arguments:
  project-id            Synapse Project ID to upload to (e.g., syn123456789).
  local-folder-path     Path of the folder to upload.

optional arguments:
  -h, --help            show this help message and exit
  -r REMOTE_FOLDER_PATH, --remote-folder-path REMOTE_FOLDER_PATH
                        Folder to upload to in Synapse.
  -u USERNAME, --username USERNAME
                        Synapse username.
  -p PASSWORD, --password PASSWORD
                        Synapse password.
  -d DEPTH, --depth DEPTH
                        The maximum number of child folders or files under a
                        Synapse Project/Folder.
  -t THREADS, --threads THREADS
                        The number of threads to create for uploading files.
  -cmo, --create-manifest-only
                        Create a manifest file.
  -dr, --dry-run        Dry run only. Do not upload any folders or files.
  -v, --verbose         Print out additional processing information
  -l LOG_LEVEL, --log-level LOG_LEVEL
                        Set the logging level.
```