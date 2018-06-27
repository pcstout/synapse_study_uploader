#!/usr/bin/python

# Copyright 2017-present, Bill & Melinda Gates Foundation
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import sys, os, argparse, getpass, tempfile, shutil, psutil
import Queue, threading, time, signal
import synapseclient, pydicom
from synapseclient import Project, Folder, File
from datetime import datetime

class SynapseStudyUploader:

    # Maximum number of files per Project/Folder in Synapse.
    MAX_SYNAPSE_DEPTH = 10000

    # Default number of threads to create.
    DEFAULT_THREAD_COUNT = psutil.cpu_count()

    def __init__(self,
                 synapse_project,
                 local_path,
                 remote_path=None,
                 folder_depth=MAX_SYNAPSE_DEPTH,
                 thread_count=DEFAULT_THREAD_COUNT,
                 dry_run=False,
                 verbose=False,
                 username=None,
                 password=None):

        self._dry_run = dry_run
        self._verbose = verbose
        self._synapse_project = synapse_project
        self._local_path = os.path.abspath(local_path)
        self._remote_path = None
        self._folder_depth = folder_depth
        self._thread_count = thread_count
        self._synapse_folders = {}
        self._username = username
        self._password = password
        self._temp_dir = tempfile.gettempdir()

        self._files = []
        self._folders = []

        self._thread_lock = threading.Lock()
        self._work_queue = Queue.Queue()
        self._threads = []
        self._is_canceling = False

        if self._folder_depth > self.MAX_SYNAPSE_DEPTH:
            raise Exception('Maximum object depth cannot be more than {0}'.format(self.MAX_SYNAPSE_DEPTH))
    
        if remote_path != None and len(remote_path.strip()) > 0:
            self._remote_path = remote_path.strip().lstrip(os.sep).rstrip(os.sep)
            if len(self._remote_path) == 0:
                self._remote_path = None

        signal.signal(signal.SIGINT, self.on_sigint)


    def start(self):
        if self._dry_run:
            print('~~ Dry Run ~~')

        self.login()

        project = self._synapse_client.get(Project(id = self._synapse_project))
        self.set_synapse_folder(self._synapse_project, project)

        print('Uploading to Project: {0} ({1})'.format(project.name, project.id))
        print('Uploading Directory: {0}'.format(self._local_path))
        print('Uploading To: {0}'.format(os.path.join(self._synapse_project, (self._remote_path or ''))))
        print('Max Threads: {0}'.format(self._thread_count))
        
        print('Loading Files...')
        self.load_files()
        print('Total Synapse Folders to Create: {0}'.format(len(self._folders)))
        print('Total Files to Upload: {0}'.format(len(self._files)))

        self.start_threads()
        print('Total Threads: {0}'.format(len(self._threads)))

        self.create_remote_path()
        self.queue_file_uploads()
        self.wait_for_threads()
                    
        if self._dry_run:
            print('Dry Run Completed Successfully.')
        else:
            print('Upload Completed Successfully.')


    def login(self):
        print('Logging into Synapse...')
        self._username = os.getenv('SYNAPSE_USER') or self._username
        self._password = os.getenv('SYNAPSE_PASSWORD') or self._password

        if self._username == None:
            self._username = input('Synapse username: ')

        if self._password == None:
            self._password = getpass.getpass(prompt='Synapse password: ')
        
        self._synapse_client = synapseclient.login(self._username, self._password, silent=True)


    def create_remote_path(self):
        if self._remote_path != None:
            path = ''
            for folder in filter(None, self._remote_path.split(os.sep)):
                path = os.path.join(path, folder)
                self.create_folder_in_synapse(path)


    def start_threads(self):
        total_threads = self._thread_count
        total_files = len(self._files)

        if total_threads > total_files:
            total_threads = total_files

        for _ in xrange(total_threads):
            thread = UploadWorker(self)
            self._threads.append(thread)
            thread.start()


    def wait_for_threads(self):
        # Wait for the queue to empty.
        while not self._work_queue.empty(): time.sleep(.100)

        # Tell the threads to exit.
        for t in self._threads: t.exit()

        # Wait for the threads to finish
        for t in self._threads: t.join()


    def queue_file_uploads(self):
        total_folders = len(self._folders)

        folder_name_padding = len(str(total_folders))
        if folder_name_padding < 2: folder_name_padding = 2

        folder_num = 0

        for files in self._folders:
            folder_num += 1
            folder_path = (self._remote_path or '')

            if total_folders > 1:
                folder_name = str(folder_num).zfill(folder_name_padding)
                folder_path = os.path.join(folder_path, folder_name)
                self.create_folder_in_synapse(folder_path)

            self._thread_lock.acquire()
            
            for file_info in files:
                folder_obj = {
                    "folder_path": folder_path,
                    "file_info": file_info
                }
                self._work_queue.put(folder_obj)

            self._thread_lock.release()


    def load_files(self):
        # Find all the files and get the calculated file name and annotations.
        for dirpath, dirnames, filenames in os.walk(self._local_path):
            for filename in filenames:
                full_file_name = os.path.join(dirpath, filename)
                
                calc_filename, annotations = self.get_metadata(full_file_name)

                self._files.append({
                    "path": dirpath,
                    "name": filename,
                    "full_path": full_file_name,
                    "calc_name": calc_filename,
                    "annotations": annotations
                })
        
        # Group the files by name.
        groups = self.group_by(self._files, 'calc_name')
        
        # Unique the duplicate file names.
        for filename, files in groups.iteritems():
            if len(files) <= 1: continue

            counter = 0
            for file in files:
                counter += 1
                file['calc_name'] = '{0}_{1}'.format(counter, file['calc_name'])

        self._folders = list([self._files[i:i + self._folder_depth] for i in xrange(0, len(self._files), self._folder_depth)])


    def create_folder_in_synapse(self, path):
        log_line = 'Processing Folder: {0}'.format(path)
        
        full_synapse_path, synapse_parent, folder_name = self.to_synapse_path(path)
        
        log_line += '\n  -> {0}'.format(full_synapse_path)

        print (log_line)

        synapse_folder = Folder(folder_name, parent=synapse_parent)

        if self._dry_run:
            # Give the folder a fake id so it doesn't blow when this folder is used as a parent.
            synapse_folder.id = 'syn0'
        else:
            synapse_folder = self._synapse_client.store(synapse_folder, forceVersion=False)

        self.set_synapse_folder(full_synapse_path, synapse_folder)
        return synapse_folder


    STRING = 'str'
    INTEGER = 'int'
    DATE = 'date'


    DICOM_ANNOTATION_FIELDS = {
        "ContentDate": DATE
        ,"ContentTime": INTEGER
        ,"DeviceSerialNumber": STRING
        ,"InstanceNumber": INTEGER
        ,"InstitutionName": STRING
        ,"Manufacturer": STRING
        ,"Modality": STRING
        ,"PatientBirthDate": DATE
        ,"PatientID": STRING
        ,"PerformedProcedureStepID": STRING
        ,"PerformedProcedureStepStartDate": DATE
        ,"PerformedProcedureStepStartTime": STRING
        ,"SOPClassUID": STRING
        ,"SOPInstanceUID": STRING
        #,"SequenceOfUltrasoundRegions": STRING
        ,"SeriesDate": DATE
        ,"SeriesInstanceUID": STRING
        ,"SeriesNumber": INTEGER
        ,"SeriesTime": INTEGER
        ,"SoftwareVersions": STRING
        ,"StudyDate": DATE
        ,"StudyID": STRING
        ,"StudyInstanceUID": STRING
        ,"StudyTime": INTEGER
    }


    def get_metadata(self, local_file):
        filename = os.path.basename(local_file)
        annotations = {}

        if local_file.lower().endswith('.dcm'):
            ds = pydicom.dcmread(local_file)
            filename = '{0}_{1}_{2}'.format(ds.PatientID, ds.StudyDate, filename).replace('-', '_')

            for field_name, type in self.DICOM_ANNOTATION_FIELDS.iteritems():
                value = self.dicom_field_to_annotation_field(ds, field_name, type=type)
                if value != None:
                    annotations[field_name] = value
        else:
            None

        return filename, annotations


    def dicom_field_to_annotation_field(self, dataset, field_name, type=STRING):
        data_element = dataset.data_element(field_name)
        value = None

        if data_element == None:
            print('Field not found: {0}'.format(field_name))
        elif data_element.value != None:
            value = data_element.value
            try:
                if type == self.INTEGER:
                    value = int(value)
                elif type == self.DATE:
                    value = datetime.strptime(value, '%Y%M%d').date()
            except:
                print('Could not parse {0}: {1}'.format(type, value))
                
        return value


    def get_synapse_folder(self, synapse_path):
        return self._synapse_folders[synapse_path]


    def set_synapse_folder(self, synapse_path, parent):
        self._synapse_folders[synapse_path] = parent


    def to_synapse_path(self, *paths):
        all_paths = [self._synapse_project]

        all_paths += paths

        full_synapse_path = os.path.join(*all_paths)
        synapse_parent_path = os.path.dirname(full_synapse_path)
        synapse_parent = self.get_synapse_folder(synapse_parent_path)
        name = os.path.basename(full_synapse_path)

        return full_synapse_path, synapse_parent, name


    def group_by(self, items, prop):
        groups = {}
        for item in items:
            key = item[prop]
            if key not in groups: groups[key] = []
            groups[key].append(item)

        return groups


    def on_sigint(self, signum, frame):
        if self._is_canceling: return
        self._is_canceling = True
        print('Canceling...')
        for t in self._threads: t.exit()
        sys.exit(1)


class UploadWorker (threading.Thread):
    exit_thread = False

    def __init__(self, parent):
        super(UploadWorker, self).__init__()
        self._parent = parent
        self._lock = self._parent._thread_lock
        self._queue = self._parent._work_queue


    def run(self):
        self._synapse_client = synapseclient.login(self._parent._username, self._parent._password, silent=True)

        while not self.exit_thread:
            self._lock.acquire()
            if not self._queue.empty():
                folder_obj = self._queue.get()
                self._lock.release()
                self.upload_file_to_synapse(folder_obj["file_info"], folder_obj["folder_path"])
            else:
                self._lock.release()
            time.sleep(.100)


    def exit(self):
        self.exit_thread = True


    def upload_file_to_synapse(self, file_info, synapse_folder_path):
        filename = file_info['calc_name']
        full_file_name = file_info['full_path']
        annotations = file_info['annotations']

        temp_file = os.path.join(self._parent._temp_dir, filename)

        # Copy the file to a temp directory with its new name.
        shutil.copyfile(full_file_name, temp_file)

        full_synapse_path, synapse_parent, _ = self._parent.to_synapse_path(synapse_folder_path, filename)
        
        log_line = 'Processing File: {0}'.format(full_file_name)
        log_line += '\n  -> {0}'.format(full_synapse_path)

        if self._parent._verbose:
            for key, value in annotations.iteritems():
                log_line += '\n    -> {0}: {1}'.format(key, value)

        print(log_line)

        if not self._parent._dry_run:
            self._synapse_client.store(
                File(temp_file, parent=synapse_parent, annotations=annotations), forceVersion=False
            )

        # Delete the temp file.
        os.remove(temp_file)


def main(argv):
    parser = argparse.ArgumentParser()
    parser.add_argument('project_id', metavar='project-id', help='Synapse Project ID to upload to (e.g., syn123456789).')
    parser.add_argument('local_folder_path', metavar='local-folder-path', help='Path of the folder to upload.')
    parser.add_argument('-r', '--remote-folder-path', help='Folder to upload to in Synapse.', default=None)
    parser.add_argument('-u', '--username', help='Synapse username.', default=None)
    parser.add_argument('-p', '--password', help='Synapse password.', default=None)
    parser.add_argument('-d', '--depth', help='The maximum number of child folders or files under a Synapse Project/Folder.', type=int, default=SynapseStudyUploader.MAX_SYNAPSE_DEPTH)
    parser.add_argument('-t', '--threads', help='The number of threads to create for uploading files.', type=int, default=SynapseStudyUploader.DEFAULT_THREAD_COUNT)
    parser.add_argument('-dr', '--dry-run', help='Dry run only. Do not upload any folders or files.', default=False, action='store_true')
    parser.add_argument('-v', '--verbose', help='Print out additional processing information', default=False, action='store_true')

    args = parser.parse_args()
    
    SynapseStudyUploader(
        args.project_id
        ,args.local_folder_path
        ,remote_path=args.remote_folder_path
        ,folder_depth=args.depth
        ,thread_count=args.threads
        ,dry_run=args.dry_run
        ,verbose=args.verbose
        ,username=args.username
        ,password=args.password
        ).start()


if __name__ == "__main__":
    main(sys.argv[1:])
