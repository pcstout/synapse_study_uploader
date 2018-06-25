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

import sys, os, argparse, getpass, tempfile, shutil
import synapseclient
import pydicom
from synapseclient import Project, Folder, File
from datetime import datetime

class SynapseStudyUploader:


    def __init__(self, synapse_project, local_path, remote_path=None, dry_run=False, verbose=False, username=None, password=None):
        self._dry_run = dry_run
        self._verbose = verbose
        self._synapse_project = synapse_project
        self._local_path = os.path.abspath(local_path)
        self._remote_path = None
        self._synapse_folders = {}
        self._username = username
        self._password = password
        self._temp_dir = tempfile.gettempdir()
    
        if remote_path != None and len(remote_path.strip()) > 0:
            self._remote_path = remote_path.strip().lstrip(os.sep).rstrip(os.sep)
            if len(self._remote_path) == 0:
                self._remote_path = None



    def start(self):
        if self._dry_run:
            print('~~ Dry Run ~~')

        self.login()

        project = self._synapse_client.get(Project(id = self._synapse_project))
        self.set_synapse_folder(self._synapse_project, project)

        print('Uploading to Project: {0} ({1})'.format(project.name, project.id))
        print('Uploading Directory: {0}'.format(self._local_path))

        if self._remote_path != None:
            print('Uploading To: {0}'.format(self._remote_path))
        
        # Create the remote_path if specified.
        if self._remote_path != None:
            full_path = ''
            for folder in filter(None, self._remote_path.split(os.sep)):
                full_path = os.path.join(full_path, folder)
                self.create_directory_in_synapse(full_path, virtual_path=True)

        # Create the folders and upload the files.
        for dirpath, dirnames, filenames in os.walk(self._local_path):
            
            for filename in filenames:
                full_file_name = os.path.join(dirpath, filename)
                self.upload_file_to_synapse(full_file_name)

        if self._dry_run:
            print('Dry Run Completed Successfully.')
        else:
            print('Upload Completed Successfully.')



    def get_synapse_folder(self, synapse_path):
        return self._synapse_folders[synapse_path]



    def set_synapse_folder(self, synapse_path, parent):
        self._synapse_folders[synapse_path] = parent



    def login(self):
        print('Logging into Synapse...')
        syn_user = os.getenv('SYNAPSE_USER') or self._username
        syn_pass = os.getenv('SYNAPSE_PASSWORD') or self._password

        if syn_user == None:
            syn_user = input('Synapse username: ')

        if syn_pass == None:
            syn_pass = getpass.getpass(prompt='Synapse password: ')
        
        self._synapse_client = synapseclient.Synapse()
        self._synapse_client.login(syn_user, syn_pass, silent=True)



    def get_synapse_path(self, local_path, virtual_path=False):
        if virtual_path:
            return os.path.join(self._synapse_project, local_path)
        else:
            return os.path.join(self._synapse_project
                                ,(self._remote_path if self._remote_path else '')
                                ,local_path.split(os.sep)[-1]
                                )



    def create_directory_in_synapse(self, path, virtual_path=False):
        print('Processing Folder: {0}'.format(path))
        
        full_synapse_path = self.get_synapse_path(path, virtual_path)
        synapse_parent_path = os.path.dirname(full_synapse_path)
        synapse_parent = self.get_synapse_folder(synapse_parent_path)
        folder_name = os.path.basename(full_synapse_path)

        print('  -> {0}'.format(full_synapse_path))

        synapse_folder = Folder(folder_name, parent=synapse_parent)

        if self._dry_run:
            # Give the folder a fake id so it doesn't blow when this folder is used as a parent.
            synapse_folder.id = 'syn0'
        else:
            synapse_folder = self._synapse_client.store(synapse_folder, forceVersion=False)

        self.set_synapse_folder(full_synapse_path, synapse_folder)



    def upload_file_to_synapse(self, local_file):
        print('Processing File: {0}'.format(local_file))

        filename, annotations = self.get_metadata(local_file)

        temp_file = os.path.join(self._temp_dir, filename)

        # Copy the file to a temp directory with its new name.
        shutil.copyfile(local_file, temp_file)

        full_synapse_path = self.get_synapse_path(temp_file)
        synapse_parent_path = os.path.dirname(full_synapse_path)
        synapse_parent = self.get_synapse_folder(synapse_parent_path)
        
        print('  -> {0}'.format(full_synapse_path))

        if self._verbose:
            for key, value in annotations.iteritems():
                print('    -> {0}: {1}'.format(key, value))

        if not self._dry_run:
            self._synapse_client.store(
                File(temp_file, parent=synapse_parent, annotations=annotations), forceVersion=False
            )

        # Delete the temp file.
        os.remove(temp_file)



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



def main(argv):
    parser = argparse.ArgumentParser()
    parser.add_argument('project_id', metavar='project-id', help='Synapse Project ID to upload to (e.g., syn123456789).')
    parser.add_argument('local_folder_path', metavar='local-folder-path', help='Path of the folder to upload.')
    parser.add_argument('-r', '--remote-folder-path', help='Folder to upload to in Synapse.', default=None)
    parser.add_argument('-u', '--username', help='Synapse username.', default=None)
    parser.add_argument('-p', '--password', help='Synapse password.', default=None)
    parser.add_argument('-d', '--dry-run', help='Dry run only. Do not upload any folders or files.', default=False, action='store_true')
    parser.add_argument('-v', '--verbose', help='Print out additional processing information', default=False, action='store_true')

    args = parser.parse_args()
    
    SynapseStudyUploader(
        args.project_id
        ,args.local_folder_path
        ,remote_path=args.remote_folder_path
        ,dry_run=args.dry_run
        ,verbose=args.verbose
        ,username=args.username
        ,password=args.password
        ).start()



if __name__ == "__main__":
    main(sys.argv[1:])
