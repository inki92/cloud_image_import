import os
import tarfile
import subprocess
from datetime import datetime
import json
import time
import argparse
import lzma

class AWSImageCreator:
    """
    Class for prepare, upload and
    import .tar or .raw to AWS EC2.
    """

    def __init__(self, aws_cli_path='aws',
                 path_to_images='cloud_images_val_shared/image/'):
        self.aws_cli_path = aws_cli_path
        self.path_to_images = path_to_images

    def unpack(self, tar_file_path):
        """
        Method for unpack .tar with logs from image composer.
        Also unpack image.xz from this .tar.
        :param tar_file_path: path to .tar file from image_composer.
        :return: path/name to unpacked .raw image from .tar.
        """
        try:
            with tarfile.open(tar_file_path, 'r') as tar:
                tar.extractall(path=self.path_to_images)
                for member in tar.getmembers():
                    if member.name.endswith('.xz'):
                        image_xz = self.path_to_images + member.name
                        unxz_image = os.path.splitext(image_xz)[0]
                        with lzma.open(image_xz, 'rb') as compressed_file:
                            with open(unxz_image, 'wb') as output:
                                output.write(compressed_file.read())
            files = os.listdir(self.path_to_images)
            for file in files:
                if file.endswith('.raw'):
                    image = self.path_to_images + file
                    return image

        except Exception as e:
            print("Unpacking error:", e)


    def upload_to_s3(self, local_file_path,
                     s3_bucket_name,
                     region):
        """
        Method for upload .raw image to S3 bucket.
        :param local_file_path: path to .raw image from image composer
        :param s3_bucket_name: name of S3 bucket from AWS
        :param region: supported region
        :return: name of the uploaded s3 object
        """
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        s3_object_name_orig = os.path.basename(local_file_path)
        s3_object_name = f"{os.path.splitext(s3_object_name_orig)[0]}_{timestamp}.raw"
        subprocess.run([
            self.aws_cli_path,
            's3', 'cp', local_file_path,
            f's3://{s3_bucket_name}/{s3_object_name}',
            '--region', region
        ])
        return s3_object_name

    def import_snapshot_and_create_ami(self, s3_object_name,
                                       s3_bucket_name,
                                       boot_mode,
                                       delete_after_import,
                                       region):
        """
        Method for importing S3 object as snapshot
        and importing this snapshot as an AMI image.
        :param s3_object_name: name from upload_to_s3 method
        :param s3_bucket_name: name of the AWS S3 bucket
        :param boot_mode: bios or uefi for creation AMI
        :param delete_after_import: delete or not S3 object after importing
        :param region: region from AWS account
        :return: AMI ID for cloud-image-val resources.json
        """
        # Import the raw file as a snapshot
        import_response = subprocess.run([
            self.aws_cli_path, 'ec2', 'import-snapshot',
            '--disk-container',
            f'Format=raw,UserBucket={{S3Bucket={s3_bucket_name},S3Key={s3_object_name}}}',
            '--description', 'Imported snapshot from raw file',
            '--region', region
        ], capture_output=True, text=True)

        import_task = import_response.stdout.strip()
        import_task_json = json.loads(import_task)
        import_task_id = import_task_json.get('ImportTaskId')

        # Wait for the import task to complete
        while True:
            status_response = subprocess.run([
                self.aws_cli_path, 'ec2', 'describe-import-snapshot-tasks',
                '--import-task-ids', import_task_id,
                '--region', region
            ], capture_output=True, text=True)

            status_response = status_response.stdout.strip()
            status_response_json = json.loads(status_response)
            status = (
                status_response_json)['ImportSnapshotTasks'][0]['SnapshotTaskDetail']['Status']

            if status == 'completed':
                break
            elif status == 'error':
                print('Error: Import task failed. Exiting.')
                return
            else:
                print(f'Import task status: {status}. Waiting...')
                try:
                    progress = (
                        status_response_json)['ImportSnapshotTasks'][0]['SnapshotTaskDetail']['Progress']
                    print(f'Current progress: {progress}')
                except:
                    print('No more progress')
                time.sleep(10)  # wait for 10 seconds before checking again

        snapshot_id = (
            status_response_json)['ImportSnapshotTasks'][0]['SnapshotTaskDetail']['SnapshotId']

        # Create an AMI from the snapshot with optional boot mode
        ami_creation_args = [
            self.aws_cli_path, 'ec2', 'register-image',
            '--name', s3_object_name,
            '--architecture', 'x86_64',
            '--block-device-mappings',
            f'DeviceName=/dev/sda1,Ebs={{SnapshotId={snapshot_id}}}',
            '--virtualization-type', 'hvm',
            '--root-device-name', '/dev/sda1',
            '--region', region,
            '--ena-support'
        ]

        if boot_mode:
            ami_creation_args.extend(['--boot-mode', boot_mode])

        create_ami_response = (
            subprocess.run(ami_creation_args, capture_output=True, text=True))

        if 'already in use' in str(create_ami_response):
            print('This snapshot is already in use:')
            print(create_ami_response)
        else:
            ami_id = create_ami_response.stdout.strip()
            print(f'AMI ID: {ami_id}')

        # Delete S3 object if specified by the user or using the --delete option
        if delete_after_import:
            self.delete_s3_object(s3_bucket_name, s3_object_name, region)
        else:
            self.prompt_delete_s3_object(s3_bucket_name, s3_object_name, region)

        return ami_id

    def delete_s3_object(self, s3_object_name, s3_bucket_name, region):
        """ Method for delete S3 object. """
        subprocess.run([
            self.aws_cli_path, 's3', 'rm',
            f's3://{s3_bucket_name}/{s3_object_name}', '--region', region])

    def prompt_delete_s3_object(self, s3_object_name, s3_bucket_name, region):
        """ Method for ask user about deleting. """
        while True:
            delete_input = (
                input(
                    f"Do you want to delete the S3 object '{s3_object_name}' after import as an AMI? (y/n): ").lower())

            if delete_input == 'y':
                self.delete_s3_object(s3_bucket_name, s3_object_name, region)
                print(f"S3 object '{s3_object_name}' deleted.")
                break
            elif delete_input == 'n':
                print(f"S3 object '{s3_object_name}' not deleted.")
                break
            else:
                print("Invalid input. Please enter 'y' for yes or 'n' for no.")


class AzureImageCreator:
    """
    Class for preparing, uploading
    and importing image to Azure.
    """

    def __init__(self, az_cli_path='az', path_to_images='cloud_images_val_shared/image/'):
        self.path_to_images = path_to_images
        self.az_cli_path = az_cli_path

    def unpack(self, tar_file_path):
        """
        Method for unpacking tar file from .tar from image composer,
        rename .vhdfixed.
        :param tar_file_path: path to .tar.
        :return: /path/image_name
        """
        try:
            with tarfile.open(tar_file_path, 'r') as tar:
                tar.extractall(path=self.path_to_images)
                for member in tar.getmembers():
                    if member.name.endswith('.xz'):
                        image_xz = self.path_to_images + member.name
                        unxz_image = os.path.splitext(image_xz)[0]
                        with lzma.open(image_xz, 'rb') as compressed_file:
                            with open(unxz_image, 'wb') as output:
                                output.write(compressed_file.read())
                files = os.listdir(self.path_to_images)
                for file in files:
                    if file.endswith('.vhdfixed'):
                        image_orig = self.path_to_images + file
                        renamed_image_name = file.replace(".vhdfixed", ".vhd")
                        image = self.path_to_images + renamed_image_name
                        os.rename(image_orig, image)
                        return image
        except Exception as e:
            print("Unpacking error:", e)

    def upload(self, local_file_path, container_name,
               storage_account_name):
        """
        Method for upload image from image composer
        as a blob to Azure.
        :param local_file_path: path to image
        :param container_name: name of Azure container
        :param storage_account_name: Azure storage account name
        :return: uploaded blob name
        """
        blob_name = os.path.basename(local_file_path)
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        blob_name_orig, file_extension = os.path.splitext(blob_name)
        blob_name = f"{blob_name_orig}_{timestamp}{file_extension}"
        subprocess.run([
            self.az_cli_path, 'storage', 'blob', 'upload',
            '--account-name', storage_account_name,
            '--file', local_file_path,
            '--container-name', container_name,
            '--name', blob_name
        ])
        return blob_name

    def create_image_from_blob(self,
                               blob_name,
                               delete_after_import,
                               region, resource_group,
                               container_name,
                               storage_account_name):
        """
        Method for import uploaded blob as
        an Azure image.
        :param blob_name: blob name from upload method
        :param delete_after_import: delete or not blob after import
        :param region: region of Azure account
        :param resource_group: resource group of Azure account
        :param container_name: Azure container(bucket)
        :param storage_account_name: Azure storage account
        :return: resource ID of the imported image
        """

        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        image_name = f"{blob_name}-{timestamp}"

        create_image_cmd = [
            self.az_cli_path, 'image', 'create',
            '--resource-group', resource_group,
            '--name', image_name,
            '--source', f'https://{storage_account_name}.blob.core.windows.net/{container_name}/{blob_name}',
            '--os-type', 'Linux',
            '--location', region
        ]

        create_image_response = subprocess.run(create_image_cmd, capture_output=True, text=True)
        if create_image_response.returncode != 0:
            print("Error creating the image:", create_image_response.stderr)
            return None

        image_info = json.loads(create_image_response.stdout)

        print('Resulting blob ID:')
        print(image_info['id'])

        if delete_after_import:
            self.delete(blob_name, container_name, storage_account_name)
        else:
            self.prompt_delete(blob_name, container_name, storage_account_name)

        return image_info['id']

    def delete(self, blob_name, container_name, storage_account_name):
        """ Method for delete uploaded blob. """
        subprocess.run([
            self.az_cli_path, 'storage', 'blob', 'delete',
            '--account-name', storage_account_name,
            '--container-name', container_name,
            '--name', blob_name
        ])

    def prompt_delete(self, blob_name,
                      container_name,
                      storage_account_name):
        """ Method for ask user about deleting. """
        while True:
            delete_input = input(
                f"Do you want to delete the S3 object '{blob_name}' after import as an AMI? (y/n): ").lower()
            if delete_input == 'y':
                self.delete(blob_name, container_name, storage_account_name)
                print(f"Bucket object '{blob_name}' deleted.")
                break
            elif delete_input == 'n':
                print(f"Bucket object '{blob_name}' not deleted.")
                break
            else:
                print("Invalid input. Please enter 'y' for yes or 'n' for no.")


class GCPImageCreator:
    """
    Class for preparing, upload and
    import image to Google Cloud Platform.
    """

    def __init__(self, gcp_cli_path='gcloud', gcp_util_path='gsutil',
                 path_to_images='cloud_images_val_shared/image/'):
        self.gcp_cli_path = gcp_cli_path
        self.gcp_util_path = gcp_util_path
        self.path_to_images = path_to_images

    def unpack(self, tar_file_path):
        """
        Method for unpack .tar from image composer .tar and get archive tar.gz
        :param tar_file_path: path to .tar file from image composer.
        :return: path/name to packed tar.gz archive
        """
        image = ''
        with tarfile.open(tar_file_path, 'r') as tar:
            tar.extractall(path=self.path_to_images)
            for member in tar.getmembers():
                if member.name.endswith('.tar.gz'):
                    image = self.path_to_images + member.name
        print(f"image_name:{image}")
        return image

    def upload_to_bucket(self, raw_archive_name, gcp_bucket_name):
        """
        Method for upload .raw.tar.gz archive to gcloud bucket.
        :param raw_archive_name: path/name to packed tar.gz archive
        :param gcp_bucket_name: name of gcloud bucket
        :return: name of the uploaded gcloud object
        """

        subprocess.run([
            self.gcp_util_path,
            '-m', 'cp', '-n', raw_archive_name,
            f'gs://{gcp_bucket_name}'
        ])
        return raw_archive_name

    def create_image(self,
                     gcloud_object_name,
                     gcp_bucket_name,
                     boot_mode,
                     delete_after_import):
        """
        Method for import uploaded archive as a Gcloud image.
        :param gcp_bucket_name: name of gcloud bucket
        :param gcloud_object_name: gcloud object name from upload method
        :param boot_mode: bios or uefi
        :param delete_after_import: delete or not tar.gz archive after import

        :return: resource name of the imported image
        """
        basename_image = os.path.basename(gcloud_object_name)
        image_name = 'image-' + os.path.splitext(basename_image)[0].replace('-image.tar', '')

        if boot_mode == 'uefi':
            create_image_cmd = [
                self.gcp_cli_path, 'compute', 'images', 'create', image_name,
                '--source-uri', f'gs://{gcp_bucket_name}/{basename_image}',
                '--guest-os-features=UEFI_COMPATIBLE,VIRTIO_SCSI_MULTIQUEUE,SEV_CAPABLE'
            ]
        else:
            create_image_cmd = [
                self.gcp_cli_path, 'compute', 'images', 'create', image_name,
                '--source-uri', f'gs://{gcp_bucket_name}/{basename_image}'
            ]

        create_image_response = subprocess.run(create_image_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                               universal_newlines=True)

        if create_image_response.returncode != 0:
            print("Error creating the image:", create_image_response.stderr)
            return None

        create_image_stdout = create_image_response.stdout.split('\n')
        values = create_image_stdout[1].split()
        image_name = values[0]
        image_project = values[1]

        print(f'Image created: {image_name}')

        if delete_after_import:
            self.delete(basename_image, gcp_bucket_name)
        else:
            self.prompt_delete(basename_image, gcp_bucket_name)

        full_image_path = f'projects/{image_project}/global/images/{image_name}'

        return full_image_path

    def delete(self, gcloud_object_name, gcp_bucket_name):
        """ Method for deleting uploaded object. """
        subprocess.run([
            self.gcp_cli_path, 'storage', 'rm',
            f'gs://{gcp_bucket_name}/{gcloud_object_name}'
        ])

    def prompt_delete(self, gcloud_object_name, gcp_bucket_name):
        """ Method for asking user about deleting. """
        while True:
            delete_input = input(
                f"Do you want to delete the gcloud object '{gcloud_object_name}' after import? (y/n): ").lower()
            if delete_input == 'y':
                self.delete(gcloud_object_name, gcp_bucket_name)
                print(f"Google cloud object '{gcloud_object_name}' deleted.")
                break
            elif delete_input == 'n':
                print(f"Google cloud object '{gcloud_object_name}' not deleted.")
                break
            else:
                print("Invalid input. Please enter 'y' for yes or 'n' for no.")


def main():
    """ Main script with argparser. """
    parser = argparse.ArgumentParser(description='Image Creation Script')
    parser.add_argument('-p', '--path', help='Path to .tar file')
    parser.add_argument('-i', '--image', help='Path to raw file (bypasses unpacking and renaming)')
    parser.add_argument('-c', '--cloud', required=True, choices=['aws', 'gcp', 'azure'], help='Type of cloud (aws, gcp, or azure)')
    parser.add_argument('--boot', choices=['uefi', 'bios'], help='Type of boot (uefi or bios)')
    parser.add_argument('--bucket', help='bucket name for any cloud')
    parser.add_argument('--delete', action='store_true', help='Automatically delete S3 object after import as an AMI')
    parser.add_argument('--region', help='cloud account region')
    parser.add_argument('--resource_group', help='resource group for Azure or use default')
    parser.add_argument('--storage_account_name', help='storage account name for Azure or use default')
    args = parser.parse_args()

    if not args.path and not args.image:
        print('Either -p or -i option is required. Use --help for more information.')
        return

    if args.path and args.image:
        print('Both -p and -i options cannot be used together. Use --help for more information.')
        return

    # Import snapshot and create AMI
    boot_mode = args.boot if args.boot else None

    # AWS
    if args.cloud == 'aws':
        aws_image_creator = AWSImageCreator()

        if args.path:
            # Unpack and upload to S3
            tar_file_path = args.path
            raw_file_name = aws_image_creator.unpack(tar_file_path)
            s3_object_name = aws_image_creator.upload_to_s3(raw_file_name, args.bucket, args.region)
            print(f'Uploaded as: {s3_object_name}')

        elif args.image:
            # Upload the raw file to S3 (check for existing names and rename if necessary)
            raw_file_name = args.image
            s3_object_name = aws_image_creator.upload_to_s3(raw_file_name, args.bucket, args.region)
            print(f'Uploaded as: {s3_object_name}')

        ami_id = aws_image_creator.import_snapshot_and_create_ami(s3_object_name, args.bucket, boot_mode, args.delete, args.region)
        print(f'Resulting AMI ID: {ami_id}')

    # AZURE
    elif args.cloud == 'azure':

        azure_image_creator = AzureImageCreator()

        if args.path:
            # Unpack and upload to bucket
            tar_file_path = args.path
            raw_file_name = azure_image_creator.unpack(tar_file_path)
            blob_name = azure_image_creator.upload(raw_file_name, args.bucket, args.storage_account_name)
            print(f'Uploaded as: {blob_name}')

        elif args.image:
            raw_file_name = args.image
            blob_name = azure_image_creator.upload(raw_file_name, args.bucket, args.storage_account_name)
            print(f'Uploaded as: {blob_name}')

        azure_id = azure_image_creator.create_image_from_blob(blob_name, args.delete, args.region, args.resource_group, args.bucket, args.storage_account_name)

        print(f'Resulting blob ID: {azure_id}')

    # GCP
    elif args.cloud == 'gcp':

        gcp_image_creator = GCPImageCreator()

        if args.path:
            # Unpack and upload to GCP bucket
            tar_file_path = args.path
            raw_name = gcp_image_creator.unpack(tar_file_path)
            gloud_object_name = gcp_image_creator.upload_to_bucket(raw_name, args.bucket)
            print(f'Uploaded as: {gloud_object_name}')

        elif args.image:
            # Upload the raw archive file to GCP (check for existing names and rename if necessary)
            raw_name = args.image
            gloud_object_name = gcp_image_creator.upload_to_bucket(raw_name, args.bucket)
            print(f'Uploaded as: {gloud_object_name}')

        gcp_name = gcp_image_creator.create_image(gloud_object_name, args.bucket, boot_mode, args.delete)
        print(f'Resulting image name: {gcp_name}')

    else:
        print('Unsupported cloud. This script works only with AWS, Azure or GCP')
        return


if __name__ == "__main__":
    main()
