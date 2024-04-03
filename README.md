# cloud_image_import
Script for importing custom cloud images to AWS, Azure or GCP cloud providers.

## Description

This project is utilized for importing custom system images, created by an image composer utility or manually, to various cloud platforms. 

## Supported cloud providers

Supported platforms include AWS, Azure, and GCP.

## Supported Formats

### If images created by image composer utility:
- **AWS**: Compressed image in `disk.raw.xz` format within a tar archive obtained from the image composer.
- **Azure**: Compressed image in `disk.vhdfixed.xz` format within a tar archive.
- **GCP**: Compressed image in `disk.raw.tar.gz` format within a tar archive.

### If images created manually:

- **AWS**:  `disk.raw`/`disk.ami`
- **Azure**: `disk.vhd`
- **GCP**: `disk.raw.tar.gz`
- 
## Requirements
- Installed and authenticated client specific to the required cloud platform:
  - AWS CLI for AWS (`aws cli`)
  - Azure CLI for Azure (`az cli`)
  - Google Cloud CLI for GCP (`gcloud cli`)

## Usage
```bash
python3 image_import.py -p <path_to_image_archive> -c <cloud_provider> [--additional_options]
```

### Options
- `-p`, `--path`: Path to the image archive.
- `-c`, `--cloud`: Cloud provider (`aws`, `azure`, `gcp`).
- `--bucket`: Azure storage account blob container.
- `--region`: Azure region.
- `--resource_group`: Azure resource group.
- `--storage_account_name`: Azure storage account name.

### Example

Importing a custom Azure image from a .tar archive obtained from the image composer:
```bash
python3 image_import.py -p cloud_images/image/custom_azure_image.tar -c azure --bucket your_azure_storage --region your_azure_region --resource_group your_resource_group --storage_account_name your_storage_account_name
```
For more information, run:
```bash
python3 image_import.py -h
```
