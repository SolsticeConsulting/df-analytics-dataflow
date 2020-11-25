from argparse import ArgumentParser
from google.cloud.storage import Client, Blob
from io import StringIO
import logging
import progressbar


parser = ArgumentParser()
parser.add_argument("--bucket-name", required=True,
                    help="GCS Bucket Name to target.")
parser.add_argument("--target-pattern", required=True,
                    help="Pattern Files to match from GCS Bucket to compress to batch file.")
parser.add_argument("--verbose", required=False, action="store_true")
args = parser.parse_args()

# Logging Configuration
if args.verbose:
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
else:
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

logger = logging.getLogger("Batch.Creation.Driver")
logger.debug(f'Arg Parser values: {args}')

# Arg Parse Details
target_bucket = args.bucket_name
target_pattern = args.target_pattern

if __name__ == '__main__':
    logger.info("Searching for files to parse...")
    analytic_files_to_batch = list(Client().list_blobs(bucket_or_name=target_bucket, prefix="analytic_records/"))
    conversation_files_to_batch = list(Client().list_blobs(bucket_or_name=target_bucket, prefix="conversation_records/"))

    if len(analytic_files_to_batch) == 0:
        logger.error(f'No Analytic Files Found!')
        exit(1)
    elif len(conversation_files_to_batch) == 0:
        logger.error(f'No Conversation Files Found!')
        exit(1)
    else:
        logger.info(f'Found {len(analytic_files_to_batch)} analytic files to search.')
        logger.info(f'Found {len(conversation_files_to_batch)} conversation files to search.')

    filtered_analytic_files = list(filter(lambda blob: blob.name.find(target_pattern) > 0, analytic_files_to_batch))
    filtered_conversation_files = list(filter(lambda blob: blob.name.find(target_pattern) > 0, conversation_files_to_batch))

    if len(filtered_analytic_files) == 0:
        logger.error(f'No Analytic Files match the pattern "{target_pattern}"!')
        exit(1)
    elif len(filtered_conversation_files) == 0:
        logger.error(f'No Conversation Files match the pattern "{target_pattern}"!')
        exit(1)
    else:
        logger.info(f'Found {len(filtered_analytic_files)} analytic files to batch.')
        logger.info(f'Found {len(filtered_conversation_files)} conversation files to batch.')

    analytic_output_blob = Client().bucket(bucket_name=target_bucket).blob(blob_name=f'analytics_batch_{target_pattern}')
    analytic_output_contents = StringIO()

    conversation_output_blob = Client().bucket(bucket_name=target_bucket).blob(blob_name=f'conversations_batch_{target_pattern}')
    conversation_output_contents = StringIO()

    logger.info("Beginning Parsing...")

    # Type Hint
    blob: Blob
    for blob in progressbar.progressbar(filtered_analytic_files, redirect_stdout=True, redirect_stderr=True):
        logger.debug(f'Starting Analytic File : {blob.name}')
        blob_contents = StringIO(blob.download_as_bytes().decode("UTF-8"))
        analytic_output_contents.writelines(blob_contents)

    # Upload file to GCS
    logger.info(f'Uploading analytics batch file with name : {analytic_output_blob.name}')
    analytic_output_blob.upload_from_string(analytic_output_contents.getvalue())

    # Type Hint
    blob: Blob
    for blob in progressbar.progressbar(filtered_conversation_files, redirect_stdout=True, redirect_stderr=True):
        logger.debug(f'Starting Conversation File : {blob.name}')
        blob_contents = StringIO(blob.download_as_bytes().decode("UTF-8"))
        conversation_output_contents.writelines(blob_contents)

    # Upload file to GCS
    logger.info(f'Uploading conversations batch file with name : {conversation_output_blob.name}')
    conversation_output_blob.upload_from_string(conversation_output_contents.getvalue())

    logger.info("Batch Creation complete!")
