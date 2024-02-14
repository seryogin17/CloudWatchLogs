import argparse
import boto3
import docker
import logging
import sys
import textwrap

from botocore.exceptions import ClientError
from datetime import datetime, timedelta
from dateutil import parser


LOGGER_FORMAT = "%(levelname)-8s | %(message)s"
logger = logging.getLogger()
SIZE_PADDING = 26
LOG_MESSAGE_MAX_SIZE = 262144
MAX_BATCH_SIZE = 1048576
LOG_EVENTS_BATCH_MAX_LEN = 10000

LIMIT_FOR_LOG_FROM_PAST_IN_DAYS = 14
LIMIT_FOR_LOG_FROM_FUTURE_IN_DAYS = 2

VALID_LOG_GROUP_NAME_PATTERN = r'^(?!aws/)[a-zA-Z0-9_\-/.#]{1,512}$'
VALID_LOG_STREAM_NAME_PATTERN = r'^[^:*]{1,512}$'


def set_logger(logger_format) -> None:
    handler = logging.StreamHandler()
    handler.setLevel(logging.INFO)
    handler.setFormatter(logging.Formatter(logger_format))
    logger.setLevel(logging.INFO)
    logger.addHandler(handler)
    logger.propagate = False


def parse_args():
    parser = argparse.ArgumentParser(description='Run a Docker container and log output to AWS CloudWatch.')
    parser.add_argument('--docker-image', required=True, help='Docker Image to use')
    parser.add_argument('--bash-command', required=True, help='Bash command to run in the Docker container')
    parser.add_argument('--aws-access-key-id', required=True, help='AWS Access Key ID')
    parser.add_argument('--aws-secret-access-key', required=True, help='AWS Secret Access Key')
    parser.add_argument('--aws-region', required=True, help='AWS Region Name')
    parser.add_argument('--aws-cloudwatch-group', required=True, help='CloudWatch Log Group Name')
    parser.add_argument('--aws-cloudwatch-stream', required=True, help='CloudWatch Log Stream Name')

    return vars(parser.parse_args())


def get_log_events(aws_cloudwatch_client, aws_cloudwatch_group, aws_cloudwatch_stream, nextToken=None):
    get_log_events_args = {"logGroupName": aws_cloudwatch_group,
                           "logStreamName": aws_cloudwatch_stream,
                           "startFromHead": True}
    if nextToken:
        get_log_events_args.update({"nextToken": nextToken})

    response = aws_cloudwatch_client.get_log_events(**get_log_events_args)

    log_events = response['events'] if response['events'] else []

    if response["nextForwardToken"] != nextToken:
        log_events += get_log_events(aws_cloudwatch_client,
                                     aws_cloudwatch_group,
                                     aws_cloudwatch_stream,
                                     response["nextForwardToken"])

    return log_events


def timestamp_fits_timespan(batch, log_timestamp):
    return (len(batch) == 0) or (timedelta(milliseconds=batch[0]["timestamp"]-log_timestamp) < timedelta(hours=24))


def log_fits_batch(batch, message_size, batch_size):
    return (len(batch) < LOG_EVENTS_BATCH_MAX_LEN) and (message_size + batch_size <= MAX_BATCH_SIZE)


def validate_timestamp(log_iso_timestamp, retention_period):
    log_unix_timestamp = parser.parse(log_iso_timestamp).timestamp()
    current_unix_timestamp = datetime.now().timestamp()
    if retention_period:
        min_past_limit = min(LIMIT_FOR_LOG_FROM_PAST_IN_DAYS, retention_period)
    else:
        min_past_limit = LIMIT_FOR_LOG_FROM_PAST_IN_DAYS

    log_timedelta_in_days = timedelta(seconds=current_unix_timestamp-log_unix_timestamp).days

    if (log_timedelta_in_days > 0 and log_timedelta_in_days > min_past_limit) or \
        (log_timedelta_in_days < 0 and abs(log_timedelta_in_days) > LIMIT_FOR_LOG_FROM_FUTURE_IN_DAYS):

        logger.warning(f"Log time '{datetime.fromtimestamp(log_unix_timestamp)}' is out of permitted range, "
                       f"resetting it to current time '{datetime.fromtimestamp(current_unix_timestamp)}'...")
        return round(current_unix_timestamp * 1000)

    return round(log_unix_timestamp * 1000)


def get_batch_of_log_events(docker_logs, retention_period):
    batch = []
    batch_size = 0

    for docker_log in docker_logs:
        log_iso_timestamp, log_message = docker_log.decode().strip().split(" ", 1)
        log_unix_timestamp_in_ms = validate_timestamp(log_iso_timestamp, retention_period)
        for log_line in textwrap.wrap(log_message, LOG_MESSAGE_MAX_SIZE):
            log_event = {"message": log_line,
                         "timestamp": log_unix_timestamp_in_ms}

            log_line_size = len(log_line.encode()) + SIZE_PADDING

            if not (log_fits_batch(batch, log_line_size, batch_size) and \
                    timestamp_fits_timespan(batch, log_unix_timestamp_in_ms)):
                yield batch
                batch = []
                batch_size = 0

                batch += [log_event]
                batch_size += log_line_size

    yield batch


def put_log_events(aws_cloudwatch_client, aws_cloudwatch_group, aws_cloudwatch_stream, retention_period, docker_logs):

    for log_events_batch in get_batch_of_log_events(docker_logs, retention_period):
        if log_events_batch:
            response = aws_cloudwatch_client.put_log_events(logGroupName=aws_cloudwatch_group,
                                                            logStreamName=aws_cloudwatch_stream,
                                                            logEvents=log_events_batch)


def set_aws_cloudwatch_stream(aws_cloudwatch_client, aws_cloudwatch_group, aws_cloudwatch_stream):
    response = aws_cloudwatch_client.describe_log_streams(logGroupName=aws_cloudwatch_group,
                                                          logStreamNamePrefix=aws_cloudwatch_stream)

    if (len(response['logStreams']) > 0) and (response['logStreams'][0]["logStreamName"] == aws_cloudwatch_stream):
        logger.info(f"AWS CloudWatch stream '{aws_cloudwatch_stream}' already exists, proceeding with it...")
    else:
        logger.info(f"CloudWatch stream '{aws_cloudwatch_stream}' not found, creating it...")
        aws_cloudwatch_client.create_log_stream(logGroupName=aws_cloudwatch_group,
                                                logStreamName=aws_cloudwatch_stream)


def set_aws_cloudwatch_group(aws_cloudwatch_client, aws_cloudwatch_group):
    response = aws_cloudwatch_client.describe_log_groups(logGroupNamePrefix=aws_cloudwatch_group)

    if (len(response['logGroups']) > 0) and (response['logGroups'][0]["logGroupName"] == aws_cloudwatch_group):
        logger.info(f"AWS CloudWatch group '{aws_cloudwatch_group}' already exists, proceeding with it...")
        return response['logGroups'][0].get("retentionInDays")
    else:
        logger.info(f"CloudWatch group '{aws_cloudwatch_group}' not found, creating it...")
        aws_cloudwatch_client.create_log_group(logGroupName=aws_cloudwatch_group)


def set_aws_cloudwatch_group_and_stream(aws_cloudwatch_client, aws_cloudwatch_group, aws_cloudwatch_stream):
    try:
        retention_period = set_aws_cloudwatch_group(aws_cloudwatch_client, aws_cloudwatch_group)
        set_aws_cloudwatch_stream(aws_cloudwatch_client, aws_cloudwatch_group, aws_cloudwatch_stream)
        return retention_period

    except aws_cloudwatch_client.exceptions.InvalidParameterException as error:
        logger.error("Failed to validate CloudWatch group and/or stream naming. ")
        logger.error(f"To fix use the following regex patterns: "
                     f"for group name '{VALID_LOG_GROUP_NAME_PATTERN}', "
                     f"and for stream name '{VALID_LOG_STREAM_NAME_PATTERN}'.")
        sys.exit(1)

    except ClientError as error:
        logger.error(str(error))
        sys.exit(1)


def run(aws_access_key_id, aws_secret_access_key,
        aws_region, aws_cloudwatch_group, aws_cloudwatch_stream,
        docker_image, bash_command):

    logger.info("Initializing AWS CloudWatch client...")
    aws_cloudwatch_client = boto3.client('logs',
                                         aws_access_key_id=aws_access_key_id,
                                         aws_secret_access_key=aws_secret_access_key,
                                         region_name=aws_region)

    retention_period = set_aws_cloudwatch_group_and_stream(aws_cloudwatch_client,
                                                                      aws_cloudwatch_group,
                                                                      aws_cloudwatch_stream)

    logger.info("Initializing Docker client...")
    docker_client = docker.from_env()

    logger.info(f"Running Docker image '{docker_image}'...")
    container = docker_client.containers.run(docker_image,
                                             command=bash_command,
                                             detach=True)

    logger.info("Saving log events...")
    put_log_events(aws_cloudwatch_client,
                   aws_cloudwatch_group,
                   aws_cloudwatch_stream,
                   retention_period,
                   container.logs(stream=True,
                                  stdout=True,
                                  stderr=True,
                                  timestamps=True))

    # logger.info("Reading all log events from stream...")
    # log_events = get_log_events(aws_cloudwatch_client,
    #                             aws_cloudwatch_group,
    #                             aws_cloudwatch_stream)
    # print(*log_events, sep="\n")

if __name__ == '__main__':
    args = parse_args()
    set_logger(LOGGER_FORMAT)
    run(**args)
    logger.info("Run is completed.")
