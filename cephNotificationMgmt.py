#!/usr/bin/env python3
# -----------------------------------------------------------------------------
# Project: Ceph Notification Manager
# File: cephNotificationMgmt.py
# Author: JordahlLabs
# Created for: OSNEXUS
#
# Copyright (c) 2025 JordahlLabs
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
#
# -----------------------------------------------------------------------------
# Description:
#   This module provides a lightweight management interface for configuring
#   Ceph RGW bucket notifications using S3- and SNS-compatible APIs via boto3.
#
#   The primary use case is interactive or ad-hoc administration of Ceph
#   bucket notifications, particularly AMQP-based push notifications.
#
#   While this implementation has not been fully validated against Amazon S3,
#   it is designed to be largely S3-compatible and may work with minimal changes.
#
#   Currently implemented:
#     - Bucket enumeration
#     - Topic enumeration and inspection
#     - AMQP topic creation
#     - Bucket notification creation and removal
#
#   Extensibility:
#     - Additional notification transports (Kafka, HTTP/S, etc.) can be added
#       by implementing additional topic-creation methods.
#
#   Error Handling:
#     - This module intentionally returns raw API responses serialized as JSON
#       strings (including error responses).
#     - Exceptions are not raised to support interactive scripting and
#       inspection of Ceph/S3 API behavior.
#
#   Security:
#     - TLS verification and credential handling are intentionally delegated
#       to the calling script or execution environment.
#
# -----------------------------------------------------------------------------

import boto3
from botocore.client import Config
import json


class CephNotificationMgr:
    """
    Manages Ceph RGW bucket notifications using S3 and SNS-compatible APIs.

    This class is intended for interactive or administrative use cases where
    Ceph bucket notifications must be created, inspected, or removed on demand.

    The implementation focuses on AMQP-based notifications but is structured
    to allow easy extension to additional notification backends.
    """

    def __init__(self, endpoint='', access_key='', secret_key='',
                 region="us-east-1", aws_profile=''):
        """
        Initialize the notification manager.

        Authentication may be performed using either:
          - Explicit access/secret keys, or
          - A named AWS profile (recommended when available)

        Parameters:
            endpoint (str): Ceph RGW endpoint URL
            access_key (str): S3 access key (ignored if aws_profile is set)
            secret_key (str): S3 secret key (ignored if aws_profile is set)
            region (str): AWS region name (default: us-east-1)
            aws_profile (str): AWS CLI profile name to use
        """

        if aws_profile:
            session = boto3.Session(profile_name=aws_profile)

            self.s3 = session.client(
                "s3",
                endpoint_url=endpoint,
                config=Config(signature_version="s3v4"),
                use_ssl=True,
                verify=False
            )

            self.sns = session.client(
                "sns",
                endpoint_url=endpoint,
                config=Config(signature_version="s3v4"),
                use_ssl=True,
                verify=False
            )
        else:
            self.s3 = boto3.client(
                "s3",
                endpoint_url=endpoint,
                aws_access_key_id=access_key,
                aws_secret_access_key=secret_key,
                region_name=region,
                config=Config(signature_version="s3v4"),
                use_ssl=True,
                verify=False
            )

            self.sns = boto3.client(
                "sns",
                endpoint_url=endpoint,
                aws_access_key_id=access_key,
                aws_secret_access_key=secret_key,
                region_name=region,
                config=Config(signature_version="s3v4"),
                use_ssl=True,
                verify=False
            )

    # --------------------------------------------------------
    # Helper: Convert API responses to JSON
    # --------------------------------------------------------
    def _to_json(self, obj):
        """
        Convert a Python object into a JSON-formatted string.

        This helper normalizes boto3 responses (including datetime objects)
        into JSON for easier inspection and logging.

        Parameters:
            obj (any): Python object to serialize

        Returns:
            str: Pretty-printed JSON string
        """
        try:
            return json.dumps(obj, indent=2, default=str)
        except Exception as e:
            return json.dumps({"error": f"JSON encode failed: {e}"}, indent=2)

    # --------------------------------------------------------
    # LIST ALL BUCKETS
    # --------------------------------------------------------
    def list_buckets(self):
        """
        List all buckets visible to the authenticated user.

        Returns:
            str: JSON-formatted response from the S3 API
        """
        try:
            resp = self.s3.list_buckets()
            return self._to_json(resp)
        except Exception as e:
            return self._to_json({"error": str(e)})

    # --------------------------------------------------------
    # LIST ALL TOPICS
    # --------------------------------------------------------
    def list_topics(self):
        """
        List all notification topics.

        In Ceph RGW, topics are typically used as push-notification targets
        (e.g., AMQP exchanges).

        Returns:
            str: JSON-formatted response from the SNS API
        """
        try:
            resp = self.sns.list_topics()
            return self._to_json(resp)
        except Exception as e:
            return self._to_json({"error": str(e)})

    # --------------------------------------------------------
    # GET A TOPIC
    # --------------------------------------------------------
    def get_topic(self, topic_arn):
        """
        Retrieve attributes for a specific notification topic.

        Parameters:
            topic_arn (str): ARN of the topic

        Returns:
            str: JSON-formatted topic attributes
        """
        try:
            resp = self.sns.get_topic_attributes(TopicArn=topic_arn)
            return self._to_json(resp)
        except Exception as e:
            return self._to_json({"error": str(e)})

    # --------------------------------------------------------
    # CREATE OR UPDATE AN AMQP TOPIC
    # --------------------------------------------------------
    def create_amqp_topic(self, exchange, topic_name, amqp_uri,
                          ca_location='', verify_ssl=False):
        """
        Create or update an AMQP-backed notification topic.

        This method configures a Ceph RGW SNS topic that publishes events
        to an AMQP exchange (e.g., RabbitMQ).

        Parameters:
            exchange (str): AMQP exchange name
            topic_name (str): Ceph/SNS topic name
            amqp_uri (str): AMQP endpoint URI (host:port/vhost)
            ca_location (str): Optional CA certificate path
            verify_ssl (bool): Whether to verify TLS certificates

        Returns:
            str: JSON-formatted response from the SNS API
        """

        attributes = {
            'amqp-exchange': exchange,
            'amqp-ack-level': 'broker',
            'use-ssl': 'true',
            'verify-ssl': str(verify_ssl).lower()
        }

        if ca_location:
            attributes['ca-location'] = ca_location
            attributes['push-endpoint'] = 'amqps://' + amqp_uri
        else:
            attributes['push-endpoint'] = 'amqp://' + amqp_uri

        try:
            resp = self.sns.create_topic(
                Name=topic_name,
                Attributes=attributes
            )
            return self._to_json(resp)
        except Exception as e:
            return self._to_json({"error": str(e)})

    # --------------------------------------------------------
    # DELETE A TOPIC
    # --------------------------------------------------------
    def delete_topic(self, topic_arn):
        """
        Delete a notification topic.

        Parameters:
            topic_arn (str): ARN of the topic to delete

        Returns:
            str: JSON-formatted response
        """
        try:
            resp = self.sns.delete_topic(TopicArn=topic_arn)
            return self._to_json(resp)
        except Exception as e:
            return self._to_json({"error": str(e)})

    # --------------------------------------------------------
    # LIST NOTIFICATIONS FOR A BUCKET
    # --------------------------------------------------------
    def list_notifications(self, bucket, owner):
        """
        Retrieve notification configuration for a bucket.

        Parameters:
            bucket (str): Bucket name
            owner (str): Expected bucket owner ID

        Returns:
            str: JSON-formatted notification configuration
        """
        try:
            resp = self.s3.get_bucket_notification_configuration(
                Bucket=bucket,
                ExpectedBucketOwner=owner
            )
            return self._to_json(resp)
        except Exception as e:
            return self._to_json({"error": str(e)})

    # --------------------------------------------------------
    # CREATE AMQP NOTIFICATION
    # --------------------------------------------------------
    def create_notification(self, bucket, topic_arn, notification_id):
        """
        Create an AMQP-based bucket notification.

        This configures the bucket to emit object create and delete events
        to the specified topic.

        Parameters:
            bucket (str): Bucket name
            topic_arn (str): ARN of the notification topic
            notification_id (str): Identifier for the notification rule

        Returns:
            str: JSON-formatted response
        """

        bucket_notifications_configuration = {
            'TopicConfigurations': [
                {
                    'Id': notification_id,
                    'TopicArn': topic_arn,
                    'Events': [
                        's3:ObjectCreated:*',
                        's3:ObjectRemoved:*'
                    ]
                }
            ]
        }

        try:
            resp = self.s3.put_bucket_notification_configuration(
                Bucket=bucket,
                NotificationConfiguration=bucket_notifications_configuration
            )
            return self._to_json(resp)
        except Exception as e:
            return self._to_json({"error": str(e)})

    # --------------------------------------------------------
    # DELETE ALL NOTIFICATIONS FROM A BUCKET
    # --------------------------------------------------------
    def delete_notifications(self, bucket, owner):
        """
        Remove all notification rules from a bucket.

        This is accomplished by submitting an empty notification
        configuration to the S3 API.

        Parameters:
            bucket (str): Bucket name
            owner (str): Expected bucket owner ID

        Returns:
            str: JSON-formatted response
        """
        try:
            resp = self.s3.put_bucket_notification_configuration(
                Bucket=bucket,
                NotificationConfiguration={}
            )
            return self._to_json(resp)
        except Exception as e:
            return self._to_json({"error": str(e)})
