# ceph-notification-mgmt

CephNotificationMgr is a lightweight Python utility for managing Ceph RGW bucket notifications using S3- and SNS-compatible APIs via boto3. It is designed for ad-hoc and administrative use cases where bucket notifications need to be inspected, created, or removed interactively, with a focus on AMQP-based event delivery. While primarily targeting Ceph Object Gateway deployments, the implementation is largely S3-compatible and structured to serve as a practical starting point for extending notification support to additional transports such as Kafka or HTTP.
