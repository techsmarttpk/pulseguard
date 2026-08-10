"""Shared library used by every PulseGuard Python service.

Kept intentionally small: schemas, Kafka helpers, DB helpers, logging and
Prometheus metric factories. Each service copies this package into its
Docker image (see each service's Dockerfile) and imports it as
``pulseguard_common``.
"""
