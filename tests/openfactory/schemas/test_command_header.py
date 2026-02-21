import unittest
from uuid import uuid4
from datetime import datetime, timezone
from openfactory.schemas.command_header import CommandHeader, CommandEnvelope
from pydantic import ValidationError


class TestCommandHeader(unittest.TestCase):
    """
    Unit tests for class CommandHeader
    """

    def test_valid_header(self):
        """ Test a valid CommandHeader configuration """
        header = CommandHeader(
            correlation_id=uuid4(),
            sender_uuid="ROBOT-ABC"
        )

        self.assertIsNotNone(header.correlation_id)
        self.assertIsNotNone(header.sender_uuid)
        self.assertIsInstance(header.timestamp, datetime)
        self.assertIsNone(header.signature)

    def test_invalid_uuid(self):
        """ Test that invalid UUID values raise ValidationError """
        with self.assertRaises(ValidationError):
            CommandHeader(
                correlation_id="not-a-uuid",
                sender_uuid="CNC-123"
            )

    def test_sender_id_string_allowed(self):
        """ Test that arbitrary sender_id strings are allowed """
        header = CommandHeader(
            correlation_id=uuid4(),
            sender_uuid="CNC-1238"
        )
        self.assertEqual(header.sender_uuid, "CNC-1238")

    def test_extra_field_forbidden(self):
        """ Test that extra fields are rejected """
        with self.assertRaises(ValidationError):
            CommandHeader(
                correlation_id=uuid4(),
                sender_uuid=uuid4(),
                unexpected_field="not allowed"
            )

    def test_timestamp_default(self):
        """ Test that timestamp is automatically set if not provided """
        header = CommandHeader(
            correlation_id=uuid4(),
            sender_uuid="ASSET-456"
        )
        self.assertIsInstance(header.timestamp, datetime)
        # Make sure it's timezone-aware UTC
        self.assertIsNotNone(header.timestamp.tzinfo)
        self.assertEqual(header.timestamp.tzinfo.utcoffset(header.timestamp).total_seconds(), 0)

    def test_timestamp_default_now(self):
        """ Test that the timestamp default is set to current UTC time """
        before = datetime.now(tz=timezone.utc)
        header = CommandHeader(
            correlation_id=uuid4(),
            sender_uuid="ASSET-456"
        )
        after = datetime.now(tz=timezone.utc)

        self.assertIsInstance(header.timestamp, datetime)
        self.assertIsNotNone(header.timestamp.tzinfo)
        self.assertEqual(header.timestamp.tzinfo.utcoffset(header.timestamp).total_seconds(), 0)

        # Check that timestamp is within the creation window
        self.assertTrue(before <= header.timestamp <= after,
                        f"timestamp {header.timestamp} not within {before} and {after}")


class TestCommandEnvelope(unittest.TestCase):
    """
    Unit tests for class CommandEnvelope
    """

    def setUp(self):
        self.valid_header = CommandHeader(
            correlation_id=uuid4(),
            sender_uuid="ASSET-123"
        )

    def test_valid_envelope(self):
        """ Test a valid CommandEnvelope configuration """
        envelope = CommandEnvelope(
            header=self.valid_header,
            arguments={
                "x": "10",
                "y": "20",
                "z": "30"
            }
        )

        self.assertEqual(envelope.arguments["x"], "10")
        self.assertEqual(envelope.arguments["y"], "20")
        self.assertEqual(envelope.arguments["z"], "30")

    def test_arguments_default_empty(self):
        """ Test that arguments default to empty dict """
        envelope = CommandEnvelope(header=self.valid_header)
        self.assertEqual(envelope.arguments, {})

    def test_arguments_must_be_string_values(self):
        """ Test that non-string argument values raise ValidationError """
        with self.assertRaises(ValidationError):
            CommandEnvelope(
                header=self.valid_header,
                arguments={
                    "x": 10  # not allowed, must be string
                }
            )

    def test_extra_field_forbidden(self):
        """ Test that extra fields in envelope are rejected """
        with self.assertRaises(ValidationError):
            CommandEnvelope(
                header=self.valid_header,
                arguments={},
                extra_field="not allowed"
            )

    def test_nested_extra_field_forbidden(self):
        """ Test that extra fields inside header are rejected """
        with self.assertRaises(ValidationError):
            CommandEnvelope(
                header={
                    "correlation_id": str(uuid4()),
                    "sender_uuid": str(uuid4()),
                    "unexpected": "not allowed"
                },
                arguments={}
            )
