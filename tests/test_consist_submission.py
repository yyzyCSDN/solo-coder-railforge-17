import unittest
from datetime import datetime, timezone

from railforge.consist.integrity import WagonLink, validate
from railforge.workflows.control import RailForgeWorkflowControl

U = timezone.utc
AT = datetime(2026, 3, 1, 12, tzinfo=U)


def three_car_consist(**middle):
    link = dict(position=2, wagon='W2', previous='W1', next='W3', brake_connected=True)
    link.update(middle)
    return [
        WagonLink(1, 'W1', None, 'W2', True),
        WagonLink(**link),
        WagonLink(3, 'W3', 'W2', None, True),
    ]


class JointValidationTests(unittest.TestCase):
    """Position continuity, chain references and brake connectivity are
    validated together for every consist, including three-car cuts."""

    def test_replaced_middle_connector_with_wrong_references_is_rejected(self):
        with self.assertRaisesRegex(ValueError, 'chain'):
            validate(three_car_consist(previous='W9', next='W8'))

    def test_middle_brake_pipe_open_is_rejected(self):
        with self.assertRaisesRegex(ValueError, 'brake'):
            validate(three_car_consist(brake_connected=False))

    def test_position_gap_is_rejected(self):
        rows = [WagonLink(1, 'W1', None, 'W2', True),
                WagonLink(3, 'W2', 'W1', None, True)]
        with self.assertRaisesRegex(ValueError, 'position'):
            validate(rows)

    def test_valid_three_car_consist_passes(self):
        self.assertEqual(validate(three_car_consist()), ('W1', 'W3'))


class AtomicSubmissionTests(unittest.TestCase):
    def setUp(self):
        self.control = RailForgeWorkflowControl()

    def test_submission_publishes_versioned_event_and_audit_together(self):
        receipt = self.control.validate_consist('op-1', three_car_consist(), 'CONSIST-1', AT)
        self.assertEqual(receipt.result, ('W1', 'W3'))
        self.assertEqual(receipt.version, 1)
        self.assertEqual(receipt.event.topic, 'consist.validated')
        self.assertEqual(receipt.event.payload['status'], 'committed')
        self.assertEqual(receipt.event.payload['control_path'], 'atomic')
        self.assertEqual(receipt.event.payload['version'], 1)
        self.assertEqual(receipt.audit_index, 1)
        self.assertEqual(self.control.store.read('CONSIST-1').value, ['W1', 'W3'])
        self.assertTrue(self.control.stream.verify_chain())
        self.assertTrue(self.control.trail.verify())

    def test_duplicate_submission_returns_same_receipt(self):
        first = self.control.validate_consist('op-1', three_car_consist(), 'CONSIST-1', AT)
        second = self.control.validate_consist('op-1', three_car_consist(), 'CONSIST-1', AT)
        self.assertIs(first, second)
        self.assertEqual(self.control.stream.head, 1)
        self.assertEqual(self.control.store.read('CONSIST-1').version, 1)

    def test_rejected_consist_commits_nothing(self):
        with self.assertRaises(ValueError):
            self.control.validate_consist('op-bad', three_car_consist(brake_connected=False),
                                          'CONSIST-2', AT)
        self.assertEqual(self.control.stream.head, 0)
        self.assertIsNone(self.control.store.read('CONSIST-2'))
        self.assertNotIn('op-bad', self.control.receipts)

    def test_audit_failure_rolls_back_store_and_event(self):
        original = self.control.trail.append
        self.control.trail.append = lambda *a, **k: (_ for _ in ()).throw(RuntimeError('sink down'))
        try:
            with self.assertRaises(RuntimeError):
                self.control.validate_consist('op-2', three_car_consist(), 'CONSIST-3', AT)
        finally:
            self.control.trail.append = original
        self.assertIsNone(self.control.store.read('CONSIST-3'))
        self.assertEqual(self.control.stream.head, 0)
        self.assertNotIn('op-2', self.control.receipts)
        self.assertTrue(self.control.stream.verify_chain())
        self.assertTrue(self.control.trail.verify())
        # Same operation id can be retried and then commits cleanly.
        receipt = self.control.validate_consist('op-2', three_car_consist(), 'CONSIST-3', AT)
        self.assertEqual(receipt.version, 1)
        self.assertEqual(self.control.stream.head, 1)

    def test_event_failure_rolls_back_store(self):
        original = self.control.stream.publish
        self.control.stream.publish = lambda *a, **k: (_ for _ in ()).throw(RuntimeError('broker down'))
        try:
            with self.assertRaises(RuntimeError):
                self.control.validate_consist('op-3', three_car_consist(), 'CONSIST-4', AT)
        finally:
            self.control.stream.publish = original
        self.assertIsNone(self.control.store.read('CONSIST-4'))
        self.assertEqual(self.control.stream.head, 0)
        self.assertTrue(self.control.trail.verify())


if __name__ == '__main__':
    unittest.main()
