#!/usr/bin/env python3
"""
Unit tests for the cron parser in autoscaler_daemon.py.
Tests the _cron_matches(cron_expr, dt) method via a minimal stub.
"""
import sys
import os
import unittest
from datetime import datetime

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)),
                                '..', '..', '..', 'src', 'main', 'resources',
                                'addon-services', 'AZURE_AUTOSCALER', '1.0.0', 'package', 'files'))

from autoscaler_daemon import AutoscalerDaemon


class CronMatcherStub:
    """Minimal stub to test _cron_matches without full daemon initialisation."""
    _cron_matches = AutoscalerDaemon._cron_matches
    _cron_field_matches = AutoscalerDaemon._cron_field_matches


class TestCronParser(unittest.TestCase):
    """Tests for AutoscalerDaemon._cron_matches."""

    def setUp(self):
        self.matcher = CronMatcherStub()

    # ------------------------------------------------------------------ #
    # Wildcard
    # ------------------------------------------------------------------ #
    def test_wildcard_matches_any(self):
        """'* * * * *' should match any datetime."""
        self.assertTrue(self.matcher._cron_matches('* * * * *', datetime(2026, 1, 1, 0, 0)))
        self.assertTrue(self.matcher._cron_matches('* * * * *', datetime(2026, 6, 15, 12, 30)))
        self.assertTrue(self.matcher._cron_matches('* * * * *', datetime(2026, 12, 31, 23, 59)))

    # ------------------------------------------------------------------ #
    # Exact minute / hour
    # ------------------------------------------------------------------ #
    def test_exact_minute_hour(self):
        """'30 9 * * *' should match 9:30 AM and NOT 9:31 AM."""
        self.assertTrue(self.matcher._cron_matches('30 9 * * *', datetime(2026, 3, 25, 9, 30)))
        self.assertFalse(self.matcher._cron_matches('30 9 * * *', datetime(2026, 3, 25, 9, 31)))

    # ------------------------------------------------------------------ #
    # Day-of-week (numeric)
    # ------------------------------------------------------------------ #
    def test_monday_cron(self):
        """'0 9 * * 1' (Monday=1 in cron) should match 2026-03-23 which is a Monday."""
        monday = datetime(2026, 3, 23, 9, 0)
        self.assertEqual(monday.strftime('%A'), 'Monday')
        self.assertTrue(self.matcher._cron_matches('0 9 * * 1', monday))

    def test_sunday_cron_zero(self):
        """'0 9 * * 0' (Sunday=0 in cron) should match 2026-03-29 which is a Sunday."""
        sunday = datetime(2026, 3, 29, 9, 0)
        self.assertEqual(sunday.strftime('%A'), 'Sunday')
        self.assertTrue(self.matcher._cron_matches('0 9 * * 0', sunday))

    def test_friday_cron(self):
        """'0 17 * * 5' (Friday=5 in cron) should match 2026-03-27 at 17:00."""
        friday = datetime(2026, 3, 27, 17, 0)
        self.assertEqual(friday.strftime('%A'), 'Friday')
        self.assertTrue(self.matcher._cron_matches('0 17 * * 5', friday))

    # ------------------------------------------------------------------ #
    # Named day range
    # ------------------------------------------------------------------ #
    def test_named_day_MON_FRI(self):
        """'0 8 * * MON-FRI' should match weekdays but NOT Saturday or Sunday."""
        # Monday 2026-03-23
        self.assertTrue(self.matcher._cron_matches('0 8 * * MON-FRI',
                                                   datetime(2026, 3, 23, 8, 0)))
        # Wednesday 2026-03-25
        self.assertTrue(self.matcher._cron_matches('0 8 * * MON-FRI',
                                                   datetime(2026, 3, 25, 8, 0)))
        # Friday 2026-03-27
        self.assertTrue(self.matcher._cron_matches('0 8 * * MON-FRI',
                                                   datetime(2026, 3, 27, 8, 0)))
        # Saturday 2026-03-28
        self.assertFalse(self.matcher._cron_matches('0 8 * * MON-FRI',
                                                    datetime(2026, 3, 28, 8, 0)))
        # Sunday 2026-03-29
        self.assertFalse(self.matcher._cron_matches('0 8 * * MON-FRI',
                                                    datetime(2026, 3, 29, 8, 0)))

    # ------------------------------------------------------------------ #
    # Comma list
    # ------------------------------------------------------------------ #
    def test_comma_list(self):
        """'0 9,17 * * *' should match 09:00 and 17:00, but NOT 10:00."""
        self.assertTrue(self.matcher._cron_matches('0 9,17 * * *', datetime(2026, 3, 25, 9, 0)))
        self.assertTrue(self.matcher._cron_matches('0 9,17 * * *', datetime(2026, 3, 25, 17, 0)))
        self.assertFalse(self.matcher._cron_matches('0 9,17 * * *', datetime(2026, 3, 25, 10, 0)))

    # ------------------------------------------------------------------ #
    # Day-of-month
    # ------------------------------------------------------------------ #
    def test_day_of_month(self):
        """'0 0 15 * *' should match the 15th of any month at midnight."""
        self.assertTrue(self.matcher._cron_matches('0 0 15 * *', datetime(2026, 4, 15, 0, 0)))
        self.assertFalse(self.matcher._cron_matches('0 0 15 * *', datetime(2026, 4, 14, 0, 0)))

    # ------------------------------------------------------------------ #
    # Specific month
    # ------------------------------------------------------------------ #
    def test_specific_month(self):
        """'0 0 1 6 *' should match June 1st at midnight."""
        self.assertTrue(self.matcher._cron_matches('0 0 1 6 *', datetime(2026, 6, 1, 0, 0)))
        self.assertFalse(self.matcher._cron_matches('0 0 1 6 *', datetime(2026, 7, 1, 0, 0)))

    # ------------------------------------------------------------------ #
    # Step expressions (*/N)
    # ------------------------------------------------------------------ #
    def test_star_step_every_5_minutes(self):
        """'*/5 * * * *' should match minutes 0, 5, 10, ... 55."""
        self.assertTrue(self.matcher._cron_matches('*/5 * * * *', datetime(2026, 1, 1, 0, 0)))
        self.assertTrue(self.matcher._cron_matches('*/5 * * * *', datetime(2026, 1, 1, 0, 15)))
        self.assertTrue(self.matcher._cron_matches('*/5 * * * *', datetime(2026, 1, 1, 12, 30)))
        self.assertFalse(self.matcher._cron_matches('*/5 * * * *', datetime(2026, 1, 1, 0, 3)))
        self.assertFalse(self.matcher._cron_matches('*/5 * * * *', datetime(2026, 1, 1, 0, 22)))

    def test_star_step_every_2_hours(self):
        """'0 */2 * * *' should match hours 0, 2, 4, ... 22."""
        self.assertTrue(self.matcher._cron_matches('0 */2 * * *', datetime(2026, 1, 1, 0, 0)))
        self.assertTrue(self.matcher._cron_matches('0 */2 * * *', datetime(2026, 1, 1, 14, 0)))
        self.assertFalse(self.matcher._cron_matches('0 */2 * * *', datetime(2026, 1, 1, 3, 0)))

    def test_range_step(self):
        """'1-30/5 * * * *' should match 1, 6, 11, 16, 21, 26 but NOT 0 or 31."""
        self.assertTrue(self.matcher._cron_matches('1-30/5 * * * *', datetime(2026, 1, 1, 0, 1)))
        self.assertTrue(self.matcher._cron_matches('1-30/5 * * * *', datetime(2026, 1, 1, 0, 6)))
        self.assertTrue(self.matcher._cron_matches('1-30/5 * * * *', datetime(2026, 1, 1, 0, 26)))
        self.assertFalse(self.matcher._cron_matches('1-30/5 * * * *', datetime(2026, 1, 1, 0, 0)))
        self.assertFalse(self.matcher._cron_matches('1-30/5 * * * *', datetime(2026, 1, 1, 0, 3)))
        self.assertFalse(self.matcher._cron_matches('1-30/5 * * * *', datetime(2026, 1, 1, 0, 31)))

    def test_step_every_15_minutes(self):
        """'*/15 * * * *' should match 0, 15, 30, 45."""
        for m in (0, 15, 30, 45):
            self.assertTrue(self.matcher._cron_matches('*/15 * * * *', datetime(2026, 1, 1, 0, m)),
                            msg='minute {0} should match */15'.format(m))
        for m in (1, 7, 14, 29, 44):
            self.assertFalse(self.matcher._cron_matches('*/15 * * * *', datetime(2026, 1, 1, 0, m)),
                             msg='minute {0} should NOT match */15'.format(m))

    # ------------------------------------------------------------------ #
    # Named months
    # ------------------------------------------------------------------ #
    def test_named_month_JAN(self):
        """'0 0 1 JAN *' should match January 1st."""
        self.assertTrue(self.matcher._cron_matches('0 0 1 JAN *', datetime(2026, 1, 1, 0, 0)))
        self.assertFalse(self.matcher._cron_matches('0 0 1 JAN *', datetime(2026, 2, 1, 0, 0)))

    # ------------------------------------------------------------------ #
    # Combined comma + range + step
    # ------------------------------------------------------------------ #
    def test_comma_with_range(self):
        """'0 9,17 * * MON-FRI' should match weekday 9:00 and 17:00."""
        # Wednesday 9:00
        self.assertTrue(self.matcher._cron_matches('0 9,17 * * MON-FRI', datetime(2026, 3, 25, 9, 0)))
        # Wednesday 17:00
        self.assertTrue(self.matcher._cron_matches('0 9,17 * * MON-FRI', datetime(2026, 3, 25, 17, 0)))
        # Saturday 9:00
        self.assertFalse(self.matcher._cron_matches('0 9,17 * * MON-FRI', datetime(2026, 3, 28, 9, 0)))

    # ------------------------------------------------------------------ #
    # Invalid expressions
    # ------------------------------------------------------------------ #
    def test_invalid_cron_too_few_parts(self):
        """'* * *' (3 fields) should return False."""
        self.assertFalse(self.matcher._cron_matches('* * *', datetime(2026, 1, 1, 0, 0)))

    def test_invalid_cron_too_many_parts(self):
        """'* * * * * *' (6 fields) should return False."""
        self.assertFalse(self.matcher._cron_matches('* * * * * *', datetime(2026, 1, 1, 0, 0)))


if __name__ == '__main__':
    unittest.main()
