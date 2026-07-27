# Copyright (C) 2026 East Asian Observatory
# All Rights Reserved.
#
# This program is free software; you can redistribute it and/or modify it under
# the terms of the GNU General Public License as published by the Free Software
# Foundation; either version 2 of the License, or (at your option) any later
# version.
#
# This program is distributed in the hope that it will be useful,but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
# FOR A PARTICULAR PURPOSE. See the GNU General Public License for more
# details.
#
# You should have received a copy of the GNU General Public License along with
# this program; if not, write to the Free Software Foundation, Inc.,51 Franklin
# Street, Fifth Floor, Boston, MA  02110-1301, USA

from unittest import TestCase

from jsa_proc.error import JSAProcError
from jsa_proc.qa_state import JSAQAState


class QAStateTestCase(TestCase):
    def test_state_info(self):
        states = {
            JSAQAState.UNKNOWN: 'Unknown',
            JSAQAState.INVALID: 'Invalid',
            JSAQAState.BAD: 'Bad',
            JSAQAState.QUESTIONABLE: 'Questionable',
            JSAQAState.GOOD: 'Good',
        }

        for (state, name) in states.items():
            self.assertEqual(len(state), 1)
            self.assertTrue(JSAQAState.is_valid(state))
            self.assertEqual(JSAQAState.get_name(state), name)

        self.assertFalse(JSAQAState.is_valid('!'))
        with self.assertRaises(JSAProcError):
            JSAQAState.get_name('!')

    def test_coalesce(self):
        self.assertEqual(JSAQAState.coalesce([]), JSAQAState.UNKNOWN)

        self.assertEqual(JSAQAState.coalesce([
            JSAQAState.GOOD,
        ]), JSAQAState.GOOD)

        self.assertEqual(JSAQAState.coalesce([
            JSAQAState.GOOD,
            JSAQAState.BAD,
            JSAQAState.GOOD,
        ]), JSAQAState.BAD)

        self.assertEqual(JSAQAState.coalesce([
            JSAQAState.GOOD,
            JSAQAState.UNKNOWN,
            JSAQAState.GOOD,
        ]), JSAQAState.UNKNOWN)

        self.assertEqual(JSAQAState.coalesce([
            JSAQAState.GOOD,
            JSAQAState.QUESTIONABLE,
            JSAQAState.GOOD,
        ]), JSAQAState.QUESTIONABLE)

        self.assertEqual(JSAQAState.coalesce([
            JSAQAState.GOOD,
            JSAQAState.QUESTIONABLE,
            JSAQAState.BAD,
            JSAQAState.UNKNOWN,
            JSAQAState.INVALID,
            JSAQAState.GOOD,
        ]), JSAQAState.INVALID)
