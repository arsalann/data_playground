"""Parser checks based on representative FIFA post-match report text."""

import importlib.util
import unittest
from pathlib import Path


ASSET_PATH = Path(__file__).parents[1] / "assets" / "final_raw" / "fifa_match_facts.py"
SPEC = importlib.util.spec_from_file_location("fifa_match_facts", ASSET_PATH)
fifa_match_facts = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(fifa_match_facts)


KEY_STATS_TEXT = """
Match Summary - Key Statistics
Argentina 3 1 Switzerland
Total 54.7% 8.3% 37% Total
3 Goals 1
1.99 xG (Expected Goals) 0.44
22 (7) Attempts at Goal (On Target) 11 (5)
716 (629) Total Passes (Complete) 476 (404)
98 Completed Line Breaks 99
21 Ball Progressions 34
"""

PHASE_TEXT = """
Phases of Play
43% Build Up Unopposed 39%
23% Build Up Opposed 12%
13% Progression 15%
28% Final Third 16%
3% Long Ball 5%
9% Attacking Transition 16%
1% Counter Attack 2%
6% Set Piece 5%
2% High Press 7%
4% Mid Press 3%
1% Low Press 0%
3% High Block 5%
17% Mid Block 12%
30% Low Block 41%
4% Recovery 4%
16% Defensive Transition 9%
10% Counter-press 6%
"""

SHOT_TEXT = """
Attempts at Goal Argentina
Time Player Outcome Body Part Delivery Type
9 20Alexis MAC ALLISTER On Target - Goal Head Corner
49 26Nahuel MOLINA Off Target Right Foot Pass
"""


class FifaMatchFactParserTest(unittest.TestCase):
    def test_parses_core_key_statistics(self):
        values = fifa_match_facts._metric_values(KEY_STATS_TEXT)
        self.assertEqual(values["goals"], (3.0, 1.0))
        self.assertEqual(values["xg"], (1.99, 0.44))
        self.assertEqual(values["attempts_on_target"], (7.0, 5.0))
        self.assertEqual(values["completed_passes"], (629.0, 404.0))

    def test_parses_all_phase_labels_without_summing_overlapping_shares(self):
        values = fifa_match_facts._phase_values(PHASE_TEXT)
        self.assertEqual(len(values), 17)
        self.assertEqual(values["build_up_unopposed"], (43.0, 39.0, "in_possession"))
        self.assertEqual(values["low_block"], (30.0, 41.0, "out_of_possession"))

    def test_parses_shirt_number_when_pdf_text_joins_it_to_player_name(self):
        team, shots = fifa_match_facts._shot_rows(SHOT_TEXT)
        self.assertEqual(team, "Argentina")
        self.assertEqual(len(shots), 2)
        self.assertEqual(shots[0]["player_name"], "Alexis MAC ALLISTER")
        self.assertEqual(shots[0]["outcome_group"], "goal")
        self.assertEqual(shots[1]["delivery_type"], "Pass")


if __name__ == "__main__":
    unittest.main()
