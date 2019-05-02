import unittest
from freezegun import freeze_time
from unittest import mock
import pandas as pd
from datetime import datetime
from pandas.testing import assert_frame_equal

from src.services.kafka_service import *

class TestWebIdCache(unittest.TestCase):

    def setUp(self):
        raise ExceptoinNotImplemented

    def test_decode_tags_from_message(
        self, 
        mock_message):
        



        