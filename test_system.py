"""Comprehensive test suite for the Alpine Skiing Results Query System.

Tests cover:
1. Database operations (CRUD, search, filters, statistics)
2. Extraction pipeline (PDF text extraction, image extraction via VLM)
3. Parser (time conversion, JSON extraction, result parsing)
4. Ingestion pipeline (S3 listing, skip pattern filtering, end-to-end)
5. End-to-end integration (multi-file ingestion, query verification)
6. Data quality (dedup, time accuracy, Chinese text preservation)
"""

import os
import sys
import json
import sqlite3
import tempfile
import unittest
from unittest.mock import patch, MagicMock

# Ensure project root is on sys.path
PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, PROJECT_ROOT)


def _temp_db():
    """Create a temporary DB path for testing."""
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    return path


# =========================================================================
# Test 1: Database Operations
# =========================================================================
class TestDatabaseOperations(unittest.TestCase):
    """Test database schema creation, CRUD, search, and statistics."""

    def setUp(self):
        self.db_path = _temp_db()
        from database import init_db
        init_db(self.db_path)

    def tearDown(self):
        if os.path.exists(self.db_path):
            os.remove(self.db_path)
        wal = self.db_path + "-wal"
        shm = self.db_path + "-shm"
        if os.path.exists(wal):
            os.remove(wal)
        if os.path.exists(shm):
            os.remove(shm)

    def test_init_db_creates_tables(self):
        """init_db should create competitions, events, results, processed_files tables."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = {row[0] for row in cursor.fetchall()}
        conn.close()
        self.assertIn("competitions", tables)
        self.assertIn("events", tables)
        self.assertIn("results", tables)
        self.assertIn("processed_files", tables)

    def test_init_db_creates_indexes(self):
        """init_db should create performance indexes."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='index'")
        indexes = {row[0] for row in cursor.fetchall()}
        conn.close()
        self.assertIn("idx_results_name", indexes)
        self.assertIn("idx_results_event_id", indexes)
        self.assertIn("idx_events_competition_id", indexes)
        self.assertIn("idx_processed_files_s3_key", indexes)

    def test_init_db_idempotent(self):
        """Calling init_db twice should not raise errors."""
        from database import init_db
        init_db(self.db_path)  # second call
        # Should not raise

    def test_insert_competition(self):
        """insert_competition should return a valid id."""
        from database import insert_competition
        comp_id = insert_competition("25-26雪季", "测试比赛", "云顶", "2025-01-15", db_path=self.db_path)
        self.assertIsInstance(comp_id, int)
        self.assertGreater(comp_id, 0)

    def test_insert_competition_dedup(self):
        """Inserting the same season+name should return the existing id."""
        from database import insert_competition
        id1 = insert_competition("25-26雪季", "测试比赛", db_path=self.db_path)
        id2 = insert_competition("25-26雪季", "测试比赛", db_path=self.db_path)
        self.assertEqual(id1, id2)

    def test_insert_event(self):
        """insert_event should return a valid id."""
        from database import insert_competition, insert_event
        comp_id = insert_competition("25-26雪季", "测试比赛", db_path=self.db_path)
        event_id = insert_event(comp_id, "大回转", "女", "U11", "总成绩", "test.pdf", db_path=self.db_path)
        self.assertIsInstance(event_id, int)
        self.assertGreater(event_id, 0)

    def test_insert_and_query_results(self):
        """Insert results and verify they can be queried back."""
        from database import insert_competition, insert_event, insert_results, search_results
        comp_id = insert_competition("25-26雪季", "北京冠军赛", db_path=self.db_path)
        event_id = insert_event(comp_id, "大回转", "女", "U11", db_path=self.db_path)
        insert_results(event_id, [
            {"rank": 1, "bib": "13", "name": "姚知涵", "team": "顺义区",
             "total_time": "0:00:48.09", "total_seconds": 48.09, "status": "OK"},
            {"rank": 2, "bib": "7", "name": "李小明", "team": "海淀区",
             "total_time": "0:00:50.60", "total_seconds": 50.60, "status": "OK"},
        ], db_path=self.db_path)

        results = search_results(db_path=self.db_path)
        self.assertEqual(len(results), 2)
        self.assertEqual(results[0]["name"], "姚知涵")

    def test_search_results_season_filter(self):
        """search_results should filter by season."""
        from database import insert_competition, insert_event, insert_results, search_results
        comp1 = insert_competition("25-26雪季", "比赛A", db_path=self.db_path)
        comp2 = insert_competition("24-25雪季", "比赛B", db_path=self.db_path)
        e1 = insert_event(comp1, "大回转", "女", "U11", db_path=self.db_path)
        e2 = insert_event(comp2, "回转", "男", "U12", db_path=self.db_path)
        insert_results(e1, [{"rank": 1, "name": "选手A", "status": "OK"}], db_path=self.db_path)
        insert_results(e2, [{"rank": 1, "name": "选手B", "status": "OK"}], db_path=self.db_path)

        results = search_results({"season": "25-26雪季"}, db_path=self.db_path)
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["name"], "选手A")

    def test_search_results_name_filter(self):
        """search_results should support partial name matching."""
        from database import insert_competition, insert_event, insert_results, search_results
        comp = insert_competition("25-26雪季", "比赛A", db_path=self.db_path)
        ev = insert_event(comp, "大回转", "女", "U11", db_path=self.db_path)
        insert_results(ev, [
            {"rank": 1, "name": "姚知涵", "status": "OK"},
            {"rank": 2, "name": "李小明", "status": "OK"},
        ], db_path=self.db_path)

        results = search_results({"name": "姚"}, db_path=self.db_path)
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["name"], "姚知涵")

    def test_search_results_discipline_filter(self):
        """search_results should filter by discipline."""
        from database import insert_competition, insert_event, insert_results, search_results
        comp = insert_competition("25-26雪季", "比赛A", db_path=self.db_path)
        e1 = insert_event(comp, "大回转", "女", "U11", db_path=self.db_path)
        e2 = insert_event(comp, "回转", "女", "U11", db_path=self.db_path)
        insert_results(e1, [{"rank": 1, "name": "选手A", "status": "OK"}], db_path=self.db_path)
        insert_results(e2, [{"rank": 1, "name": "选手B", "status": "OK"}], db_path=self.db_path)

        results = search_results({"discipline": "大回转"}, db_path=self.db_path)
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["name"], "选手A")

    def test_search_results_multiple_filters(self):
        """search_results should support combining multiple filters."""
        from database import insert_competition, insert_event, insert_results, search_results
        comp = insert_competition("25-26雪季", "比赛A", db_path=self.db_path)
        e1 = insert_event(comp, "大回转", "女", "U11", db_path=self.db_path)
        e2 = insert_event(comp, "大回转", "男", "U11", db_path=self.db_path)
        insert_results(e1, [{"rank": 1, "name": "选手A", "status": "OK"}], db_path=self.db_path)
        insert_results(e2, [{"rank": 1, "name": "选手B", "status": "OK"}], db_path=self.db_path)

        results = search_results({"discipline": "大回转", "gender": "女"}, db_path=self.db_path)
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["name"], "选手A")

    def test_search_results_no_match(self):
        """search_results should return empty list when no records match."""
        from database import search_results
        results = search_results({"season": "不存在"}, db_path=self.db_path)
        self.assertEqual(len(results), 0)

    def test_get_athlete_history(self):
        """get_athlete_history should return all results for an athlete across competitions."""
        from database import insert_competition, insert_event, insert_results, get_athlete_history
        comp1 = insert_competition("25-26雪季", "比赛A", db_path=self.db_path)
        comp2 = insert_competition("24-25雪季", "比赛B", db_path=self.db_path)
        e1 = insert_event(comp1, "大回转", "女", "U11", db_path=self.db_path)
        e2 = insert_event(comp2, "回转", "女", "U11", db_path=self.db_path)
        insert_results(e1, [{"rank": 1, "name": "姚知涵", "status": "OK"}], db_path=self.db_path)
        insert_results(e2, [{"rank": 3, "name": "姚知涵", "status": "OK"}], db_path=self.db_path)
        insert_results(e1, [{"rank": 2, "name": "其他选手", "status": "OK"}], db_path=self.db_path)

        history = get_athlete_history("姚知涵", db_path=self.db_path)
        self.assertEqual(len(history), 2)
        for r in history:
            self.assertEqual(r["name"], "姚知涵")

    def test_get_athlete_history_partial_match(self):
        """get_athlete_history should support partial name matching."""
        from database import insert_competition, insert_event, insert_results, get_athlete_history
        comp = insert_competition("25-26雪季", "比赛A", db_path=self.db_path)
        ev = insert_event(comp, "大回转", "女", "U11", db_path=self.db_path)
        insert_results(ev, [
            {"rank": 1, "name": "姚知涵", "status": "OK"},
            {"rank": 2, "name": "姚明", "status": "OK"},
        ], db_path=self.db_path)

        history = get_athlete_history("姚", db_path=self.db_path)
        self.assertEqual(len(history), 2)

    def test_get_filter_options(self):
        """get_filter_options should return all unique values for dropdowns."""
        from database import insert_competition, insert_event, get_filter_options
        comp = insert_competition("25-26雪季", "北京冠军赛", db_path=self.db_path)
        insert_event(comp, "大回转", "女", "U11", db_path=self.db_path)
        insert_event(comp, "回转", "男", "U12", db_path=self.db_path)

        opts = get_filter_options(db_path=self.db_path)
        self.assertIn("25-26雪季", opts["seasons"])
        self.assertIn("北京冠军赛", opts["competitions"])
        self.assertIn("大回转", opts["disciplines"])
        self.assertIn("回转", opts["disciplines"])
        self.assertIn("U11", opts["age_groups"])
        self.assertIn("U12", opts["age_groups"])
        self.assertIn("女", opts["genders"])
        self.assertIn("男", opts["genders"])

    def test_get_statistics(self):
        """get_statistics should return correct counts."""
        from database import (
            insert_competition, insert_event, insert_results,
            mark_file_processed, get_statistics
        )
        comp = insert_competition("25-26雪季", "测试", db_path=self.db_path)
        ev = insert_event(comp, "大回转", "女", "U11", db_path=self.db_path)
        insert_results(ev, [
            {"rank": 1, "name": "选手A", "status": "OK"},
            {"rank": 2, "name": "选手B", "status": "OK"},
        ], db_path=self.db_path)
        mark_file_processed("test1.pdf", "pdf", "success", db_path=self.db_path)
        mark_file_processed("test2.pdf", "pdf", "failed", "error", db_path=self.db_path)

        stats = get_statistics(db_path=self.db_path)
        self.assertEqual(stats["competitions"], 1)
        self.assertEqual(stats["events"], 1)
        self.assertEqual(stats["results"], 2)
        self.assertEqual(stats["athletes"], 2)
        self.assertEqual(stats["files_processed"], 1)
        self.assertEqual(stats["files_failed"], 1)

    def test_mark_file_processed(self):
        """mark_file_processed should record file status."""
        from database import mark_file_processed, is_file_processed
        mark_file_processed("s3://test/file.pdf", "pdf", "success", db_path=self.db_path)
        self.assertTrue(is_file_processed("s3://test/file.pdf", db_path=self.db_path))

    def test_is_file_processed_failed(self):
        """is_file_processed should return False for failed files."""
        from database import mark_file_processed, is_file_processed
        mark_file_processed("s3://test/file.pdf", "pdf", "failed", "some error", db_path=self.db_path)
        self.assertFalse(is_file_processed("s3://test/file.pdf", db_path=self.db_path))

    def test_is_file_processed_unknown(self):
        """is_file_processed should return False for unknown files."""
        from database import is_file_processed
        self.assertFalse(is_file_processed("s3://unknown/file.pdf", db_path=self.db_path))

    def test_mark_file_processed_replace(self):
        """Marking a file again should update (replace) the record."""
        from database import mark_file_processed, is_file_processed
        mark_file_processed("s3://test/file.pdf", "pdf", "failed", "err", db_path=self.db_path)
        self.assertFalse(is_file_processed("s3://test/file.pdf", db_path=self.db_path))
        mark_file_processed("s3://test/file.pdf", "pdf", "success", db_path=self.db_path)
        self.assertTrue(is_file_processed("s3://test/file.pdf", db_path=self.db_path))


# =========================================================================
# Test 2: Extraction Pipeline
# =========================================================================
class TestExtractionPipeline(unittest.TestCase):
    """Test PDF text extraction and image extraction."""

    def test_extract_from_pdf_text_based(self):
        """extract_from_pdf should return text from text-based PDFs without calling VLM."""
        from extractor import extract_from_pdf
        pdf_path = os.path.join(PROJECT_ROOT, "samples", "sample_text2.pdf")
        if not os.path.exists(pdf_path):
            self.skipTest("sample_text2.pdf not found")

        # Patch call_qwen3_vl to ensure it's NOT called for text PDFs
        with patch("extractor.call_qwen3_vl") as mock_vlm:
            result = extract_from_pdf(pdf_path)
            mock_vlm.assert_not_called()

        self.assertIsInstance(result, str)
        self.assertGreater(len(result), 50)
        # Should contain Chinese skiing competition text
        self.assertIn("大回转", result)
        # Should NOT be prefixed with VLM marker
        self.assertFalse(result.startswith("[VLM_EXTRACTED]"))

    def test_extract_from_pdf_contains_results(self):
        """Extracted text should contain athlete result data."""
        from extractor import extract_from_pdf
        pdf_path = os.path.join(PROJECT_ROOT, "samples", "sample_text2.pdf")
        if not os.path.exists(pdf_path):
            self.skipTest("sample_text2.pdf not found")

        with patch("extractor.call_qwen3_vl"):
            result = extract_from_pdf(pdf_path)

        # Should contain competition-related text
        has_results = any(kw in result for kw in ["成绩", "名次", "总成绩", "成绩公告"])
        self.assertTrue(has_results, f"Expected result keywords in extracted text")

    def test_extract_from_image_calls_vlm(self):
        """extract_from_image should call the VLM API."""
        from extractor import extract_from_image
        img_path = os.path.join(PROJECT_ROOT, "samples", "sample_img1.jpg")
        if not os.path.exists(img_path):
            self.skipTest("sample_img1.jpg not found")

        mock_json = json.dumps({
            "competition": "测试比赛",
            "results": [{"rank": 1, "name": "测试选手", "status": "OK"}]
        }, ensure_ascii=False)

        with patch("extractor.call_qwen3_vl", return_value=mock_json) as mock_vlm:
            result = extract_from_image(img_path)
            mock_vlm.assert_called_once()

        self.assertIn("[VLM_EXTRACTED]", result)
        self.assertIn("测试比赛", result)

    def test_extract_from_pdf_all_samples(self):
        """All text-based sample PDFs should extract >50 chars without VLM."""
        from extractor import extract_from_pdf
        text_pdfs = ["sample_text1.pdf", "sample_text2.pdf", "sample_text3.pdf"]
        for name in text_pdfs:
            pdf_path = os.path.join(PROJECT_ROOT, "samples", name)
            if not os.path.exists(pdf_path):
                continue
            with patch("extractor.call_qwen3_vl") as mock_vlm:
                result = extract_from_pdf(pdf_path)
                mock_vlm.assert_not_called()
            self.assertGreater(len(result), 50, f"{name} should have >50 chars of text")


# =========================================================================
# Test 3: Parser
# =========================================================================
class TestParser(unittest.TestCase):
    """Test time conversion and result parsing."""

    def test_time_to_seconds_simple(self):
        """time_to_seconds('32.40') should return 32.4."""
        from parser import time_to_seconds
        self.assertAlmostEqual(time_to_seconds("32.40"), 32.40, places=2)

    def test_time_to_seconds_hhmmss(self):
        """time_to_seconds('0:00:24.07') should return 24.07."""
        from parser import time_to_seconds
        self.assertAlmostEqual(time_to_seconds("0:00:24.07"), 24.07, places=2)

    def test_time_to_seconds_mmss(self):
        """time_to_seconds('00:30.90') should return 30.90."""
        from parser import time_to_seconds
        self.assertAlmostEqual(time_to_seconds("00:30.90"), 30.90, places=2)

    def test_time_to_seconds_mmss_over_minute(self):
        """time_to_seconds('01:03.32') should return 63.32."""
        from parser import time_to_seconds
        self.assertAlmostEqual(time_to_seconds("01:03.32"), 63.32, places=2)

    def test_time_to_seconds_mmss_short_minutes(self):
        """time_to_seconds('1:39.58') should return 99.58."""
        from parser import time_to_seconds
        self.assertAlmostEqual(time_to_seconds("1:39.58"), 99.58, places=2)

    def test_time_to_seconds_two_minutes(self):
        """time_to_seconds('02:13.23') should return 133.23."""
        from parser import time_to_seconds
        self.assertAlmostEqual(time_to_seconds("02:13.23"), 133.23, places=2)

    def test_time_to_seconds_none_input(self):
        """time_to_seconds(None) should return None."""
        from parser import time_to_seconds
        self.assertIsNone(time_to_seconds(None))

    def test_time_to_seconds_empty(self):
        """time_to_seconds('') should return None."""
        from parser import time_to_seconds
        self.assertIsNone(time_to_seconds(""))

    def test_time_to_seconds_dnf(self):
        """time_to_seconds('DNF') should return None."""
        from parser import time_to_seconds
        self.assertIsNone(time_to_seconds("DNF"))

    def test_time_to_seconds_dns(self):
        """time_to_seconds('DNS') should return None."""
        from parser import time_to_seconds
        self.assertIsNone(time_to_seconds("DNS"))

    def test_time_to_seconds_dq(self):
        """time_to_seconds('DQ') should return None."""
        from parser import time_to_seconds
        self.assertIsNone(time_to_seconds("DQ"))

    def test_parse_results_vlm_json(self):
        """parse_results should parse VLM-extracted JSON directly."""
        from parser import parse_results

        vlm_output = "[VLM_EXTRACTED]\n" + json.dumps({
            "competition": "2025北京市冠军赛",
            "date": "2025-01-15",
            "venue": "密苑云顶",
            "discipline": "大回转",
            "gender": "女",
            "age_group": "U11",
            "round_type": "总成绩",
            "results": [
                {"rank": 1, "bib": "13", "name": "姚知涵", "team": "顺义区",
                 "run1_time": "0:00:24.07", "run2_time": "0:00:24.02",
                 "total_time": "0:00:48.09", "time_diff": "0:00:00.00", "status": "OK"},
                {"rank": None, "bib": "22", "name": "王大力", "team": "朝阳区",
                 "status": "DNF"},
            ]
        })

        result = parse_results(vlm_output)
        self.assertEqual(result["competition"], "2025北京市冠军赛")
        self.assertEqual(result["discipline"], "大回转")
        self.assertEqual(len(result["results"]), 2)

        # Check first result
        r1 = result["results"][0]
        self.assertEqual(r1["rank"], 1)
        self.assertEqual(r1["name"], "姚知涵")
        self.assertEqual(r1["status"], "OK")
        self.assertAlmostEqual(r1["total_seconds"], 48.09, places=2)

        # Check DNF result
        r2 = result["results"][1]
        self.assertIsNone(r2["rank"])
        self.assertEqual(r2["status"], "DNF")

    def test_parse_results_required_fields(self):
        """Parsed results should contain all required fields."""
        from parser import parse_results

        vlm_output = "[VLM_EXTRACTED]\n" + json.dumps({
            "competition": "测试",
            "discipline": "回转",
            "gender": "男",
            "age_group": "U12",
            "results": [
                {"rank": 1, "bib": "1", "name": "选手", "team": "队伍",
                 "total_time": "0:00:30.00", "status": "OK"}
            ]
        })

        result = parse_results(vlm_output)
        # Top-level fields
        self.assertIn("competition", result)
        self.assertIn("discipline", result)
        self.assertIn("gender", result)
        self.assertIn("age_group", result)
        self.assertIn("results", result)

        # Result entry fields
        r = result["results"][0]
        for field in ["rank", "bib", "name", "team", "status"]:
            self.assertIn(field, r)

    def test_parse_results_dnf_dns_dq(self):
        """Parser should correctly handle DNF/DNS/DQ status."""
        from parser import parse_results

        vlm_output = "[VLM_EXTRACTED]\n" + json.dumps({
            "competition": "测试",
            "results": [
                {"rank": 1, "name": "OK选手", "status": "OK", "total_time": "30.00"},
                {"rank": None, "name": "DNF选手", "status": "DNF"},
                {"rank": None, "name": "DNS选手", "status": "DNS"},
                {"rank": None, "name": "DQ选手", "status": "DQ"},
            ]
        })

        result = parse_results(vlm_output)
        statuses = {r["name"]: r["status"] for r in result["results"]}
        self.assertEqual(statuses["OK选手"], "OK")
        self.assertEqual(statuses["DNF选手"], "DNF")
        self.assertEqual(statuses["DNS选手"], "DNS")
        self.assertEqual(statuses["DQ选手"], "DQ")

    def test_parse_results_empty_input(self):
        """parse_results should handle empty input gracefully."""
        from parser import parse_results
        result = parse_results("")
        self.assertIn("error", result)

    def test_parse_results_none_input(self):
        """parse_results should handle None input gracefully."""
        from parser import parse_results
        result = parse_results(None)
        self.assertIn("error", result)

    def test_extract_json_with_markdown_fences(self):
        """_extract_json should strip markdown code fences."""
        from parser import _extract_json
        text = '```json\n{"competition": "test", "results": []}\n```'
        result = _extract_json(text)
        self.assertEqual(result["competition"], "test")

    def test_extract_json_with_thinking_tags(self):
        """_extract_json should strip <think> tags."""
        from parser import _extract_json
        text = '<think>reasoning here</think>\n{"competition": "test", "results": []}'
        result = _extract_json(text)
        self.assertEqual(result["competition"], "test")


# =========================================================================
# Test 4: Ingestion Pipeline
# =========================================================================
class TestIngestionPipeline(unittest.TestCase):
    """Test S3 listing, skip pattern filtering, and ingestion orchestration."""

    def test_list_s3_files(self):
        """list_s3_files should return a list of supported file keys from S3."""
        from ingestion import list_s3_files

        mock_paginator = MagicMock()
        mock_paginator.paginate.return_value = [
            {"Contents": [
                {"Key": "ski/比赛成绩汇总/25-26雪季/比赛/results.pdf"},
                {"Key": "ski/比赛成绩汇总/25-26雪季/比赛/photo.jpg"},
                {"Key": "ski/比赛成绩汇总/25-26雪季/比赛/data.xls"},
                {"Key": "ski/比赛成绩汇总/25-26雪季/比赛/image.png"},
            ]}
        ]

        mock_client = MagicMock()
        mock_client.get_paginator.return_value = mock_paginator

        with patch("ingestion._get_s3_client", return_value=mock_client):
            files = list_s3_files()

        # .xls not in SUPPORTED_EXTENSIONS
        self.assertEqual(len(files), 3)
        self.assertTrue(any(f.endswith(".pdf") for f in files))
        self.assertTrue(any(f.endswith(".jpg") for f in files))
        self.assertTrue(any(f.endswith(".png") for f in files))

    def test_should_process_skip_patterns(self):
        """should_process should skip files matching skip patterns."""
        from ingestion import should_process

        with patch("ingestion.is_file_processed", return_value=False):
            # Normal result file
            self.assertTrue(should_process("ski/比赛成绩汇总/25-26雪季/比赛/高山滑雪大回转_U11_女子.pdf"))
            # Start order - should skip
            self.assertFalse(should_process("ski/比赛成绩汇总/25-26雪季/比赛/出发顺序_大回转_U11_女子.pdf"))
            # Order book - should skip
            self.assertFalse(should_process("ski/比赛成绩汇总/25-26雪季/比赛/秩序册.pdf"))

    def test_should_process_already_processed(self):
        """should_process should return False for already-processed files."""
        from ingestion import should_process

        with patch("ingestion.is_file_processed", return_value=True):
            self.assertFalse(should_process("ski/比赛成绩汇总/some/file.pdf"))

    def test_infer_season(self):
        """_infer_season should extract season from S3 key."""
        from ingestion import _infer_season
        self.assertEqual(
            _infer_season("ski/比赛成绩汇总/25-26雪季/比赛/file.pdf"),
            "25-26雪季"
        )
        self.assertEqual(
            _infer_season("ski/比赛成绩汇总/24-25雪季/其他/file.pdf"),
            "24-25雪季"
        )
        # No season in path
        self.assertIsNone(
            _infer_season("ski/比赛成绩汇总/朝阳区锦标赛/file.pdf")
        )

    def test_get_file_type(self):
        """_get_file_type should return correct extension without dot."""
        from ingestion import _get_file_type
        self.assertEqual(_get_file_type("path/to/file.pdf"), "pdf")
        self.assertEqual(_get_file_type("path/to/file.JPG"), "jpg")
        self.assertEqual(_get_file_type("path/to/file.png"), "png")
        self.assertEqual(_get_file_type("path/to/file.heic"), "heic")

    def test_process_file_end_to_end(self):
        """process_file should download, extract, parse, and insert into DB."""
        from ingestion import process_file
        from database import init_db, search_results

        db_path = _temp_db()
        init_db(db_path)

        s3_key = "ski/比赛成绩汇总/25-26雪季/北京冠军赛/大回转_U11_女子.pdf"
        sample_pdf = os.path.join(PROJECT_ROOT, "samples", "sample_text2.pdf")

        mock_parsed = {
            "competition": "2025北京市冠军赛",
            "date": "2025-01-15",
            "discipline": "大回转",
            "gender": "女",
            "age_group": "U11",
            "round_type": "总成绩",
            "results": [
                {"rank": 1, "bib": "13", "name": "姚知涵", "team": "顺义区",
                 "run1_time": "0:00:24.07", "run2_time": "0:00:24.02",
                 "total_time": "0:00:48.09", "status": "OK"},
            ]
        }

        with patch("ingestion.download_file", return_value=sample_pdf), \
             patch("ingestion.extract_from_pdf", return_value="mock text"), \
             patch("ingestion.parse_results", return_value=mock_parsed), \
             patch("ingestion.time_to_seconds", side_effect=lambda t: 48.09 if t else None), \
             patch("ingestion.insert_competition", return_value=1) as mock_ins_comp, \
             patch("ingestion.insert_event", return_value=1) as mock_ins_evt, \
             patch("ingestion.insert_results") as mock_ins_res, \
             patch("ingestion.mark_file_processed") as mock_mark:

            result = process_file(s3_key)

        self.assertTrue(result)
        mock_ins_comp.assert_called_once()
        mock_ins_evt.assert_called_once()
        mock_ins_res.assert_called_once()
        mock_mark.assert_called_with(s3_key, "pdf", "success")

        # Cleanup
        os.remove(db_path)

    def test_run_ingestion_with_progress(self):
        """run_ingestion should call progress_callback at each step."""
        from ingestion import run_ingestion

        progress_calls = []

        def track_progress(current, total, key):
            progress_calls.append((current, total, key))

        mock_files = [
            "ski/比赛成绩汇总/25-26雪季/比赛/file1.pdf",
            "ski/比赛成绩汇总/25-26雪季/比赛/file2.pdf",
        ]

        with patch("ingestion.init_db"), \
             patch("ingestion.list_s3_files", return_value=mock_files), \
             patch("ingestion.should_process", return_value=True), \
             patch("ingestion.process_file", return_value=True):

            counts = run_ingestion(max_files=2, progress_callback=track_progress)

        self.assertEqual(counts["processed"], 2)
        self.assertEqual(counts["failed"], 0)
        # Should have progress calls for each file + completion
        self.assertGreater(len(progress_calls), 0)


# =========================================================================
# Test 5: End-to-End Integration
# =========================================================================
class TestEndToEndIntegration(unittest.TestCase):
    """Test full pipeline from extraction to query."""

    def test_pdf_extract_to_db_query(self):
        """Extract text from a real PDF, mock the LLM parsing, and verify DB query."""
        from extractor import extract_from_pdf
        from database import init_db, insert_competition, insert_event, insert_results, search_results

        db_path = _temp_db()
        init_db(db_path)

        # Extract from real PDF (text-based, no VLM needed)
        pdf_path = os.path.join(PROJECT_ROOT, "samples", "sample_text2.pdf")
        if not os.path.exists(pdf_path):
            self.skipTest("sample_text2.pdf not found")

        with patch("extractor.call_qwen3_vl"):
            extracted = extract_from_pdf(pdf_path)

        self.assertGreater(len(extracted), 50)

        # Simulate parsed results (as if LLM parsed the text)
        comp_id = insert_competition("25-26雪季", "2025北京市冠军赛", "密苑云顶", "2025-01-15", db_path=db_path)
        event_id = insert_event(comp_id, "大回转", "女", "U11", "总成绩", "test.pdf", db_path=db_path)
        insert_results(event_id, [
            {"rank": 1, "bib": "13", "name": "姚知涵", "team": "顺义区",
             "run1_time": "0:00:24.07", "run2_time": "0:00:24.02",
             "total_time": "0:00:48.09", "run1_seconds": 24.07, "run2_seconds": 24.02,
             "total_seconds": 48.09, "time_diff": "0:00:00.00", "status": "OK"},
            {"rank": 2, "bib": "7", "name": "李小明", "team": "海淀区",
             "run1_time": "0:00:25.10", "run2_time": "0:00:25.50",
             "total_time": "0:00:50.60", "run1_seconds": 25.10, "run2_seconds": 25.50,
             "total_seconds": 50.60, "time_diff": "0:00:02.51", "status": "OK"},
            {"rank": None, "bib": "22", "name": "王大力", "team": "朝阳区",
             "status": "DNF"},
        ], db_path=db_path)

        # Verify queries
        all_results = search_results(db_path=db_path)
        self.assertEqual(len(all_results), 3)

        # Filter by name
        name_results = search_results({"name": "姚"}, db_path=db_path)
        self.assertEqual(len(name_results), 1)
        self.assertEqual(name_results[0]["name"], "姚知涵")

        # Filter by discipline
        disc_results = search_results({"discipline": "大回转"}, db_path=db_path)
        self.assertEqual(len(disc_results), 3)

        # Cleanup
        os.remove(db_path)

    def test_multi_competition_integration(self):
        """Insert multiple competitions and verify cross-competition queries."""
        from database import (
            init_db, insert_competition, insert_event, insert_results,
            search_results, get_athlete_history, get_filter_options, get_statistics
        )

        db_path = _temp_db()
        init_db(db_path)

        # Competition 1
        c1 = insert_competition("25-26雪季", "北京冠军赛", "密苑云顶", "2025-01-15", db_path=db_path)
        e1 = insert_event(c1, "大回转", "女", "U11", "总成绩", db_path=db_path)
        insert_results(e1, [
            {"rank": 1, "name": "姚知涵", "team": "顺义区", "total_seconds": 48.09, "status": "OK"},
            {"rank": 2, "name": "李晓雪", "team": "海淀区", "total_seconds": 50.60, "status": "OK"},
        ], db_path=db_path)

        # Competition 2
        c2 = insert_competition("25-26雪季", "北京锦标赛", "万龙", "2025-02-20", db_path=db_path)
        e2 = insert_event(c2, "回转", "女", "U11", "总成绩", db_path=db_path)
        insert_results(e2, [
            {"rank": 1, "name": "李晓雪", "team": "海淀区", "total_seconds": 62.33, "status": "OK"},
            {"rank": 2, "name": "姚知涵", "team": "顺义区", "total_seconds": 65.10, "status": "OK"},
        ], db_path=db_path)

        # Competition 3, different season
        c3 = insert_competition("24-25雪季", "冬季测试赛", "崇礼", "2024-12-10", db_path=db_path)
        e3 = insert_event(c3, "大回转", "男", "U12", db_path=db_path)
        insert_results(e3, [
            {"rank": 1, "name": "张小龙", "team": "朝阳区", "total_seconds": 55.00, "status": "OK"},
        ], db_path=db_path)

        # Verify stats
        stats = get_statistics(db_path=db_path)
        self.assertEqual(stats["competitions"], 3)
        self.assertEqual(stats["events"], 3)
        self.assertEqual(stats["results"], 5)
        self.assertEqual(stats["athletes"], 3)

        # Verify filter options
        opts = get_filter_options(db_path=db_path)
        self.assertEqual(len(opts["seasons"]), 2)
        self.assertEqual(len(opts["competitions"]), 3)
        self.assertEqual(len(opts["disciplines"]), 2)

        # Cross-competition athlete history
        history = get_athlete_history("姚知涵", db_path=db_path)
        self.assertEqual(len(history), 2)

        history_li = get_athlete_history("李晓雪", db_path=db_path)
        self.assertEqual(len(history_li), 2)

        # Season filter
        results_25 = search_results({"season": "25-26雪季"}, db_path=db_path)
        self.assertEqual(len(results_25), 4)

        results_24 = search_results({"season": "24-25雪季"}, db_path=db_path)
        self.assertEqual(len(results_24), 1)

        # Cleanup
        os.remove(db_path)

    def test_streamlit_app_syntax(self):
        """app.py should be valid Python (syntax check)."""
        import ast
        app_path = os.path.join(PROJECT_ROOT, "app.py")
        with open(app_path) as f:
            source = f.read()
        # Should not raise SyntaxError
        ast.parse(source)

    def test_all_modules_syntax(self):
        """All Python modules should parse without syntax errors."""
        import ast
        for module in ["config.py", "database.py", "extractor.py", "parser.py", "ingestion.py", "app.py"]:
            path = os.path.join(PROJECT_ROOT, module)
            if os.path.exists(path):
                with open(path) as f:
                    ast.parse(f.read())


# =========================================================================
# Test 6: Data Quality
# =========================================================================
class TestDataQuality(unittest.TestCase):
    """Test dedup, time accuracy, and Chinese text preservation."""

    def test_no_duplicate_competitions(self):
        """Inserting same competition twice should not create duplicates."""
        from database import init_db, insert_competition, get_connection

        db_path = _temp_db()
        init_db(db_path)

        insert_competition("25-26雪季", "北京冠军赛", db_path=db_path)
        insert_competition("25-26雪季", "北京冠军赛", db_path=db_path)
        insert_competition("25-26雪季", "北京冠军赛", "不同场地", db_path=db_path)

        conn = get_connection(db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) AS cnt FROM competitions")
        count = cursor.fetchone()["cnt"]
        conn.close()

        self.assertEqual(count, 1, "Should have only 1 competition, not duplicates")
        os.remove(db_path)

    def test_time_conversion_accuracy(self):
        """Verify time conversions are numerically accurate."""
        from parser import time_to_seconds

        test_cases = [
            ("32.40", 32.40),
            ("0:00:24.07", 24.07),
            ("00:30.90", 30.90),
            ("01:03.32", 63.32),
            ("1:39.58", 99.58),
            ("02:13.23", 133.23),
            ("0:01:30.00", 90.00),
            ("00:47.17", 47.17),
        ]
        for time_str, expected in test_cases:
            result = time_to_seconds(time_str)
            self.assertAlmostEqual(result, expected, places=2,
                                   msg=f"time_to_seconds('{time_str}') = {result}, expected {expected}")

    def test_chinese_text_preserved_in_db(self):
        """Chinese characters should be stored and retrieved correctly from SQLite."""
        from database import init_db, insert_competition, insert_event, insert_results, search_results

        db_path = _temp_db()
        init_db(db_path)

        comp_id = insert_competition("25-26雪季", "2025年北京市青少年滑雪冠军赛",
                                     "密苑云顶乐园", "2025-01-15", db_path=db_path)
        event_id = insert_event(comp_id, "大回转", "女", "U11", "总成绩", db_path=db_path)
        insert_results(event_id, [
            {"rank": 1, "name": "姚知涵", "team": "顺义区代表队", "status": "OK"},
        ], db_path=db_path)

        results = search_results(db_path=db_path)
        self.assertEqual(len(results), 1)
        r = results[0]
        self.assertEqual(r["name"], "姚知涵")
        self.assertEqual(r["team"], "顺义区代表队")
        self.assertEqual(r["competition"], "2025年北京市青少年滑雪冠军赛")
        self.assertEqual(r["season"], "25-26雪季")
        self.assertEqual(r["discipline"], "大回转")
        self.assertEqual(r["venue"], "密苑云顶乐园")

        os.remove(db_path)

    def test_dnf_results_have_null_times(self):
        """DNF/DNS/DQ results should have None for time seconds."""
        from parser import time_to_seconds

        # DNF athletes typically have no time
        self.assertIsNone(time_to_seconds(None))
        self.assertIsNone(time_to_seconds(""))
        self.assertIsNone(time_to_seconds("DNF"))
        self.assertIsNone(time_to_seconds("DNS"))

    def test_rank_ordering(self):
        """Search results should be ordered by rank ASC (within same date)."""
        from database import init_db, insert_competition, insert_event, insert_results, search_results

        db_path = _temp_db()
        init_db(db_path)

        comp = insert_competition("25-26雪季", "比赛", venue=None, date="2025-01-15", db_path=db_path)
        ev = insert_event(comp, "大回转", "女", "U11", db_path=db_path)
        insert_results(ev, [
            {"rank": 3, "name": "第三", "status": "OK"},
            {"rank": 1, "name": "第一", "status": "OK"},
            {"rank": 2, "name": "第二", "status": "OK"},
        ], db_path=db_path)

        results = search_results(db_path=db_path)
        ranks = [r["rank"] for r in results]
        self.assertEqual(ranks, [1, 2, 3], "Results should be ordered by rank ASC")

        os.remove(db_path)


if __name__ == "__main__":
    unittest.main(verbosity=2)
