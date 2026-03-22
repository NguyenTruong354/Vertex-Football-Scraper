import pytest
from unittest.mock import patch, MagicMock
from services.player_trend import analyze_all_players, run_and_save

@patch('db.config_db.get_connection')
def test_tc02_international_break_simulation(mock_get_conn):
    """TC-02: International break simulation (0 matches). Verify early exit."""
    mock_conn = MagicMock()
    mock_cur = MagicMock()
    mock_conn.cursor.return_value = mock_cur
    mock_get_conn.return_value = mock_conn
    
    # fetchone returns 0 active matches
    mock_cur.fetchone.return_value = [0]
    
    result = analyze_all_players('EPL', force=False)
    assert result == []
    
    # Only 1 query executed: the early exit check
    assert mock_cur.execute.call_count == 1
    assert "SELECT COUNT(*) FROM match_stats" in mock_cur.execute.call_args_list[0][0][0]

@patch('services.insight_producer.enqueue_player_trend')
@patch('db.config_db.get_connection')
def test_tc01_normal_gameweek(mock_get_conn, mock_enqueue):
    """TC-01: Mock players active in last 5 days. Verify participation gate."""
    mock_conn = MagicMock()
    mock_cur = MagicMock()
    mock_conn.cursor.return_value = mock_cur
    mock_get_conn.return_value = mock_conn
    
    mock_cur.fetchone.return_value = [10] # 10 active matches
    
    mock_cur.fetchall.side_effect = [
        [], # existing insights
        [(1, "Player A", [1,0,0], [0,0,0], [0.1,0,0], [0,0,0], [1,0,0], [0,0,0], 3)] # stats
    ]
    
    analyze_all_players('EPL', force=False)
    
    # 3 Queries: early exit -> existing insights -> main stats CTE
    assert mock_cur.execute.call_count == 3
    main_sql = mock_cur.execute.call_args_list[2][0][0]
    
    assert "active_players AS" in main_sql
    assert "INNER JOIN active_players" in main_sql

@patch('db.config_db.get_connection')
def test_tc03_skip_write_dirty_check(mock_get_conn):
    """TC-03: Run analyze with same data. Verify LLM is skipped."""
    mock_conn = MagicMock()
    mock_cur = MagicMock()
    mock_conn.cursor.return_value = mock_cur
    mock_get_conn.return_value = mock_conn
    
    mock_cur.fetchone.return_value = [5] # past matches exist
    
    # Existing insight perfectly matches calculated trend
    mock_cur.fetchall.side_effect = [
        [(1, "NEUTRAL", 0.0)], # DB has NEUTRAL, score 0
        [(1, "Player A", [0,0,0], [0,0,0], [0,0,0], [0,0,0], [0,0,0], [0,0,0], 3)] # calculated is 0
    ]
    
    with patch('services.insight_producer.enqueue_player_trend') as mock_enqueue:
        results = analyze_all_players('EPL', force=False)
        assert len(results) == 0, "Should skip returning player if existing DB data matches"
        mock_enqueue.assert_not_called()

@patch('db.config_db.get_connection')
def test_tc05_force_flag(mock_get_conn):
    """TC-05: Force flag overrides participation gate & dirty check."""
    mock_conn = MagicMock()
    mock_cur = MagicMock()
    mock_conn.cursor.return_value = mock_cur
    mock_get_conn.return_value = mock_conn
    
    mock_cur.fetchall.side_effect = [
        [(1, "NEUTRAL", 0.0)], # existing data matches perfectly!
        [(1, "Player A", [0,0,0], [0,0,0], [0,0,0], [0,0,0], [0,0,0], [0,0,0], 3)] 
    ]
    
    results = analyze_all_players('EPL', force=True)
    
    # 1. No early exit
    # Only 2 queries executed (existing insights + main query)
    assert mock_cur.execute.call_count == 2
    
    # 2. Main query does not have active_players INNER JOIN
    main_sql = mock_cur.execute.call_args_list[1][0][0]
    assert "INNER JOIN active_players ap ON pms.player_id = ap.player_id" not in main_sql
    
    # 3. Results should NOT be empty because force=True bypasses the dirty check
    assert len(results) == 1
    assert results[0]["player_name"] == "Player A"
