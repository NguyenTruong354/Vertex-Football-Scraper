import sys
import json
from pathlib import Path

# Setup environment
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from services.text_utils import clean_json_insight

def run_tests():
    print("="*60)
    print("🧪 TESTING DUAL-LANGUAGE JSON PARSING 🧪")
    print("="*60 + "\n")

    # Case 1: LLM trả JSON đúng format (No markdown)
    print("[CASE 1] Valid JSON format")
    raw1 = '{"en": "Liverpool dominated.", "vi": "Liverpool áp đảo."}'
    res1 = clean_json_insight(raw1)
    print(f"Result: {res1}\n")

    # Case 2: LLM trả JSON có markdown fence ```json...```
    print("[CASE 2] JSON with markdown blocks")
    raw2 = '```json\n{"en": "Great match.", "vi": "Trận đấu hay."}\n```'
    res2 = clean_json_insight(raw2)
    print(f"Result: {res2}\n")

    # Case 3A: LLM trả plain text thuần (Fallback lang detection - English)
    print("[CASE 3A] Plain Text Fallback (English)")
    raw3 = 'Manchester City controls the ball with high possession.'
    res3 = clean_json_insight(raw3)
    print(f"Result: {res3}\n")

    # Case 3B: LLM trả plain text thuần (Fallback lang detection - Vietnamese)
    print("[CASE 3B] Plain Text Fallback (Vietnamese)")
    raw4 = 'M.City kiểm soát bóng cực kỳ xuất sắc trong hiệp một.'
    res4 = clean_json_insight(raw4)
    print(f"Result: {res4}\n")

    # Case 4: LLM trả empty string
    print("[CASE 4] Empty String")
    raw5 = ''
    res5 = clean_json_insight(raw5)
    print(f"Result: {res5}\n")
    
    print("="*60)

if __name__ == "__main__":
    run_tests()
