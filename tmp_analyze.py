import json
with open(r'd:\videoToMarkdownTest2\var\storage\storage\57018a9f0c5fe43f4622fb60ce8a9957\semantic_units_phase2a.json', 'r', encoding='utf-8') as f:
    data = json.load(f)
with open(r'd:\videoToMarkdownTest2\tmp_result.txt', 'w', encoding='utf-8') as out:
    out.write(f"Total units: {len(data)}\n")
    out.write("="*80 + "\n")
    for u in data:
        dur = int(u['end_sec'] - u['start_sec'])
        out.write(f"{u['unit_id']:6s} {dur:5d}s  {u['knowledge_type']:8s}  {u['knowledge_topic']}\n")
print("Done")
