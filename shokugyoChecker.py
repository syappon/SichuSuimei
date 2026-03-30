import json
import sys
from collections import Counter
from pathlib import Path

input_path = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("famous_people.json")
output_path = input_path.with_name("occupations_list.json")

with open(input_path, encoding="utf-8") as f:
    data = json.load(f)["data"]

occ_counter = Counter()
for person in data:
    for occ in person.get("occupations", []):
        if occ:
            occ_counter[occ] += 1

# コンソール出力
print(f"総人数: {len(data)}")
print(f"ユニーク職業数: {len(occ_counter)}")
print()
print("─── 上位100件 ───")
for occ, count in occ_counter.most_common(100):
    print(f"{count:5d}  {occ}")

# JSON出力（全件・頻度順）
records = [{"occupation": occ, "count": count} for occ, count in occ_counter.most_common()]
with open(output_path, "w", encoding="utf-8") as f:
    json.dump(records, f, ensure_ascii=False, indent=2)

print(f"\nJSON出力: {output_path} ({len(records)}件)")
