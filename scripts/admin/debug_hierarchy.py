import json

with open("data/raw/category_hierarchy_map.json", encoding="utf-8") as f:
    data = json.load(f)
    print("Total entries:", len(data))

    # Tìm entries có 'Trang trí nhà cửa'
    count = 0
    for url, info in data.items():
        if "trang trí nhà" in info.get("name", "").lower():
            print(f"\nURL: {url}")
            print(f'Name: {info.get("name")}')
            print(f'Parent chain: {info.get("parent_chain", [])}')
            if info.get("parent_chain"):
                for parent_url in info.get("parent_chain", []):
                    parent_info = data.get(parent_url)
                    if parent_info:
                        print(f'  Parent: {parent_info.get("name")}')
            count += 1
            if count >= 2:
                break
