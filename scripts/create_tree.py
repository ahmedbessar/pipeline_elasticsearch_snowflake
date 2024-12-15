import json

# Function to build category tree recursively
def build_category_tree(categories, parent_id=None):
    tree = []
    for cat_id, category in categories.items():
        if category['parent_id'] == parent_id:
            # Find children recursively
            category['children'] = build_category_tree(categories, category['id'])
            tree.append(category)
    return tree

# Read the input JSON from kw.json
with open("kw.json", "r", encoding="utf-8") as file:
    taxonomies = json.load(file)

# Build the tree starting from top-level categories
category_tree = build_category_tree(taxonomies)

# Save the result to a new file called category_tree.json
with open('category_treeV2.json', 'w', encoding='utf-8') as f:
    json.dump(category_tree, f, ensure_ascii=False, indent=4)

print("Category tree saved to category_tree.json.")