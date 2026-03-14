import json
import time
from collections import defaultdict, deque
from PurviewCatalogClient.purviewcatalogclient import get_purview_catalog_client

def build_dependency_graph(assets):
    graph = defaultdict(list)
    indegree = defaultdict(int)

    for asset in assets:
        qn = asset["attributes"]["qualifiedName"]
        indegree[qn] = indegree.get(qn, 0)

        if "relationshipAttributes" in asset:
            for rel_val in asset["relationshipAttributes"].values():
                if isinstance(rel_val, dict) and "qualifiedName" in rel_val.get("attributes", {}):
                    parent_qn = rel_val["attributes"]["qualifiedName"]
                    graph[parent_qn].append(qn)
                    indegree[qn] += 1
    return graph, indegree

def topological_sort(graph, indegree):
    queue = deque([node for node, deg in indegree.items() if deg == 0])
    order = []
    while queue:
        node = queue.popleft()
        order.append(node)
        for child in graph[node]:
            indegree[child] -= 1
            if indegree[child] == 0:
                queue.append(child)
    return order

def restore_metadata():
    client = get_purview_catalog_client()
    BACKUP_FILE = input("Enter backup filepath: ")

    with open(BACKUP_FILE, "r") as f:
        assets = json.load(f)

    # Build dependency graph
    graph, indegree = build_dependency_graph(assets)
    restore_order = topological_sort(graph, indegree)

    # Map qualifiedName → asset
    asset_map = {a["attributes"]["qualifiedName"]: a for a in assets}

    for qn in restore_order:
        asset = asset_map.get(qn)
        if not asset:
            continue
        try:
            # Create entity
            client.entity.create_or_update_entities(entities={"entities": [asset]})
            print(f"Restored {asset['typeName']} : {qn}")

            guid = asset.get("guid")
            if not guid:
                continue

            # Restore classifications
            if "classifications" in asset:
                for classification in asset["classifications"]:
                    try:
                        client.classification.add_classifications(
                            guid=guid, classifications=[classification]
                        )
                    except Exception as e:
                        print(f"Failed classification for {qn}: {e}")

            # Restore labels
            if "labels" in asset.get("attributes", {}):
                try:
                    client.entity.add_labels(
                        guid=guid, labels=asset["attributes"]["labels"]
                    )
                except Exception as e:
                    print(f"Failed labels for {qn}: {e}")

            # Restore glossary terms
            if "meanings" in asset:
                try:
                    client.glossary.assign_term(guid=guid, terms=asset["meanings"])
                except Exception as e:
                    print(f"Failed glossary terms for {qn}: {e}")

            time.sleep(0.2)

        except Exception as e:
            print(f"Error restoring {qn}: {e}")

if __name__ == "__main__":
    restore_metadata()
