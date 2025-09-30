from collections import defaultdict
from datetime import date
from queue import Queue
from typing import Any, Union

from copy import deepcopy
from tqdm import tqdm

from kopl.util import ValueClass


def conv_enc(s: str) -> str:
    """Convert encoding for strings with unicode escape sequences.

    This function handles unicode escape sequences in strings by attempting
    to decode them properly. If decoding fails, it removes the unicode prefix.

    Args:
        s: The input string that may contain unicode escape sequences

    Returns:
        The string with unicode sequences properly decoded or cleaned
    """
    try:
        s = s.encode("utf-8").decode("unicode_escape")
    except Exception:
        s = s.replace("\\u", "")
    return s


def lambda_list() -> defaultdict[Any, list[Any]]:
    """Create a defaultdict that returns empty lists for missing keys.

    This is a factory function used to create defaultdict instances that
    automatically create empty lists when accessing non-existent keys.

    Returns:
        A defaultdict that creates empty lists for missing keys
    """
    return defaultdict(list)


def lambda_set() -> defaultdict[Any, set[Any]]:
    """Create a defaultdict that returns empty sets for missing keys.

    This is a factory function used to create defaultdict instances that
    automatically create empty sets when accessing non-existent keys.

    Returns:
        A defaultdict that creates empty sets for missing keys
    """
    return defaultdict(set)


class KB(object):
    """Knowledge Base wrapper for KoPL engine.

    This class processes and indexes a knowledge base containing entities and concepts.
    It builds multiple indices for efficient querying including:
    - Name-to-ID mappings for entity lookup
    - Concept hierarchy with inheritance relationships
    - Inverted indices for attributes and relations
    - Forward indices for relation traversal

    The knowledge base follows a specific JSON format with entities and concepts
    where entities have attributes and relations, and concepts define taxonomies.

    Args:
        kb: Knowledge base dictionary containing 'entities' and 'concepts' keys
    """

    def __init__(self, kb: dict[str, Any]) -> None:
        """Initialize the knowledge base with comprehensive indexing.

        This constructor processes the input knowledge base to create multiple
        indices for efficient querying. It handles entity-concept relationships,
        builds inverted indices for attributes and relations, and converts all
        values to strongly-typed ValueClass instances.

        Args:
            kb: Dictionary with 'entities' and 'concepts' keys containing the knowledge base data
        """
        # Main entity storage - contains both entities and concepts
        self.entities: dict[str, dict[str, Any]] = {}
        for cid in kb["concepts"]:
            self.entities[cid] = kb["concepts"][cid]
            self.entities[cid]["relations"] = []
            self.entities[cid]["attributes"] = []
            self.entities[cid]["isA"] = self.entities[cid].pop("subclassOf")
        for eid in kb["entities"]:
            self.entities[eid] = kb["entities"][eid]
            self.entities[eid]["isA"] = self.entities[eid].pop("instanceOf")
        # some entities may have relations with concepts, we add them into self.entities for visiting convenience
        for eid in kb["entities"]:
            for rel_info in kb["entities"][eid]["relations"]:
                obj_id = rel_info["object"]
                if obj_id in kb["concepts"]:
                    rel_info_for_con = {
                        "relation": rel_info["relation"],
                        "direction": "forward"
                        if rel_info["direction"] == "backward"
                        else "backward",
                        "object": eid,
                        "qualifiers": deepcopy(rel_info["qualifiers"]),
                    }
                    if rel_info_for_con not in self.entities[obj_id]["relations"]:
                        self.entities[obj_id]["relations"].append(rel_info_for_con)
        # print('convert encoding')
        # # 后续都要注意这个编码问题
        # for ent_id, ent_info in tqdm(self.entities.items()):
        # 	ent_info['name'] = conv_enc(ent_info['name'])
        # 	for attr_info in ent_info['attributes']:
        # 		attr_info['key'] = conv_enc(attr_info['key'])
        # 		if attr_info['value']['type'] == 'string':
        # 			attr_info['value']['value'] = conv_enc(attr_info['value']['value'])
        # 		replace_pair = []
        # 		for qk in attr_info['qualifiers']:
        # 			cqk = conv_enc(qk)
        # 			if cqk != qk:
        # 				replace_pair.append((qk, cqk))
        # 		for old_k, new_k in replace_pair:
        # 			attr_info['qualifiers'][new_k] = attr_info['qualifiers'].pop(old_k)
        # 		for qk, qvs in attr_info['qualifiers'].items():
        # 			for qv in qvs:
        # 				if qv['type'] == 'string':
        # 					qv['value'] = conv_enc(qv['value'])

        # for rel_info in ent_info['relations']:
        # 	rel_info['relation'] = conv_enc(rel_info['relation'])
        # 	replace_pair = []
        # 	for qk in rel_info['qualifiers']:
        # 		cqk = conv_enc(qk)
        # 		if cqk != qk:
        # 			replace_pair.append((qk, cqk))
        # 	for old_k, new_k in replace_pair:
        # 		rel_info['qualifiers'][new_k] = rel_info['qualifiers'].pop(old_k)
        # 	for qk, qvs in rel_info['qualifiers'].items():
        # 		for qv in qvs:
        # 			if qv['type'] == 'string':
        # 				qv['value'] = conv_enc(qv['value'])
        print("process concept")
        # Name-to-ID mapping: entity name -> list of entity IDs (includes both concepts and entities)
        self.name_to_id: defaultdict[str, list[str]] = defaultdict(list)
        # Concept hierarchy: concept ID -> set of entity IDs that belong to this concept (converted to list later)
        self.concept_to_entity: Union[
            defaultdict[str, set[str]], dict[str, list[str]]
        ] = defaultdict(set)
        for ent_id, ent_info in tqdm(self.entities.items()):
            self.name_to_id[ent_info["name"]].append(ent_id)
            for c in self.get_all_concepts(
                ent_id
            ):  # merge entity into ancestor concept
                self.concept_to_entity[c].add(ent_id)
        # Convert from defaultdict[str, set] to dict[str, list] for final usage
        self.concept_to_entity = {k: list(v) for k, v in self.concept_to_entity.items()}
        # List of all concept IDs in the knowledge base
        self.concepts: list[str] = list(self.concept_to_entity.keys())

        print("process attribute and relation")
        # All unique attribute keys found in the knowledge base (converted to list later)
        self.attribute_keys: Union[set[str], list[str]] = set()
        # All unique relation types found in the knowledge base (converted to list later)
        self.relations: Union[set[str], list[str]] = set()
        # Mapping from attribute/qualifier keys to their value types
        self.key_type: dict[str, str] = {}
        # Inverted index for attributes: attribute_key -> {entity_id: [indices]}
        # Maps each attribute key to entities that have that attribute,
        # with indices pointing to positions in entities[ent_id]['attributes']
        self.attribute_inv_index: defaultdict[str, defaultdict[str, list[int]]] = (
            defaultdict(lambda_list)
        )

        # Inverted index for relations: (relation, direction) -> {entity_id: [indices]}
        # Maps each (relation_type, direction) pair to entities with that relation,
        # with indices pointing to positions in entities[ent_id]['relations']
        self.relation_inv_index: defaultdict[
            tuple[str, str], defaultdict[str, list[int]]
        ] = defaultdict(lambda_list)

        # Forward relation index: (subject_id, object_id) -> [indices]
        # Maps entity pairs to relation indices for efficient relation traversal
        self.forward_relation_index: defaultdict[tuple[str, str], list[int]] = (
            defaultdict(list)
        )

        # Entity sets for efficient filtering and querying (converted to lists later)
        self.entity_set_with_attribute: Union[set[str], list[str]] = set()
        self.entity_set_with_quantity_attribute: Union[set[str], list[str]] = set()
        self.entity_set_with_attribute_qualifier: Union[set[str], list[str]] = set()
        self.entity_set_with_relation: Union[set[str], list[str]] = set()
        self.entity_set_with_relation_qualifier: Union[set[str], list[str]] = set()
        for ent_id, ent_info in tqdm(self.entities.items()):
            for idx, attr_info in enumerate(ent_info["attributes"]):
                self.attribute_keys.add(attr_info["key"])
                self.key_type[attr_info["key"]] = attr_info["value"]["type"]
                self.attribute_inv_index[attr_info["key"]][ent_id].append(idx)
                self.entity_set_with_attribute.add(ent_id)
                if attr_info["value"]["type"] == "quantity":
                    self.entity_set_with_quantity_attribute.add(ent_id)
                for qk in attr_info["qualifiers"]:
                    self.attribute_keys.add(qk)
                    self.entity_set_with_attribute_qualifier.add(ent_id)
                    for qv in attr_info["qualifiers"][qk]:
                        self.key_type[qk] = qv["type"]

            for idx, rel_info in enumerate(ent_info["relations"]):
                self.relations.add(rel_info["relation"])
                self.relation_inv_index[(rel_info["relation"], rel_info["direction"])][
                    ent_id
                ].append(idx)
                if rel_info["direction"] == "forward":
                    self.forward_relation_index[(ent_id, rel_info["object"])].append(
                        idx
                    )
                self.entity_set_with_relation.add(ent_id)
                for qk in rel_info["qualifiers"]:
                    self.attribute_keys.add(qk)
                    self.entity_set_with_relation_qualifier.add(ent_id)
                    for qv in rel_info["qualifiers"][qk]:
                        self.key_type[qk] = qv["type"]

            # parse values into ValueClass object
            for attr_info in ent_info["attributes"]:
                attr_info["value"] = self._parse_value(attr_info["value"])
                for qk, qvs in attr_info["qualifiers"].items():
                    attr_info["qualifiers"][qk] = [self._parse_value(qv) for qv in qvs]
            for rel_info in ent_info["relations"]:
                for qk, qvs in rel_info["qualifiers"].items():
                    rel_info["qualifiers"][qk] = [self._parse_value(qv) for qv in qvs]

        self.attribute_keys = list(self.attribute_keys)
        self.relations = list(self.relations)
        # Note: key_type is one of string/quantity/date, but date means the key may have values of type year
        self.key_type = {
            k: v if v != "year" else "date" for k, v in self.key_type.items()
        }
        self.entity_set_with_both_attribute_and_relation = list(
            self.entity_set_with_attribute & self.entity_set_with_relation
        )
        self.entity_set_with_attribute = list(self.entity_set_with_attribute)
        self.entity_set_with_quantity_attribute = list(
            self.entity_set_with_quantity_attribute
        )
        self.entity_set_with_attribute_qualifier = list(
            self.entity_set_with_attribute_qualifier
        )
        self.entity_set_with_relation = list(self.entity_set_with_relation)
        self.entity_set_with_relation_qualifier = list(
            self.entity_set_with_relation_qualifier
        )

        print("extract seen values")
        # Values seen for each attribute key (converted from set to list later)
        self.key_values: Any = defaultdict(set)
        # Values seen for each concept-attribute combination (not including qualifier values)
        self.concept_key_values: Any = defaultdict(lambda_set)
        # Relations for each concept
        self.concept_relations: defaultdict[
            str, defaultdict[tuple[str, str], list[str]]
        ] = defaultdict(lambda_list)

        for ent_id, ent_info in tqdm(self.entities.items()):
            for attr_info in ent_info["attributes"]:
                k, v = attr_info["key"], attr_info["value"]
                self.key_values[k].add(v)
                for c in self.get_all_concepts(ent_id):
                    self.concept_key_values[c][k].add(v)
                # merge qualifier statistics into attribute
                for qk, qvs in attr_info["qualifiers"].items():
                    for qv in qvs:
                        self.key_values[qk].add(qv)

            for rel_info in ent_info["relations"]:
                for c in self.get_all_concepts(ent_id):
                    self.concept_relations[c][
                        (rel_info["relation"], rel_info["direction"])
                    ].append(rel_info["object"])
                # merge qualifier statistics into attribute
                for qk, qvs in rel_info["qualifiers"].items():
                    for qv in qvs:
                        self.key_values[qk].add(qv)
        for k in self.key_values:
            self.key_values[k] = list(self.key_values[k])
        for c in self.concept_key_values:
            for k in self.concept_key_values[c]:
                self.concept_key_values[c][k] = list(self.concept_key_values[c][k])

        print("number of concepts: %d" % len(self.concepts))
        print("number of entities: %d" % len(self.entities))
        print("number of attribute keys: %d" % len(self.attribute_keys))
        print("number of relations: %d" % len(self.relations))

    def get_direct_concepts(self, ent_id: str) -> list[str]:
        """Get the direct parent concept IDs of a given entity or concept.

        This method returns the immediate parent concepts in the concept hierarchy.
        For entities, these are the concepts they are instances of. For concepts,
        these are the parent concepts in the subclass hierarchy.

        Args:
            ent_id: The entity or concept ID to get parents for

        Returns:
            List of direct parent concept IDs. Returns empty list if entity not found.
        """
        if ent_id in self.entities:
            return [
                i
                for i in self.entities[ent_id]["isA"]
                if i in self.entities and i != ent_id
            ]
        else:
            return []

    def get_all_concepts(self, ent_id: str) -> list[str]:
        """Get all ancestor concept IDs of a given entity or concept.

        This method performs a breadth-first search up the concept hierarchy
        to find all ancestor concepts. It handles cycles in the hierarchy by
        tracking visited concepts to prevent infinite loops.

        Args:
            ent_id: The entity or concept ID to get all ancestors for

        Returns:
            List of all ancestor concept IDs in the hierarchy above the given entity
        """
        ancestors = set()
        q = Queue()
        for c in self.get_direct_concepts(ent_id):
            q.put(c)
        while not q.empty():
            con_id = q.get()
            if (
                con_id in self.entities and con_id not in ancestors
            ):  # Prevent infinite loops in case of circular hierarchies
                ancestors.add(con_id)
                for c in self.entities[con_id]["isA"]:
                    q.put(c)
        return list(ancestors)

    def print_statistics(self) -> None:
        """Print detailed statistics about the knowledge base content.

        This method counts and displays statistics about the knowledge base including:
        - Number of relation facts (entity-relation-entity triples)
        - Number of attribute facts (entity-attribute-value triples)
        - Number of qualifier facts (additional context on relations/attributes)

        The statistics help understand the size and complexity of the knowledge base.
        """
        cnt_rel, cnt_attr, cnt_qual = 0, 0, 0
        for ent_id, ent_info in self.entities.items():
            for attr_info in ent_info["attributes"]:
                cnt_attr += 1
                for qk in attr_info["qualifiers"]:
                    for qv in attr_info["qualifiers"][qk]:
                        cnt_qual += 1
        for ent_id, ent_info in self.entities.items():
            for rel_info in ent_info["relations"]:
                cnt_rel += 1
                for qk in rel_info["qualifiers"]:
                    for qv in rel_info["qualifiers"][qk]:
                        cnt_qual += 1

        print("number of relation knowledge: %d" % cnt_rel)
        print("number of attribute knowledge: %d" % cnt_attr)
        print("number of qualifier knowledge: %d" % cnt_qual)

    def _parse_value(self, value: dict[str, Any]) -> ValueClass:
        """Parse a value dictionary into a strongly-typed ValueClass instance.

        This internal method converts raw value dictionaries from the knowledge base
        into ValueClass objects that support proper comparison operations. It handles
        four data types: string, quantity (with units), date, and year.

        Args:
            value: Dictionary with 'type' key and type-specific value fields

        Returns:
            ValueClass instance with parsed value and appropriate type

        Raises:
            ValueError: If the value cannot be parsed according to its declared type
        """
        if value["type"] == "string":
            result = ValueClass("string", value["value"])
        elif value["type"] == "quantity":
            result = ValueClass("quantity", float(value["value"]), value["unit"])
        else:
            x = str(value["value"])
            if "/" in x or ("-" in x and "-" != x[0]):
                split_char = "/" if "/" in x else "-"
                p1, p2 = x.find(split_char), x.rfind(split_char)
                y, m, d = int(x[:p1]), int(x[p1 + 1 : p2]), int(x[p2 + 1 :])
                result = ValueClass("date", date(y, m, d))
            else:
                result = ValueClass("year", int(x))
        return result
