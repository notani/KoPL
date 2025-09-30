from collections import defaultdict, Counter
from datetime import date
from typing import Any, Optional, Union

from kopl.data import KB
from kopl.util import ValueClass, comp

# Type aliases for better readability
EntityTuple = tuple[list[str], Optional[list[dict[str, Any]]]]


class KoPLEngine(object):
    """Main execution engine for KoPL (Knowledge-oriented Programming Language).

    This engine provides 27+ basic functions for knowledge operations including
    search, filtering, navigation, queries, logic operations, and verification.
    """

    def __init__(self, kb: dict[str, Any]) -> None:
        """Initialize the KoPL engine with a knowledge base.

        Args:
            kb: Knowledge base dictionary with 'entities' and 'concepts' keys
        """
        self.kb = KB(kb)

    def _parse_key_value(
        self, key: Optional[str], value: str, typ: Optional[str] = None
    ) -> ValueClass:
        """Parse a key-value pair into a ValueClass instance based on the expected type.

        This method converts string values into strongly-typed ValueClass instances
        that support proper comparison operations. It handles four data types:
        string, quantity (with units), date (YYYY-MM-DD or YYYY/MM/DD), and year.

        Args:
            key: The attribute key used to determine type if typ is not provided (can be None)
            value: The string value to be parsed
            typ: Optional explicit type specification ("string", "quantity", "date", "year")

        Returns:
            A ValueClass instance with the parsed value and appropriate type

        Raises:
            KeyError: If key is not found in kb.key_type when typ is None
            ValueError: If value cannot be parsed according to the expected type
        """
        if typ is None:
            if key is None:
                raise ValueError("Either key or typ must be provided")
            typ = self.kb.key_type[key]

        if typ == "string":
            return ValueClass("string", value)
        elif typ == "quantity":
            if " " in value:
                vs = value.split()
                v = vs[0]
                unit = " ".join(vs[1:])
            else:
                v = value
                unit = "1"
            return ValueClass("quantity", float(v), unit)
        else:
            if "/" in value or ("-" in value and "-" != value[0]):
                split_char = "/" if "/" in value else "-"
                p1, p2 = value.find(split_char), value.rfind(split_char)
                y, m, d = int(value[:p1]), int(value[p1 + 1 : p2]), int(value[p2 + 1 :])
                return ValueClass("date", date(y, m, d))
            else:
                return ValueClass("year", int(value))

    def forward(
        self,
        program: list[str],
        inputs: list[list[Any]],
        ignore_error: bool = False,
        show_details: bool = False,
    ) -> Union[list[str], str, None]:
        """Execute a KoPL program with automatic dependency inference.

        This is the main execution method that takes a program (list of function names)
        and their inputs, automatically infers dependencies between functions based on
        their signatures, and executes them in the correct order. Functions like
        'And', 'Or', 'SelectBetween' expect two entity sets, while filters expect one.

        Args:
            program: List of function names to execute in sequence
            inputs: List of input parameters for each function in the program
            ignore_error: If True, return None on error instead of raising exception
            show_details: If True, print debug information during execution

        Returns:
            The final result as a list of strings, single string, or None if error occurred
            and ignore_error is True

        Raises:
            Exception: Any exception that occurs during program execution if ignore_error is False
        """
        memory = []
        program = ["<START>"] + program + ["<END>"]
        inputs = [[]] + inputs + [[]]
        try:
            # infer the dependency based on the function definition
            dependency = []
            branch_stack = []
            for i, p in enumerate(program):
                if p in {"<START>", "<END>", "<PAD>"}:
                    dep = []
                elif p in {"FindAll", "Find"}:
                    dep = []
                    branch_stack.append(i - 1)
                elif p in {
                    "And",
                    "Or",
                    "SelectBetween",
                    "QueryRelation",
                    "QueryRelationQualifier",
                }:
                    dep = [branch_stack[-1], i - 1]
                    branch_stack = branch_stack[:-1]
                else:
                    dep = [i - 1]
                dependency.append(dep)

            memory = []
            for p, dep, inp in zip(program, dependency, inputs):
                if p == "What":
                    p = "QueryName"
                if p == "<START>":
                    res = None
                elif p == "<END>":
                    break
                else:
                    fun_args = [memory[x] for x in dep]
                    func = getattr(self, p)
                    res = func(*fun_args, *inp)

                memory.append(res)
                if show_details:
                    print(p, dep, inp)
                    print(res)
            return (
                [str(_) for _ in memory[-1]]
                if isinstance(memory[-1], list)
                else str(memory[-1])
            )
        except Exception:
            if ignore_error:
                return None
            else:
                raise

    def FindAll(self) -> EntityTuple:
        """Return all entities in the knowledge base.

        This function retrieves every entity ID from the knowledge base,
        including both regular entities and concepts. It serves as the starting
        point for many KoPL programs that need to operate on the entire entity set.

        Returns:
            A tuple containing (entity_ids, None) where entity_ids is a list
            of all entity identifiers in the knowledge base
        """
        entity_ids = list(self.kb.entities.keys())
        return (entity_ids, None)

    def Find(self, name: str) -> EntityTuple:
        """Find all entities with a specific name.

        This function searches the knowledge base for entities that have
        the exact given name. Multiple entities can share the same name
        (e.g., people with the same full name), so this returns a list.

        Args:
            name: The exact name string to search for

        Returns:
            A tuple containing (entity_ids, None) where entity_ids is a list
            of entity identifiers that have the given name

        Raises:
            KeyError: If no entities with the given name exist
        """
        entity_ids = self.kb.name_to_id[name]
        return (entity_ids, None)

    def FilterConcept(self, entities: EntityTuple, concept_name: str) -> EntityTuple:
        """Filter entities to find those belonging to a specific concept.

        This function filters the input entities to include only those that
        are instances of the given concept or its subconcepts. For example,
        filtering by "basketball player" would return entities that are
        classified as basketball players in the knowledge base.

        Args:
            entities: A tuple of (entity_ids, facts) from a previous operation
            concept_name: The name of the concept to filter by

        Returns:
            A tuple containing (filtered_entity_ids, None) where filtered_entity_ids
            is the intersection of input entities with entities of the given concept

        Raises:
            KeyError: If the concept_name is not found in the knowledge base
        """
        entity_ids, _ = entities
        concept_ids = self.kb.name_to_id[concept_name]
        entity_ids_2 = []
        for i in concept_ids:
            entity_ids_2 += self.kb.concept_to_entity.get(i, [])
        entity_ids = list(set(entity_ids) & set(entity_ids_2))
        return (entity_ids, None)

    def _filter_attribute(
        self, entity_ids: list[str], tgt_key: str, tgt_value: str, op: str, typ: str
    ) -> EntityTuple:
        """Internal helper method to filter entities by attribute values.

        Args:
            entity_ids: List of entity IDs to filter
            tgt_key: The attribute key to filter on
            tgt_value: The target value to compare against
            op: Comparison operator ("=", "!=", "<", ">")
            typ: Value type ("string", "quantity", "date", "year")

        Returns:
            A tuple of (matching_entity_ids, matching_attribute_facts)
        """
        tgt_value_obj = self._parse_key_value(tgt_key, tgt_value, typ)
        res_ids = []
        res_facts = []
        entity_ids_set = set(entity_ids) & set(
            self.kb.attribute_inv_index[tgt_key].keys()
        )
        for ent_id in entity_ids_set:
            for idx in self.kb.attribute_inv_index[tgt_key][ent_id]:
                attr_info = self.kb.entities[ent_id]["attributes"][idx]
                k, v = attr_info["key"], attr_info["value"]
                if (
                    k == tgt_key
                    and v.can_compare(tgt_value_obj)
                    and comp(v, tgt_value_obj, op)
                ):
                    res_ids.append(ent_id)
                    res_facts.append(attr_info)
        return (res_ids, res_facts)

    def FilterStr(self, entities: EntityTuple, key: str, value: str) -> EntityTuple:
        """Filter entities by string attribute values using exact match.

        This function filters entities to find those whose specified attribute
        exactly matches the given string value. For example, filtering by
        ('nationality', 'American') would return entities with nationality="American".

        Args:
            entities: A tuple of (entity_ids, facts) from a previous operation
            key: The attribute key to filter on (e.g., "nationality", "position")
            value: The exact string value to match

        Returns:
            A tuple of (matching_entity_ids, matching_attribute_facts) where
            matching_attribute_facts contains the attribute triples that satisfied the condition
        """
        entity_ids, _ = entities
        op = "="
        return self._filter_attribute(entity_ids, key, value, op, "string")

    def FilterNum(
        self, entities: EntityTuple, key: str, value: str, op: str
    ) -> EntityTuple:
        """Filter entities by numerical attribute values with comparison operators.

        This function filters entities based on numerical attributes using various
        comparison operators. Values can include units (e.g., "200 centimetre", "75 kilogram").
        Only entities with the same unit can be compared.

        Args:
            entities: A tuple of (entity_ids, facts) from a previous operation
            key: The attribute key to filter on (e.g., "height", "weight", "age")
            value: The numerical value with optional unit (e.g., "200 centimetre", "30")
            op: Comparison operator ("=", "!=", "<", ">")

        Returns:
            A tuple of (matching_entity_ids, matching_attribute_facts) where
            matching_attribute_facts contains the attribute triples that satisfied the condition

        Raises:
            ValueError: If value cannot be parsed as a number or units don't match
        """
        entity_ids, _ = entities
        return self._filter_attribute(entity_ids, key, value, op, "quantity")

    def FilterYear(
        self, entities: EntityTuple, key: str, value: str, op: str
    ) -> EntityTuple:
        """Filter entities by year attribute values with comparison operators.

        This function filters entities based on year attributes using comparison
        operators. The value should be a 4-digit year string.

        Args:
            entities: A tuple of (entity_ids, facts) from a previous operation
            key: The attribute key to filter on (e.g., "birth_year", "founded_year")
            value: The year value as a string (e.g., "1984", "2000")
            op: Comparison operator ("=", "!=", "<", ">")

        Returns:
            A tuple of (matching_entity_ids, matching_attribute_facts) where
            matching_attribute_facts contains the attribute triples that satisfied the condition

        Raises:
            ValueError: If value cannot be parsed as a year
        """
        entity_ids, _ = entities
        return self._filter_attribute(entity_ids, key, value, op, "year")

    def FilterDate(
        self, entities: EntityTuple, key: str, value: str, op: str
    ) -> EntityTuple:
        """Filter entities by date attribute values with comparison operators.

        This function filters entities based on date attributes using comparison
        operators. Dates can be in YYYY-MM-DD or YYYY/MM/DD format.

        Args:
            entities: A tuple of (entity_ids, facts) from a previous operation
            key: The attribute key to filter on (e.g., "birth_date", "founding_date")
            value: The date value as a string (e.g., "1984-12-30", "2000/01/01")
            op: Comparison operator ("=", "!=", "<", ">")

        Returns:
            A tuple of (matching_entity_ids, matching_attribute_facts) where
            matching_attribute_facts contains the attribute triples that satisfied the condition

        Raises:
            ValueError: If value cannot be parsed as a valid date
        """
        entity_ids, _ = entities
        return self._filter_attribute(entity_ids, key, value, op, "date")

    def _filter_qualifier(
        self,
        entity_ids: list[str],
        facts: Optional[list[dict[str, Any]]],
        tgt_key: str,
        tgt_value: str,
        op: str,
        typ: str,
    ) -> EntityTuple:
        """Internal helper method to filter entities by qualifier values on their attributes/relations.

        Args:
            entity_ids: List of entity IDs to filter
            facts: List of attribute/relation facts corresponding to entity_ids
            tgt_key: The qualifier key to filter on
            tgt_value: The target qualifier value to compare against
            op: Comparison operator ("=", "!=", "<", ">")
            typ: Value type ("string", "quantity", "date", "year")

        Returns:
            A tuple of (matching_entity_ids, matching_facts)
        """
        if facts is None:
            return ([], [])

        tgt_value_obj = self._parse_key_value(tgt_key, tgt_value, typ)
        res_ids = []
        res_facts = []
        for i, f in zip(entity_ids, facts):
            for qk, qvs in f["qualifiers"].items():
                if qk == tgt_key:
                    for qv in qvs:
                        if qv.can_compare(tgt_value_obj) and comp(
                            qv, tgt_value_obj, op
                        ):
                            res_ids.append(i)
                            res_facts.append(f)
                            break
        return (res_ids, res_facts)

    def QFilterStr(self, entities: EntityTuple, qkey: str, qvalue: str) -> EntityTuple:
        """Filter triples by string qualifier values using exact match.

        This function filters attribute or relation triples based on their qualifiers.
        Qualifiers provide additional context to triples (e.g., time period, location).
        This method finds triples where the specified qualifier exactly matches the given string.

        Args:
            entities: A tuple of (entity_ids, facts) where facts contains attribute/relation triples
            qkey: The qualifier key to filter on (e.g., "point_in_time", "location")
            qvalue: The exact string value the qualifier must match

        Returns:
            A tuple of (filtered_entity_ids, filtered_facts) containing only triples
            with qualifiers that match the specified condition
        """
        entity_ids, facts = entities
        op = "="
        return self._filter_qualifier(entity_ids, facts, qkey, qvalue, op, "string")

    def QFilterNum(
        self, entities: EntityTuple, qkey: str, qvalue: str, op: str
    ) -> EntityTuple:
        """Filter triples by numerical qualifier values with comparison operators.

        Similar to QFilterStr but for numerical qualifier values. This allows filtering
        based on numerical qualifiers like years, amounts, or quantities with units.

        Args:
            entities: A tuple of (entity_ids, facts) where facts contains attribute/relation triples
            qkey: The qualifier key to filter on (e.g., "salary_amount", "duration")
            qvalue: The numerical value with optional unit (e.g., "1000000 dollar", "5")
            op: Comparison operator ("=", "!=", "<", ">")

        Returns:
            A tuple of (filtered_entity_ids, filtered_facts) containing only triples
            with numerical qualifiers that satisfy the comparison condition

        Raises:
            ValueError: If qvalue cannot be parsed as a number
        """
        entity_ids, facts = entities
        return self._filter_qualifier(entity_ids, facts, qkey, qvalue, op, "quantity")

    def QFilterYear(
        self, entities: EntityTuple, qkey: str, qvalue: str, op: str
    ) -> EntityTuple:
        """Filter triples by year qualifier values with comparison operators.

        Similar to QFilterStr but for year qualifier values. This is useful for
        temporal filtering based on year qualifiers.

        Args:
            entities: A tuple of (entity_ids, facts) where facts contains attribute/relation triples
            qkey: The qualifier key to filter on (e.g., "point_in_time", "start_year")
            qvalue: The year value as a string (e.g., "2000", "1995")
            op: Comparison operator ("=", "!=", "<", ">")

        Returns:
            A tuple of (filtered_entity_ids, filtered_facts) containing only triples
            with year qualifiers that satisfy the comparison condition

        Raises:
            ValueError: If qvalue cannot be parsed as a valid year
        """
        entity_ids, facts = entities
        return self._filter_qualifier(entity_ids, facts, qkey, qvalue, op, "year")

    def QFilterDate(
        self, entities: EntityTuple, qkey: str, qvalue: str, op: str
    ) -> EntityTuple:
        """Filter triples by date qualifier values with comparison operators.

        Similar to QFilterStr but for date qualifier values. This enables precise
        temporal filtering based on full date qualifiers.

        Args:
            entities: A tuple of (entity_ids, facts) where facts contains attribute/relation triples
            qkey: The qualifier key to filter on (e.g., "point_in_time", "start_date")
            qvalue: The date value as a string (e.g., "2000-01-01", "1995/12/30")
            op: Comparison operator ("=", "!=", "<", ">")

        Returns:
            A tuple of (filtered_entity_ids, filtered_facts) containing only triples
            with date qualifiers that satisfy the comparison condition

        Raises:
            ValueError: If qvalue cannot be parsed as a valid date
        """
        entity_ids, facts = entities
        return self._filter_qualifier(entity_ids, facts, qkey, qvalue, op, "date")

    def Relate(
        self, entities: EntityTuple, relation: str, direction: str
    ) -> EntityTuple:
        """Find all entities related to input entities through a specific relation.

        This function navigates the knowledge graph by following relationships from
        the input entities. It can traverse relationships in both forward and backward
        directions to find connected entities.

        Args:
            entities: A tuple of (entity_ids, facts) from a previous operation
            relation: The relation label to follow (e.g., "spouse", "member_of", "located_in")
            direction: Either "forward" or "backward" - indicates whether input entities
                      are the subject (head) or object (tail) of the relation

        Returns:
            A tuple of (related_entity_ids, relation_facts) where related_entity_ids
            are entities connected through the specified relation, and relation_facts
            are the corresponding relation triples
        """
        entity_ids, _ = entities
        res_ids = []
        res_facts = []
        entity_ids_set = set(entity_ids) & set(
            self.kb.relation_inv_index[(relation, direction)].keys()
        )
        for ent_id in entity_ids_set:
            for idx in self.kb.relation_inv_index[(relation, direction)][ent_id]:
                rel_info = self.kb.entities[ent_id]["relations"][idx]
                res_ids.append(rel_info["object"])
                res_facts.append(rel_info)
        return (res_ids, res_facts)

    def And(self, l_entities: EntityTuple, r_entities: EntityTuple) -> EntityTuple:
        """Return the intersection of two entity sets.

        This logical operation finds entities that appear in both input sets.
        It's commonly used to combine filtering conditions or find entities
        that satisfy multiple criteria simultaneously.

        Args:
            l_entities: A tuple of (entity_ids, facts) from the left operand
            r_entities: A tuple of (entity_ids, facts) from the right operand

        Returns:
            A tuple of (intersection_entity_ids, None) where intersection_entity_ids
            contains entities present in both input sets
        """
        entity_ids_1, _ = l_entities
        entity_ids_2, _ = r_entities
        return (list(set(entity_ids_1) & set(entity_ids_2)), None)

    def Or(self, l_entities: EntityTuple, r_entities: EntityTuple) -> EntityTuple:
        """Return the union of two entity sets.

        This logical operation combines entities from both input sets, removing
        duplicates. It's used to find entities that satisfy at least one of
        multiple criteria.

        Args:
            l_entities: A tuple of (entity_ids, facts) from the left operand
            r_entities: A tuple of (entity_ids, facts) from the right operand

        Returns:
            A tuple of (union_entity_ids, None) where union_entity_ids
            contains all unique entities from both input sets
        """
        entity_ids_1, _ = l_entities
        entity_ids_2, _ = r_entities
        return (list(set(entity_ids_1) | set(entity_ids_2)), None)

    def QueryName(self, entities: EntityTuple) -> list[str]:
        """Query the names of entities.

        This function retrieves the human-readable names of the input entities.
        It's commonly used as the final step in KoPL programs to convert entity
        IDs into readable names for the end user.

        Args:
            entities: A tuple of (entity_ids, facts) from a previous operation

        Returns:
            A list of strings where each string is the name of the corresponding input entity
        """
        entity_ids, _ = entities
        res = []
        for entity_id in entity_ids:
            name = self.kb.entities[entity_id]["name"]
            res.append(name)
        return res

    def Count(self, entities: EntityTuple) -> int:
        """Count the number of entities in the input set.

        This function returns the size of the entity set, which is useful for
        answering questions about quantities (e.g., "How many basketball players
        are taller than 200cm?").

        Args:
            entities: A tuple of (entity_ids, facts) from a previous operation

        Returns:
            The number of entities in the input set as an integer
        """
        entity_ids, _ = entities
        return len(entity_ids)

    def SelectBetween(
        self, l_entities: EntityTuple, r_entities: EntityTuple, key: str, op: str
    ) -> str:
        """Select the entity with greater/smaller attribute value between two sets.

        This function compares entities from two sets based on a numerical attribute
        and returns the entity with the greater or smaller value. It handles unit
        conversion by finding the most common unit among all candidates.

        Args:
            l_entities: A tuple of (entity_ids, facts) from the left operand
            r_entities: A tuple of (entity_ids, facts) from the right operand
            key: The numerical attribute to compare (e.g., "height", "weight", "age")
            op: Comparison operator - "less" for smaller value, "greater" for larger value

        Returns:
            The name of the entity with the specified extreme value

        Raises:
            ValueError: If no entities have the specified attribute or no common unit is found
        """
        entity_ids_1, _ = l_entities
        entity_ids_2, _ = r_entities
        candidates = []
        for ent_id in entity_ids_1:
            for idx in self.kb.attribute_inv_index[key][ent_id]:
                attr_info = self.kb.entities[ent_id]["attributes"][idx]
                candidates.append((ent_id, attr_info["value"]))
        for ent_id in entity_ids_2:
            for idx in self.kb.attribute_inv_index[key][ent_id]:
                attr_info = self.kb.entities[ent_id]["attributes"][idx]
                candidates.append((ent_id, attr_info["value"]))
        candidates = list(filter(lambda x: x[1].type == "quantity", candidates))
        unit_cnt = defaultdict(int)
        for x in candidates:
            unit_cnt[x[1].unit] += 1
        common_unit = Counter(unit_cnt).most_common()[0][0]
        candidates = list(filter(lambda x: x[1].unit == common_unit, candidates))
        sort = sorted(candidates, key=lambda x: x[1])
        i = sort[0][0] if op == "less" else sort[-1][0]
        name = self.kb.entities[i]["name"]
        return name

    def SelectAmong(self, entities: EntityTuple, key: str, op: str) -> list[str]:
        """Select entities with the largest/smallest attribute value within a set.

        This function finds entities with the extreme value (maximum or minimum)
        for a specified numerical attribute within the input entity set. Multiple
        entities can have the same extreme value, so this returns a list.

        Args:
            entities: A tuple of (entity_ids, facts) from a previous operation
            key: The numerical attribute to compare (e.g., "height", "weight", "age")
            op: Comparison operator - "smallest" for minimum value, "largest" for maximum value

        Returns:
            A list of entity names that have the specified extreme value.
            Multiple entities can share the same extreme value.

        Raises:
            ValueError: If no entities have the specified attribute or no common unit is found
        """
        entity_ids, _ = entities
        entity_ids_set = set(entity_ids)
        candidates = []
        for ent_id in entity_ids_set:
            for idx in self.kb.attribute_inv_index[key][ent_id]:
                attr_info = self.kb.entities[ent_id]["attributes"][idx]
                candidates.append((ent_id, attr_info["value"]))
        candidates = list(filter(lambda x: x[1].type == "quantity", candidates))
        unit_cnt = defaultdict(int)
        for x in candidates:
            unit_cnt[x[1].unit] += 1
        common_unit = Counter(unit_cnt).most_common()[0][0]
        candidates = list(filter(lambda x: x[1].unit == common_unit, candidates))
        sort = sorted(candidates, key=lambda x: x[1])
        value = sort[0][1] if op == "smallest" else sort[-1][1]
        names = list(
            set([self.kb.entities[i]["name"] for i, v in candidates if v == value])
        )  # Multiple entities can have the same extreme value
        return names

    def QueryAttr(self, entities: EntityTuple, key: str) -> list[ValueClass]:
        """Query specific attribute values of entities.

        This function retrieves the values of a specified attribute for all
        input entities. Each entity may have multiple values for the same
        attribute, so all values are included in the result.

        Args:
            entities: A tuple of (entity_ids, facts) from a previous operation
            key: The attribute key to query (e.g., "height", "nationality", "birth_date")

        Returns:
            A list of ValueClass instances containing the attribute values.
            Each ValueClass has type information (string, quantity, date, year)
            and supports proper comparison operations.
        """
        entity_ids, _ = entities
        res = []
        for ent_id in entity_ids:
            for idx in self.kb.attribute_inv_index[key][ent_id]:
                attr_info = self.kb.entities[ent_id]["attributes"][idx]
                res.append(attr_info["value"])
        return res

    def QueryAttrUnderCondition(
        self, entities: EntityTuple, key: str, qkey: str, qvalue: str
    ) -> list[ValueClass]:
        """Query attribute values of entities under specific qualifier conditions.

        This function retrieves attribute values only when they have specific
        qualifier conditions. For example, querying salary with qualifier
        "point_in_time=2020" would return only salaries from that year.

        Args:
            entities: A tuple of (entity_ids, facts) from a previous operation
            key: The attribute key to query (e.g., "salary", "population")
            qkey: The qualifier key that must match (e.g., "point_in_time", "location")
            qvalue: The qualifier value that must match (e.g., "2020", "New York")

        Returns:
            A list of ValueClass instances containing attribute values that
            have the specified qualifier condition
        """
        entity_ids, _ = entities
        qvalue_obj = self._parse_key_value(qkey, qvalue)
        res = []
        for ent_id in entity_ids:
            for idx in self.kb.attribute_inv_index[key][ent_id]:
                attr_info = self.kb.entities[ent_id]["attributes"][idx]
                flag = False
                for qk, qvs in attr_info["qualifiers"].items():
                    if qk == qkey:
                        for qv in qvs:
                            if qv.can_compare(qvalue_obj) and comp(qv, qvalue_obj, "="):
                                flag = True
                                break
                    if flag:
                        break
                if flag:
                    v = attr_info["value"]
                    res.append(v)
        return res

    def _verify(
        self, s_value: list[ValueClass], t_value: str, op: str, typ: str
    ) -> str:
        """Internal helper method to verify if attribute values satisfy a condition.

        Args:
            s_value: List of ValueClass instances to verify
            t_value: Target value to compare against
            op: Comparison operator ("=", "!=", "<", ">")
            typ: Value type ("string", "quantity", "date", "year")

        Returns:
            "yes" if all values satisfy the condition,
            "no" if no values satisfy the condition,
            "not sure" if some but not all values satisfy the condition
        """
        attr_values = s_value
        value = self._parse_key_value(None, t_value, typ)
        match = []
        for attr_value in attr_values:
            if attr_value.can_compare(value) and comp(attr_value, value, op):
                match.append(1)
            else:
                match.append(0)
        if sum(match) >= 1 and sum(match) == len(match):
            answer = "yes"
        elif sum(match) == 0:
            answer = "no"
        else:
            answer = "not sure"
        return answer

    def VerifyStr(self, s_value: list[ValueClass], t_value: str) -> str:
        """Verify if QueryAttr or QueryAttrUnderCondition output equals a given string.

        This function checks whether the attribute values from a query operation
        match a specific string value. It's used for fact verification in QA systems.

        Args:
            s_value: A list of ValueClass instances from QueryAttr or QueryAttrUnderCondition
            t_value: The target string value to verify against

        Returns:
            "yes" if all attribute values match the target string,
            "no" if no attribute values match the target string,
            "not sure" if some but not all attribute values match
        """
        op = "="
        return self._verify(s_value, t_value, op, "string")

    def VerifyNum(self, s_value: list[ValueClass], t_value: str, op: str) -> str:
        """Verify if numerical attribute values satisfy a specific condition.

        Similar to VerifyStr but for numerical values. This allows verification
        with various comparison operators (equals, greater than, less than, etc.).

        Args:
            s_value: A list of ValueClass instances from QueryAttr or QueryAttrUnderCondition
            t_value: The target numerical value with optional unit (e.g., "200 centimetre")
            op: Comparison operator ("=", "!=", "<", ">")

        Returns:
            "yes" if all attribute values satisfy the condition,
            "no" if no attribute values satisfy the condition,
            "not sure" if some but not all attribute values satisfy the condition
        """
        return self._verify(s_value, t_value, op, "quantity")

    def VerifyYear(self, s_value: list[ValueClass], t_value: str, op: str) -> str:
        """Verify if year attribute values satisfy a specific condition.

        Similar to VerifyStr but for year values. This allows temporal verification
        with comparison operators.

        Args:
            s_value: A list of ValueClass instances from QueryAttr or QueryAttrUnderCondition
            t_value: The target year value as a string (e.g., "1984", "2000")
            op: Comparison operator ("=", "!=", "<", ">")

        Returns:
            "yes" if all attribute values satisfy the condition,
            "no" if no attribute values satisfy the condition,
            "not sure" if some but not all attribute values satisfy the condition
        """
        return self._verify(s_value, t_value, op, "year")

    def VerifyDate(self, s_value: list[ValueClass], t_value: str, op: str) -> str:
        """Verify if date attribute values satisfy a specific condition.

        Similar to VerifyStr but for date values. This allows precise temporal
        verification with comparison operators.

        Args:
            s_value: A list of ValueClass instances from QueryAttr or QueryAttrUnderCondition
            t_value: The target date value as a string (e.g., "1984-12-30", "2000/01/01")
            op: Comparison operator ("=", "!=", "<", ">")

        Returns:
            "yes" if all attribute values satisfy the condition,
            "no" if no attribute values satisfy the condition,
            "not sure" if some but not all attribute values satisfy the condition
        """
        return self._verify(s_value, t_value, op, "date")

    def QueryRelation(
        self, s_entities: EntityTuple, t_entities: EntityTuple
    ) -> list[str]:
        """Query the relations between two entity sets.

        This function finds all relation types that connect entities from the
        first set (subjects) to entities in the second set (objects). It's useful
        for discovering what types of relationships exist between entity groups.

        Args:
            s_entities: A tuple of (subject_entity_ids, facts) - the source entities
            t_entities: A tuple of (object_entity_ids, facts) - the target entities

        Returns:
            A list of relation strings representing the types of relationships
            that connect the subject entities to the object entities
        """
        entity_ids_1, _ = s_entities
        entity_ids_2, _ = t_entities
        res = []
        for entity_id_1 in entity_ids_1:
            for entity_id_2 in entity_ids_2:
                for idx in self.kb.forward_relation_index[(entity_id_1, entity_id_2)]:
                    rel_info = self.kb.entities[entity_id_1]["relations"][idx]
                    res.append(rel_info["relation"])
        return res

    def QueryAttrQualifier(
        self, entities: EntityTuple, key: str, value: str, qkey: str
    ) -> list[ValueClass]:
        """Query specific qualifier values for entity attributes.

        This function finds qualifier values for attributes that match a specific
        key-value pair. For example, querying salary qualifiers with key="salary"
        and value="1000000 dollar" might return time qualifiers like ["2020", "2021"].

        Args:
            entities: A tuple of (entity_ids, facts) from a previous operation
            key: The attribute key to match (e.g., "salary", "population")
            value: The attribute value to match (e.g., "1000000 dollar", "5000000")
            qkey: The qualifier key to retrieve (e.g., "point_in_time", "location")

        Returns:
            A list of ValueClass instances containing the qualifier values
            for attributes that match the specified key-value condition
        """
        entity_ids, _ = entities
        value_obj = self._parse_key_value(key, value)
        res = []
        for ent_id in entity_ids:
            for idx in self.kb.attribute_inv_index[key][ent_id]:
                attr_info = self.kb.entities[ent_id]["attributes"][idx]
                if (
                    attr_info["key"] == key
                    and attr_info["value"].can_compare(value_obj)
                    and comp(attr_info["value"], value_obj, "=")
                ):
                    for qk, qvs in attr_info["qualifiers"].items():
                        if qk == qkey:
                            res += qvs
        return res

    def QueryRelationQualifier(
        self, s_entities: EntityTuple, t_entities: EntityTuple, relation: str, qkey: str
    ) -> list[ValueClass]:
        """Query specific qualifier values for entity relations.

        This function finds qualifier values for relations that connect entities
        from the first set to entities in the second set. For example, querying
        "spouse" relation qualifiers might return marriage dates or locations.

        Args:
            s_entities: A tuple of (subject_entity_ids, facts) - the source entities
            t_entities: A tuple of (object_entity_ids, facts) - the target entities
            relation: The relation type to match (e.g., "spouse", "member_of", "located_in")
            qkey: The qualifier key to retrieve (e.g., "start_time", "end_time", "location")

        Returns:
            A list of ValueClass instances containing the qualifier values
            for relations that match the specified relation type between the entity sets
        """
        entity_ids_1, _ = s_entities
        entity_ids_2, _ = t_entities
        res = []
        for entity_id_1 in entity_ids_1:
            for entity_id_2 in entity_ids_2:
                for idx in self.kb.forward_relation_index[(entity_id_1, entity_id_2)]:
                    rel_info = self.kb.entities[entity_id_1]["relations"][idx]
                    if rel_info["relation"] == relation:
                        for qk, qvs in rel_info["qualifiers"].items():
                            if qk == qkey:
                                res += qvs
        return res
