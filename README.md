# KoPL: Knowledge-oriented Reasoning and Question Answering Programming Language

[Installation](#installation) | [Quick Start](#quick-start) | [Cache Loading](#cache-loading) | [Documentation](#documentation) | [Website](#website)

KoPL stands for Knowledge oriented Programming Language. It is a programming language designed for complex reasoning and question answering. Natural language questions can be represented as KoPL programs composed of basic functions, and the result of running the program is the answer to the question. Currently, KoPL provides 27 basic functions covering operations on various knowledge elements (such as concepts, entities, relations, attributes, modifiers, etc.), and supports multiple types of queries (such as counting, fact verification, comparison, etc.). KoPL offers a transparent reasoning process for complex questions, making it easy to understand and use. KoPL is extensible and can be applied to different forms of knowledge resources, such as knowledge bases and text.

The following code demonstrates how to use Python code to perform reasoning and question answering for a natural language question.

```python
from kopl.kopl import KoPLEngine
from kopl.test.test_example import example_kb

engine = KoPLEngine(example_kb) # Create an engine instance that operates on the example_kb knowledge base

# Query: Who is taller, LeBron James Jr. or his father?
ans = engine.SelectBetween( # Among two entities, query the one with the greater 'height' attribute
  engine.Find('LeBron James Jr.'), # Find the entity 'LeBron James Jr'
  engine.Relate( # Find the 'father' of 'LeBron James Jr'
    engine.Find('LeBron James Jr.'), # Find the entity 'LeBron James Jr'
    'father', # Relation label
    'forward' # 'forward' means 'LeBron James Jr.' is the head entity
  ),
  'height', # Attribute label
  'greater' # Query the entity with the greater attribute value
)

print(ans) # ans is a list of entity names

```

In this example, we query who is taller between LeBron James Jr. and his father. The KoPL program gives the correct answer: LeBron James!

# Installation

KoPL supports Linux (e.g., Ubuntu/CentOS), macOS, and Windows.

Dependencies:

* python >= 3.11

* tqdm >= 4.62

KoPL can be installed via pip. The following shows the installation command for Ubuntu:

```bash
  $ pip install KoPL tqdm
```

Run the following code:

```python
import kopl

from kopl.test.test_example import *

run_test()
```
If the test runs successfully, congratulations, you have installed KoPL.

# Quick Start
You can prepare your own knowledge base and use KoPL for reasoning and question answering. For the format of the knowledge base, please refer to [Knowledge Base](https://kopl.xlore.cn/doc/4_helloworld.html#id1).
For more examples of simple question answering using KoPL, refer to [Simple QA](https://kopl.xlore.cn/doc/5_example.html#id2), and for complex question answering, refer to [Complex QA](https://kopl.xlore.cn/doc/5_example.html#id8).

You can also use our [Query Service](https://kopl.xlore.cn/queryService) to quickly start your KoPL journey.

# Cache Loading
For large JSON knowledge bases, KoPL provides a lightweight on-disk cache to speed up repeated engine initialization.

Usage:
```python
from kopl.kopl import KoPLEngine
engine = KoPLEngine.from_json('path/to/kb.json')
```
Behavior:
* Creates/reads a pickle cache file named `kb.json.cache` beside the JSON.
* Cache stores: { version, json_mtime, json_size, kb }.
* Reuses cache if version matches and the source file's modification time and size are unchanged.
* Pass `force_rebuild=True` to ignore and overwrite the cache.
* Pass `use_cache=False` to always load the JSON directly (no read/write of cache).
* Bump the internal `ENGINE_CACHE_VERSION` constant in `kopl.kopl` to invalidate all existing caches when KB layout logic changes.

# Documentation
We provide detailed KoPL [documentation](https://kopl.xlore.cn/doc/index.html), introducing the knowledge elements KoPL targets, basic functions, and the KoPL engine API.

# Website
https://kopl.xlore.cn
https://kopl.xlore.cn
