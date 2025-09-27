"""
KoPL - Knowledge-oriented Programming Language

A knowledge-oriented programming language for complex reasoning and question
answering over knowledge bases.
"""

from .kopl import KoPLEngine
from .data import KB
from .util import ValueClass

__version__ = "1.0.0"
__all__ = ["KoPLEngine", "KB", "ValueClass"]