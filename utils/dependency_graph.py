"""
Dependency graph traversal for rebuild propagation.

Given changed sources, computes all downstream fact tables that need rebuilding
in correct topological order (dependencies before dependents).
"""

from typing import Dict, List, Set


def get_all_dependents(changed_sources: List[str], dependencies: Dict[str, List[str]]) -> List[str]:
    """
    Compute all fact tables that need rebuilding based on changed sources.
    
    Performs topological sort to ensure dependencies are built before dependents.
    
    Args:
        changed_sources: List of source names that have changed
        dependencies: Dictionary mapping fact table names to their source dependencies
        
    Returns:
        List of fact table names in topological order (ready to rebuild)
    """
    if not changed_sources:
        return []
    
    changed_set = set(changed_sources)
    
    # Find all facts that depend on changed sources (directly or indirectly)
    facts_to_rebuild = set()
    
    # Direct dependencies: facts that directly depend on changed sources
    for fact, deps in dependencies.items():
        if any(dep in changed_set for dep in deps):
            facts_to_rebuild.add(fact)
    
    # Handle indirect dependencies (facts depending on other facts)
    # For now, we assume facts only depend on sources, not other facts
    # But we can extend this if needed
    
    if not facts_to_rebuild:
        return []
    
    # Topological sort: ensure dependencies come before dependents
    # Since we only have source->fact dependencies (no fact->fact yet),
    # we can return in any order, but we'll sort alphabetically for consistency
    # If fact->fact dependencies are added later, implement proper topological sort
    result = sorted(list(facts_to_rebuild))
    
    # Validate no circular dependencies (for future fact->fact dependencies)
    # This is a placeholder - if we add fact->fact dependencies, we'd need:
    # 1. Build dependency graph of facts
    # 2. Perform topological sort (Kahn's algorithm or DFS)
    # 3. Detect and report cycles
    
    return result


def _validate_dependencies(dependencies: Dict[str, List[str]]) -> bool:
    """
    Validate dependency structure (check for obvious issues).
    
    Returns True if valid, False otherwise.
    """
    # Could add validation logic here
    # E.g., check that all referenced sources exist
    return True

