"""Least-privilege policy for the four SRS user roles.

The SRS names the roles and requires role-based permissions but leaves the
permission matrix unspecified. These defaults are a documented project policy.
"""

from __future__ import annotations

from dataclasses import dataclass


ROLES = ("administrator", "regional_manager", "manager", "analyst")
OPERATIONS = ("read", "create", "update", "deactivate")

ENTITY_PERMISSIONS: dict[str, dict[str, frozenset[str]]] = {
    "locations": {
        "administrator": frozenset(OPERATIONS),
        "regional_manager": frozenset({"read"}),
        "manager": frozenset({"read"}),
        "analyst": frozenset({"read"}),
    },
    "menu_categories": {
        "administrator": frozenset(OPERATIONS), "regional_manager": frozenset(OPERATIONS),
        "manager": frozenset({"read"}), "analyst": frozenset({"read"}),
    },
    "menu_items": {
        "administrator": frozenset(OPERATIONS), "regional_manager": frozenset(OPERATIONS),
        "manager": frozenset({"read"}), "analyst": frozenset({"read"}),
    },
    "pricing_history": {
        "administrator": frozenset(OPERATIONS), "regional_manager": frozenset(OPERATIONS),
        "manager": frozenset(OPERATIONS), "analyst": frozenset({"read"}),
    },
    "customers": {
        "administrator": frozenset(OPERATIONS), "regional_manager": frozenset({"read"}),
        "manager": frozenset({"read"}), "analyst": frozenset({"read"}),
    },
    "orders": {
        "administrator": frozenset(OPERATIONS), "regional_manager": frozenset(OPERATIONS),
        "manager": frozenset(OPERATIONS), "analyst": frozenset({"read"}),
    },
    "order_items": {
        "administrator": frozenset(OPERATIONS), "regional_manager": frozenset(OPERATIONS),
        "manager": frozenset(OPERATIONS), "analyst": frozenset({"read"}),
    },
    "promotions": {
        "administrator": frozenset(OPERATIONS), "regional_manager": frozenset(OPERATIONS),
        "manager": frozenset({"read"}), "analyst": frozenset({"read"}),
    },
    "ratings": {
        "administrator": frozenset(OPERATIONS), "regional_manager": frozenset(OPERATIONS),
        "manager": frozenset(OPERATIONS), "analyst": frozenset({"read"}),
    },
    "inventory": {
        "administrator": frozenset(OPERATIONS), "regional_manager": frozenset(OPERATIONS),
        "manager": frozenset(OPERATIONS), "analyst": frozenset({"read"}),
    },
    "wastage": {
        "administrator": frozenset(OPERATIONS), "regional_manager": frozenset(OPERATIONS),
        "manager": frozenset(OPERATIONS), "analyst": frozenset({"read"}),
    },
}


@dataclass(frozen=True)
class Principal:
    user_id: int
    username: str
    role: str
    assigned_location_id: str | None = None


def allowed(role: str, entity: str, operation: str) -> bool:
    """Return whether a known role may perform an operation on an entity."""
    if operation not in OPERATIONS:
        return False
    return operation in ENTITY_PERMISSIONS.get(entity, {}).get(role, frozenset())


def is_administrator(role: str) -> bool:
    return role == "administrator"


def scope_location_id(principal: Principal, entity: str) -> str | None:
    """Restaurant managers are limited to their assigned location where records carry a location key."""
    if principal.role == "manager" and entity not in {"locations", "menu_categories", "customers"}:
        return principal.assigned_location_id
    return None
