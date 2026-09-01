"""Narrow, release-specific V20 state-semantics compatibility receipts.

This module is deliberately covered by the full runtime-config hash but is not
itself a state-semantics dependency.  Keeping the receipt outside the hashed
state core avoids an impossible self-reference when the accepted target core
hash is recorded here.
"""

from __future__ import annotations

# Each pair is an independently reviewed, one-way lineage upgrade.  It does
# not make either endpoint compatible with any third state-semantics hash.
_AUDITED_STATE_SEMANTICS_TRANSITIONS: frozenset[tuple[str, str]] = frozenset(
    {
        # main@4b88fd6 -> the selection-critical V3 release.  The target uses
        # the LF deployment bytes for v20_service.py and the reviewed F1-only
        # repository blob; Rolling7 storage is explicitly absent.  This edge
        # preserves the authenticated state payload/revision and applies the
        # changed selection semantics only to future terminal decisions.
        (
            "ca8670343e13251287e7016ed2af1d26101f567b40f70705020733350e56dbbc",
            "94464f2a2c4a9c33c5041aeb640f0510947a438f4d5ddd305cdfc0e5f1cfba4b",
        ),
    }
)


def is_audited_state_semantics_transition(source_hash: object, target_hash: object) -> bool:
    return (
        isinstance(source_hash, str)
        and isinstance(target_hash, str)
        and (source_hash, target_hash) in _AUDITED_STATE_SEMANTICS_TRANSITIONS
    )


__all__ = ["is_audited_state_semantics_transition"]
