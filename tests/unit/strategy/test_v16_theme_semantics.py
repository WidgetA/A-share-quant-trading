"""Unit tests for the offline V16 theme-semantics protocol."""

from __future__ import annotations

import copy

import pytest

from src.strategy.v16_theme_semantics import (
    PROTOCOL_HASH,
    PROTOCOL_VERSION,
    TAXONOMY_APPROVAL_SCHEMA_VERSION,
    ThemeSemanticsValidationError,
    UnapprovedThemeSemanticsError,
    approved_taxonomy_artifact_sha256,
    audit_normalization_responses,
    build_approved_theme_index,
    build_candidate_theme_index,
    build_consensus_candidate_taxonomy,
    build_normalization_prompt,
    build_normalization_request,
    canonical_json,
    canonicalize_normalization_response,
    normalization_response_sha256,
    parse_json_strict,
    prompt_sha256,
    taxonomy_sha256,
    validate_approved_taxonomy_artifact,
    validate_disagreement_audit,
    validate_normalization_request,
    validate_normalization_response,
    validate_taxonomy,
)


def _taxonomy() -> dict:
    return {
        "taxonomy_version": "test-v1",
        "themes": [
            {
                "canonical_theme_id": "noise:index_membership",
                "canonical_name": "指数成分标签",
                "label": "noise",
                "aliases": ["沪深300"],
            },
            {
                "canonical_theme_id": "theme:advanced_packaging",
                "canonical_name": "先进封装",
                "label": "theme",
                "aliases": ["Chiplet概念", "先进封装"],
            },
            {
                "canonical_theme_id": "umbrella:semiconductors",
                "canonical_name": "半导体上位主题",
                "label": "umbrella",
                "aliases": ["半导体概念", "芯片概念"],
            },
        ],
    }


def _approved_artifact(taxonomy: dict) -> dict:
    return {
        "schema_version": TAXONOMY_APPROVAL_SCHEMA_VERSION,
        "artifact_type": "v16_theme_taxonomy_approval",
        "approval": {
            "status": "human_approved",
            "reviewed_by": "test-reviewer",
            "reviewed_at": "2026-08-25T12:00:00+08:00",
            "review_ref": "unit-test-review",
            "source_candidate_manifest_sha256": None,
            "taxonomy_sha256": taxonomy_sha256(taxonomy),
        },
        "taxonomy": taxonomy,
    }


def _request(*, allow_new_themes: bool = True) -> dict:
    return build_normalization_request(
        ["先进封装", "沪深300", "Chiplet概念"],
        stock_boards={
            "000002": ["先进封装"],
            "000001": ["先进封装", "Chiplet概念"],
        },
        cooccurrences=[("先进封装", "Chiplet概念", 7)],
        taxonomy=_taxonomy(),
        allow_new_themes=allow_new_themes,
    )


def _response(request: dict, *, provider: str = "codex", model: str = "gpt-5") -> dict:
    return {
        "schema_version": PROTOCOL_VERSION,
        "protocol_hash": PROTOCOL_HASH,
        "request_hash": request["request_hash"],
        "taxonomy_hash": request["taxonomy_hash"],
        "provider": provider,
        "model": model,
        "prompt_hash": prompt_sha256(request, provider=provider, model=model),
        "themes": [
            {
                "canonical_theme_id": "noise:index_membership",
                "canonical_name": "指数成分标签",
                "label": "noise",
                "aliases": ["沪深300"],
                "confidence": 1.0,
                "evidence": [
                    {
                        "kind": "taxonomy_alias",
                        "board_names": ["沪深300"],
                        "stock_codes": [],
                    }
                ],
            },
            {
                "canonical_theme_id": "theme:advanced_packaging",
                "canonical_name": "先进封装",
                "label": "theme",
                "aliases": ["Chiplet概念", "先进封装"],
                "confidence": 0.95,
                "evidence": [
                    {
                        "kind": "cooccurrence",
                        "board_names": ["Chiplet概念", "先进封装"],
                        "stock_codes": [],
                    },
                    {
                        "kind": "stock_overlap",
                        "board_names": ["Chiplet概念", "先进封装"],
                        "stock_codes": ["000001"],
                    },
                    {
                        "kind": "taxonomy_alias",
                        "board_names": ["Chiplet概念", "先进封装"],
                        "stock_codes": [],
                    },
                ],
            },
        ],
    }


def test_request_is_canonical_and_order_independent() -> None:
    first = _request()
    second = build_normalization_request(
        ["沪深300", "Chiplet概念", "先进封装"],
        stock_boards={
            "000001": ["Chiplet概念", "先进封装"],
            "000002": ["先进封装"],
        },
        cooccurrences=[("Chiplet概念", "先进封装", 7)],
        taxonomy=_taxonomy(),
    )

    validate_normalization_request(first)
    assert first == second
    assert first["raw_boards"] == sorted(first["raw_boards"])
    assert first["request_hash"] == second["request_hash"]
    assert first["taxonomy_hash"] == taxonomy_sha256(_taxonomy())


def test_request_builder_rejects_non_string_names_instead_of_coercing_them() -> None:
    with pytest.raises(ThemeSemanticsValidationError, match="must be a string"):
        build_normalization_request(["先进封装", 123])  # type: ignore[list-item]
    with pytest.raises(ThemeSemanticsValidationError, match="stock codes must be strings"):
        build_normalization_request(["先进封装"], stock_boards={1: ["先进封装"]})  # type: ignore[dict-item]
    with pytest.raises(ThemeSemanticsValidationError, match="board names must be strings"):
        build_normalization_request(
            ["先进封装", "芯片概念"],
            cooccurrences=[("先进封装", 123, 1)],  # type: ignore[list-item]
        )


def test_prompt_is_schema_identical_but_provider_identity_is_hash_bound() -> None:
    request = _request()
    prompt = build_normalization_prompt(request, provider="codex", model="gpt-5")
    kimi_prompt = build_normalization_prompt(request, provider="kimi", model="kimi-k2.5")

    assert (
        f"PROMPT_PAYLOAD_SHA256={prompt_sha256(request, provider='codex', model='gpt-5')}" in prompt
    )
    assert '"provider":"codex"' in prompt
    assert '"provider":"kimi"' in kimi_prompt
    assert "trade action" in prompt
    assert prompt == build_normalization_prompt(request, provider="codex", model="gpt-5")
    assert prompt != kimi_prompt


@pytest.mark.parametrize(
    ("provider", "model"),
    [("codex", "gpt-5"), ("kimi", "kimi-k2.5"), ("human", "human-review-v1")],
)
def test_all_providers_share_one_valid_response_schema(provider: str, model: str) -> None:
    request = _request(allow_new_themes=False)
    response = _response(request, provider=provider, model=model)

    validate_normalization_response(response, request)
    assert normalization_response_sha256(response, request) == canonical_json_hash(response)


def canonical_json_hash(value: object) -> str:
    # Local spelling makes the assertion explain that response hashes use canonical JSON.
    import hashlib

    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


@pytest.mark.parametrize("forbidden_key", ["trade_action", "allow_trade", "position", "rationale"])
def test_free_text_or_trade_control_fields_are_rejected(forbidden_key: str) -> None:
    request = _request()
    response = _response(request)
    response[forbidden_key] = "BLOCK"

    with pytest.raises(ThemeSemanticsValidationError, match="extra"):
        validate_normalization_response(response, request)


def test_response_must_cover_every_raw_board_exactly_once() -> None:
    request = _request()
    response = _response(request)
    response["themes"][0]["aliases"] = ["Chiplet概念"]

    with pytest.raises(ThemeSemanticsValidationError):
        validate_normalization_response(response, request)


def test_mechanical_canonicalization_sorts_but_never_promotes_a_candidate() -> None:
    request = _request()
    response = _response(request)
    response["themes"].reverse()
    response["themes"][0]["aliases"].reverse()
    response["themes"][0]["evidence"].reverse()
    for evidence in response["themes"][0]["evidence"]:
        evidence["board_names"].reverse()

    with pytest.raises(ThemeSemanticsValidationError, match="sorted"):
        validate_normalization_response(response, request)

    candidate = canonicalize_normalization_response(response, request)
    validate_normalization_response(candidate, request)
    assert candidate == _response(request)
    assert build_candidate_theme_index(candidate, request).approval_status == "candidate"

    malformed = _response(request)
    malformed["themes"][1]["aliases"].append(123)
    with pytest.raises(ThemeSemanticsValidationError, match="must be a string"):
        canonicalize_normalization_response(malformed, request)


def test_response_rejects_wrong_prompt_hash_and_unsupported_evidence() -> None:
    request = _request()
    response = _response(request)
    response["prompt_hash"] = "0" * 64
    with pytest.raises(ThemeSemanticsValidationError, match="prompt_hash"):
        validate_normalization_response(response, request)

    request_without_pair = build_normalization_request(
        request["raw_boards"],
        stock_boards={row["stock_code"]: row["board_names"] for row in request["stocks"]},
        taxonomy=_taxonomy(),
    )
    response = _response(request_without_pair)
    with pytest.raises(ThemeSemanticsValidationError, match="cooccurrence pair"):
        validate_normalization_response(response, request_without_pair)


def test_new_ids_can_be_locked_to_the_reviewed_taxonomy() -> None:
    request = _request(allow_new_themes=False)
    response = _response(request)
    response["themes"][1]["canonical_theme_id"] = "theme:chiplet"
    response["themes"][1]["canonical_name"] = "Chiplet"

    with pytest.raises(ThemeSemanticsValidationError, match="not allowed"):
        validate_normalization_response(response, request)


def test_dual_agent_audit_separates_metadata_and_semantic_disagreement() -> None:
    request = _request()
    left = _response(request)
    right = _response(request, provider="kimi", model="kimi-k2.5")
    right["themes"][1]["confidence"] = 0.8

    confidence_audit = audit_normalization_responses(left, right, request)
    assert confidence_audit["independent_agents"] is True
    assert confidence_audit["semantic_agreement"] is True
    assert confidence_audit["exact_agreement"] is False
    assert confidence_audit["disagreements"][0]["fields"] == ["confidence"]

    semantic_right = copy.deepcopy(right)
    semantic_right["themes"][1]["canonical_theme_id"] = "theme:chiplet"
    semantic_right["themes"][1]["canonical_name"] = "Chiplet"
    semantic_right["themes"][1]["evidence"] = [
        {
            "kind": "name_similarity",
            "board_names": ["Chiplet概念", "先进封装"],
            "stock_codes": [],
        }
    ]
    semantic_audit = audit_normalization_responses(left, semantic_right, request)
    assert semantic_audit["semantic_agreement"] is False
    assert len(semantic_audit["disagreements"]) == 2
    assert semantic_audit == audit_normalization_responses(left, semantic_right, request)
    validate_disagreement_audit(semantic_audit, left, semantic_right, request)
    audit_payload = {key: value for key, value in semantic_audit.items() if key != "audit_hash"}
    assert semantic_audit["audit_hash"] == canonical_json_hash(audit_payload)

    tampered = copy.deepcopy(semantic_audit)
    tampered["semantic_agreement"] = True
    with pytest.raises(ThemeSemanticsValidationError, match="deterministic audit"):
        validate_disagreement_audit(tampered, left, semantic_right, request)


def test_consensus_taxonomy_contains_only_exact_semantic_agreement() -> None:
    request = _request()
    left = _response(request)
    confidence_only = _response(request, provider="kimi", model="kimi-k2.5")
    confidence_only["themes"][1]["confidence"] = 0.8

    taxonomy, manifest = build_consensus_candidate_taxonomy(left, confidence_only, request)
    assert [theme["canonical_theme_id"] for theme in taxonomy["themes"]] == [
        "noise:index_membership",
        "theme:advanced_packaging",
    ]
    assert manifest["artifact_status"] == "candidate/not_approved"
    assert manifest["runtime_load_allowed"] is False
    assert manifest["consensus_aliases"] == request["raw_boards"]
    assert manifest["manual_review"] == []
    assert len(manifest["metadata_disagreements"]) == 2

    semantic_right = copy.deepcopy(confidence_only)
    semantic_right["themes"][1]["canonical_theme_id"] = "theme:chiplet"
    semantic_right["themes"][1]["canonical_name"] = "Chiplet"
    semantic_right["themes"][1]["evidence"] = [
        {
            "kind": "name_similarity",
            "board_names": ["Chiplet概念", "先进封装"],
            "stock_codes": [],
        }
    ]
    taxonomy, manifest = build_consensus_candidate_taxonomy(left, semantic_right, request)
    assert [theme["canonical_theme_id"] for theme in taxonomy["themes"]] == [
        "noise:index_membership"
    ]
    assert manifest["consensus_aliases"] == ["沪深300"]
    assert [row["raw_board"] for row in manifest["manual_review"]] == [
        "Chiplet概念",
        "先进封装",
    ]


def test_approved_and_candidate_indexes_cannot_be_confused() -> None:
    taxonomy = _taxonomy()
    artifact = _approved_artifact(taxonomy)
    approved = build_approved_theme_index(
        artifact,
        approved_artifact_hash=approved_taxonomy_artifact_sha256(artifact),
    )

    assert approved.approval_status == "approved"
    assert approved.raw_to_canonical_theme_id["先进封装"] == "theme:advanced_packaging"
    assert approved.bridge_theme_id("先进封装") == "theme:advanced_packaging"
    assert approved.bridge_theme_id("芯片概念") is None
    assert approved.bridge_theme_id("沪深300") is None
    assert approved.excluded_aliases == frozenset({"半导体概念", "沪深300", "芯片概念"})

    request = _request()
    candidate = build_candidate_theme_index(_response(request), request)
    assert candidate.approval_status == "candidate"
    assert candidate.raw_to_canonical_theme_id["先进封装"] == "theme:advanced_packaging"
    assert candidate.excluded_aliases == frozenset({"沪深300"})
    with pytest.raises(UnapprovedThemeSemanticsError, match="cannot be used"):
        candidate.bridge_theme_id("先进封装")

    with pytest.raises(UnapprovedThemeSemanticsError, match="human-approval artifact"):
        build_approved_theme_index(
            taxonomy,
            approved_artifact_hash=taxonomy_sha256(taxonomy),
        )

    with pytest.raises(UnapprovedThemeSemanticsError, match="externally approved artifact"):
        build_approved_theme_index(artifact, approved_artifact_hash="0" * 64)


def test_approved_artifact_requires_human_status_timezone_and_matching_taxonomy_hash() -> None:
    taxonomy = _taxonomy()
    artifact = _approved_artifact(taxonomy)
    validate_approved_taxonomy_artifact(artifact)

    candidate = copy.deepcopy(artifact)
    candidate["approval"]["status"] = "candidate/not_approved"
    with pytest.raises(ThemeSemanticsValidationError, match="human_approved"):
        validate_approved_taxonomy_artifact(candidate)

    naive = copy.deepcopy(artifact)
    naive["approval"]["reviewed_at"] = "2026-08-25T12:00:00"
    with pytest.raises(ThemeSemanticsValidationError, match="timezone"):
        validate_approved_taxonomy_artifact(naive)

    tampered = copy.deepcopy(artifact)
    tampered["taxonomy"]["taxonomy_version"] = "tampered"
    with pytest.raises(ThemeSemanticsValidationError, match="enclosed taxonomy"):
        validate_approved_taxonomy_artifact(tampered)

    candidate_version = _approved_artifact(copy.deepcopy(taxonomy))
    candidate_version["taxonomy"]["taxonomy_version"] = "candidate-test-v1"
    candidate_version["approval"]["taxonomy_sha256"] = taxonomy_sha256(
        candidate_version["taxonomy"]
    )
    with pytest.raises(ThemeSemanticsValidationError, match=r"candidate-\*"):
        validate_approved_taxonomy_artifact(candidate_version)


def test_strict_json_parser_rejects_duplicates_and_non_finite_numbers() -> None:
    with pytest.raises(ThemeSemanticsValidationError, match="duplicate"):
        parse_json_strict('{"provider":"codex","provider":"kimi"}')
    with pytest.raises(ThemeSemanticsValidationError, match="non-finite"):
        parse_json_strict('{"confidence":NaN}')


def test_taxonomy_rejects_alias_collisions_and_unstable_order() -> None:
    taxonomy = _taxonomy()
    taxonomy["themes"][2]["aliases"].append("沪深300")
    taxonomy["themes"][2]["aliases"].sort()
    with pytest.raises(ThemeSemanticsValidationError, match="already belongs"):
        validate_taxonomy(taxonomy)

    taxonomy = _taxonomy()
    taxonomy["themes"].reverse()
    with pytest.raises(ThemeSemanticsValidationError, match="sorted"):
        validate_taxonomy(taxonomy)
