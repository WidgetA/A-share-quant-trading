from src.strategy.v20.identity import named_hash, official_slot_id


def test_named_hash_is_order_independent_but_domain_separated() -> None:
    left = named_hash("V20_TEST_A", {"a": 1, "b": 2})
    right = named_hash("V20_TEST_A", {"b": 2, "a": 1})
    other = named_hash("V20_TEST_B", {"a": 1, "b": 2})
    assert left == right
    assert left != other


def test_official_slot_is_stable_per_stream_and_date() -> None:
    first = official_slot_id("main", "2026-08-31")
    assert first == official_slot_id("main", "2026-08-31")
    assert first != official_slot_id("main", "2026-09-01")
