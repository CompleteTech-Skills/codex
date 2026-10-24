use super::*;

#[test]
fn invalid_identity_is_rejected_at_its_authoritative_guard() {
    for (instance, exclusive_instance, expected) in [
        (
            None,
            Some("0123456789abcdef0123456789abcdef"),
            "typed fixture instance",
        ),
        (
            Some("invalid"),
            Some("0123456789abcdef0123456789abcdef"),
            "invalid fixture instance",
        ),
        (
            Some("0123456789abcdef0123456789abcdef"),
            None,
            "exclusive instance mismatch",
        ),
    ] {
        let state = tempfile::tempdir().expect("isolated receipt");
        let receipt = serde_json::json!({"instance": instance, "engine_id": "nonexistent-engine",
            "project": "codex-pg-isolated-test", "port": 55587});
        let exclusive = serde_json::json!({"instance": exclusive_instance,
            "purpose": "section-crud-exclusive", "disposable": true,
            "container_id": "0000000000000000000000000000000000000000000000000000000000000000"});
        std::fs::write(state.path().join("receipt.json"), receipt.to_string())
            .expect("write complete receipt");
        std::fs::write(
            state.path().join("section-crud-exclusive.json"),
            exclusive.to_string(),
        )
        .expect("write complete ownership");
        let panic = std::panic::catch_unwind(|| settings(state.path(), "runtime"))
            .expect_err("specific identity guard must reject");
        let message = panic
            .downcast_ref::<String>()
            .map(String::as_str)
            .or_else(|| panic.downcast_ref::<&str>().copied())
            .expect("panic message");
        assert!(
            message.contains(expected),
            "unexpected rejection: {message}"
        );
    }
}
