use super::*;

#[test]
fn ordinal_roundtrip_is_checked_in_both_directions() {
    for ordinal in [None, Some(0), Some(i64::MAX as u64)] {
        let encoded = checked_write_ordinal(ordinal).expect("representable ordinal");
        let line = checked_read_line(0, 0, encoded, "opaque utf8\r\n".to_string()).expect("read");
        assert_eq!(line.ordinal, ordinal);
        assert_eq!(line.line, "opaque utf8\r\n");
    }
    for ordinal in [i64::MAX as u64 + 1, u64::MAX] {
        assert_eq!(
            checked_write_ordinal(Some(ordinal)),
            Err(RolloutStoreError::InvalidRequest(
                "ordinal exceeds PostgreSQL BIGINT"
            ))
        );
    }
    assert_eq!(
        checked_read_line(0, 0, Some(-1), "retained".to_string()),
        Err(RolloutStoreError::Corrupt("negative ordinal".to_string()))
    );
}

#[test]
fn each_returned_page_line_must_hold_its_exact_expected_position() {
    let original = "{\"unknown\":1}\r\ntrailing";
    assert_eq!(
        checked_read_line(5, 5, None, original.to_string())
            .expect("valid")
            .line,
        original
    );
    for (expected, actual) in [(5, 6), (6, 5), (1, 0)] {
        assert_eq!(
            checked_read_line(expected, actual, None, original.to_string()),
            Err(RolloutStoreError::Corrupt(
                "rollout page contains a position gap".to_string()
            ))
        );
    }
    assert_eq!(
        checked_read_line(0, -1, None, original.to_string()),
        Err(RolloutStoreError::Corrupt("negative position".to_string()))
    );
}

#[test]
fn request_positions_and_batch_end_use_checked_bigint_boundaries() {
    assert_eq!(checked_request_position(0), Ok(0));
    assert_eq!(checked_request_position(i64::MAX as u64), Ok(i64::MAX));
    for value in [i64::MAX as u64 + 1, u64::MAX] {
        assert_eq!(
            checked_request_position(value),
            Err(RolloutStoreError::InvalidRequest(
                "position exceeds PostgreSQL BIGINT"
            ))
        );
        assert!(matches!(
            validate_append_request(value, &[]),
            Err(RolloutStoreError::InvalidRequest(_))
        ));
    }
    assert_eq!(
        validate_append_request(i64::MAX as u64, &[]),
        Ok(i64::MAX as u64)
    );
    let one = [(None, "opaque".to_string())];
    assert_eq!(validate_append_request(0, &one), Ok(1));
    assert_eq!(
        validate_append_request(i64::MAX as u64 - 1, &one),
        Ok(i64::MAX as u64)
    );
    assert!(matches!(
        validate_append_request(i64::MAX as u64, &one),
        Err(RolloutStoreError::InvalidRequest(_))
    ));
    assert!(matches!(
        validate_append_request(u64::MAX, &one),
        Err(RolloutStoreError::InvalidRequest(_))
    ));
}

#[test]
fn complete_batch_ordinal_validation_precedes_transaction_and_preserves_request() {
    let lines = [
        (Some(0), "first".to_string()),
        (Some(u64::MAX), "last".to_string()),
    ];
    let original = lines.clone();
    assert_eq!(
        validate_append_request(0, &lines),
        Err(RolloutStoreError::InvalidRequest(
            "ordinal exceeds PostgreSQL BIGINT"
        ))
    );
    assert_eq!(lines, original);
}
