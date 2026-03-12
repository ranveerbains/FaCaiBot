use super::*;

#[test]
fn test_validate_known_param() {
    let v = validate_param("repricing.reprice_scale", "0.05").unwrap();
    assert!((v - 0.05).abs() < f64::EPSILON);
}

#[test]
fn test_validate_unknown_param() {
    assert!(validate_param("unknown.param", "1.0").is_err());
}

#[test]
fn test_validate_out_of_range() {
    assert!(validate_param("repricing.reprice_scale", "0.5").is_err());
    assert!(validate_param("repricing.reprice_scale", "0.0001").is_err());
}

#[test]
fn test_validate_not_a_number() {
    assert!(validate_param("repricing.reprice_scale", "abc").is_err());
}

#[test]
fn test_update_config_file() {
    let dir = std::env::temp_dir().join("facaibot_test_config_editor");
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("config_test.toml");
    std::fs::write(
        &path,
        "[risk]\nphase1_timeout_ms = 2000\nphase2_timeout_ms = 2000\n",
    )
    .unwrap();

    update_config_file(&path, "risk.phase1_timeout_ms", 3000.0).unwrap();

    let contents = std::fs::read_to_string(&path).unwrap();
    assert!(contents.contains("phase1_timeout_ms = 3000"));
    assert!(contents.contains("phase2_timeout_ms = 2000"));

    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn test_update_preserves_comments() {
    let dir = std::env::temp_dir().join("facaibot_test_comments");
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("config_comments.toml");
    std::fs::write(
        &path,
        "[risk]\nphase1_timeout_ms = 2000  # Phase 1 timeout\nphase2_timeout_ms = 2000\n",
    )
    .unwrap();

    update_config_file(&path, "risk.phase1_timeout_ms", 3000.0).unwrap();

    let contents = std::fs::read_to_string(&path).unwrap();
    assert!(contents.contains("phase1_timeout_ms = 3000"));
    assert!(contents.contains("# Phase 1 timeout"));
    assert!(contents.contains("phase2_timeout_ms = 2000"));

    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn test_read_config_section() {
    let dir = std::env::temp_dir().join("facaibot_test_read_section");
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("config_read.toml");
    std::fs::write(
        &path,
        "[entry_guards]\nentry_cutoff_secs = 20\n\n[risk]\nphase1_timeout_ms = 2000\n",
    )
    .unwrap();

    let section = read_config_section(&path, "entry_guards").unwrap();
    assert!(section.contains("entry_cutoff_secs"));
    assert!(!section.contains("phase1_timeout_ms"));

    let all = read_config_all(&path).unwrap();
    assert!(all.contains("entry_guards"));
    assert!(all.contains("risk"));

    std::fs::remove_dir_all(&dir).ok();
}
