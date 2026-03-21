use super::*;

#[test]
fn test_validate_known_param() {
    let v = validate_param("quoting.min_edge", "0.05").unwrap();
    assert!((v - 0.05).abs() < f64::EPSILON);
}

#[test]
fn test_validate_unknown_param() {
    assert!(validate_param("unknown.param", "1.0").is_err());
}

#[test]
fn test_validate_out_of_range() {
    assert!(validate_param("quoting.min_edge", "0.5").is_err()); // max is 0.10
    assert!(validate_param("quoting.min_edge", "-0.01").is_err()); // min is 0.0
}

#[test]
fn test_validate_not_a_number() {
    assert!(validate_param("quoting.min_edge", "abc").is_err());
}

#[test]
fn test_update_config_file() {
    let dir = std::env::temp_dir().join("facaibot_test_config_editor_v2");
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("config_test.toml");
    std::fs::write(
        &path,
        "[risk_v2]\nmax_capital_per_market = 100\nclosing_phase_secs = 30\n",
    )
    .unwrap();

    update_config_file(&path, "risk_v2.max_capital_per_market", 200.0).unwrap();

    let contents = std::fs::read_to_string(&path).unwrap();
    assert!(contents.contains("max_capital_per_market = 200"));
    assert!(contents.contains("closing_phase_secs = 30"));

    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn test_update_preserves_comments() {
    let dir = std::env::temp_dir().join("facaibot_test_comments_v2");
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("config_comments.toml");
    std::fs::write(
        &path,
        "[risk_v2]\nmax_capital_per_market = 100  # Max capital\nclosing_phase_secs = 30\n",
    )
    .unwrap();

    update_config_file(&path, "risk_v2.max_capital_per_market", 200.0).unwrap();

    let contents = std::fs::read_to_string(&path).unwrap();
    assert!(contents.contains("max_capital_per_market = 200"));
    assert!(contents.contains("# Max capital"));
    assert!(contents.contains("closing_phase_secs = 30"));

    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn test_read_config_section() {
    let dir = std::env::temp_dir().join("facaibot_test_read_section_v2");
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("config_read.toml");
    std::fs::write(
        &path,
        "[quoting]\nmin_edge = 0.03\n\n[risk_v2]\nmax_capital_per_market = 100\n",
    )
    .unwrap();

    let section = read_config_section(&path, "quoting").unwrap();
    assert!(section.contains("min_edge"));
    assert!(!section.contains("max_capital_per_market"));

    let all = read_config_all(&path).unwrap();
    assert!(all.contains("quoting"));
    assert!(all.contains("risk_v2"));

    std::fs::remove_dir_all(&dir).ok();
}
