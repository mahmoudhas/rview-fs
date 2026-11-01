use super::*;

#[test]
fn only_paths_inside_an_export_parse() {
    let path = RelPath::parse("a/b c/x.bin").unwrap();
    assert_eq!(path.split(), ("a/b c", "x.bin"));
    assert_eq!(RelPath::parse("x").unwrap().split(), ("", "x"));
    for bad in [
        "", "/x", "x/", "a//b", "../x", "a/../b", "./x", ".hidden", "a/.tmp", "a\0b",
    ] {
        assert_eq!(RelPath::parse(bad), None, "{bad}");
    }
    assert_eq!(join("", "x"), "x");
    assert_eq!(join("a/b", "x"), "a/b/x");
}
