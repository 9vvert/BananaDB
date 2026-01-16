fn main() {
    println!("cargo:rerun-if-changed=src/parser/sql.lalrpop");
    println!("cargo:rerun-if-changed=src/parser/ast.rs");
    println!("cargo:rerun-if-changed=src/parser/error.rs");
    println!("cargo:rerun-if-changed=src/parser/lexer.rs");
    lalrpop::process_root().expect("failed to run lalrpop");
}
