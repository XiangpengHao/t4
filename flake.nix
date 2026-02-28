{
  description = "parquet-linter";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    rust-overlay.url = "github:oxalica/rust-overlay";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { nixpkgs, rust-overlay, flake-utils, ... }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = import nixpkgs {
          inherit system;
          overlays = [ (import rust-overlay) ];
        };
        llvmPackages = pkgs.llvmPackages_latest;
        rustToolchain = pkgs.rust-bin.nightly.latest.default.override {
          extensions = [ "rust-src" "rust-analyzer" "clippy" ];
        };
      in {
        devShells.default = pkgs.mkShell {
          packages =
            [ rustToolchain pkgs.pkg-config pkgs.cargo-fuzz llvmPackages.llvm ];
          ASAN_SYMBOLIZER_PATH = "${llvmPackages.llvm}/bin/llvm-symbolizer";
        };
      });
}
