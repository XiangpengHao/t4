{
  description = "parquet-linter";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    rust-overlay.url = "github:oxalica/rust-overlay";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs =
    {
      nixpkgs,
      rust-overlay,
      flake-utils,
      ...
    }:
    flake-utils.lib.eachSystem [ "x86_64-linux" ] (
      system:
      let
        pkgs = import nixpkgs {
          inherit system;
          overlays = [ (import rust-overlay) ];
        };
        llvmPackages = pkgs.llvmPackages_latest;
        rustToolchain = pkgs.rust-bin.nightly.latest.default.override {
          extensions = [
            "rust-src"
            "rust-analyzer"
            "clippy"
            "llvm-tools-preview"
          ];
        };
        verusVersion = "0.2026.02.22.2c9a6a0";
        verusRustToolchain = pkgs.rust-bin.stable."1.93.0".default.override {
          extensions = [
            "rustc-dev"
            "llvm-tools"
            "rust-src"
          ];
        };
        verus = pkgs.stdenvNoCC.mkDerivation {
          pname = "verus";
          version = verusVersion;

          src = pkgs.fetchzip {
            url = "https://github.com/verus-lang/verus/releases/download/release%2F${verusVersion}/verus-${verusVersion}-x86-linux.zip";
            hash = "sha256-vTh7kek4D4GpJJk3sjlXDjmpqdJYjMvxgsHWeIfYU0Y=";
            stripRoot = false;
          };

          strictDeps = true;

          nativeBuildInputs = [
            pkgs.autoPatchelfHook
            pkgs.makeWrapper
          ];
          buildInputs = [
            pkgs.zlib
            pkgs.stdenv.cc.cc.lib
          ];
          autoPatchelfIgnoreMissingDeps = [
            "librustc_driver*"
            "libLLVM*"
            "libstd-*"
          ];

          installPhase = ''
            runHook preInstall

            mkdir -p "$out/bin"
            cp -r verus-x86-linux/* "$out"/

            mv "$out/verus" "$out/verus-bin"
            mv "$out/cargo-verus" "$out/cargo-verus-bin"

            makeWrapper "$out/rust_verify" "$out/verus" \
              --set VERUS_ROOT "$out" \
              --set VERUS_Z3_PATH "$out/z3" \
              --prefix LD_LIBRARY_PATH : "${verusRustToolchain}/lib"

            makeWrapper "$out/cargo-verus-bin" "$out/cargo-verus" \
              --set VERUS_ROOT "$out" \
              --set VERUS_Z3_PATH "$out/z3" \
              --prefix LD_LIBRARY_PATH : "${verusRustToolchain}/lib"

            chmod +x "$out/rust_verify" "$out/z3" "$out/cargo-verus-bin" "$out/verus-bin" || true

            ln -s "$out/verus" "$out/bin/verus"
            ln -s "$out/cargo-verus" "$out/bin/cargo-verus"
            ln -s "$out/rust_verify" "$out/bin/rust_verify"
            ln -s "$out/z3" "$out/bin/z3"

            runHook postInstall
          '';
        };
      in
      {
        packages.verus = verus;

        devShells.default = pkgs.mkShell {
          packages = [
            rustToolchain
            pkgs.pkg-config
            pkgs.cargo-fuzz
            llvmPackages.llvm
            pkgs.cargo-binutils
            verus
          ];
          ASAN_SYMBOLIZER_PATH = "${llvmPackages.llvm}/bin/llvm-symbolizer";
        };
      }
    );
}
