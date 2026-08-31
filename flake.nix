{
  description = "t4";

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
    flake-utils.lib.eachSystem
      [
        "x86_64-linux"
        "aarch64-darwin"
      ]
      (
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
          # Keep in sync with the `vstd` pin in Cargo.toml, and
          # verusRustToolchain with the release's rust-toolchain.toml.
          verusVersion = "0.2026.08.30.b432e82";
          verusArtifact =
            {
              x86_64-linux = {
                suffix = "x86-linux";
                hash = "sha256-VMM0e9/Gb6fBFIqGoZaB+XLdwph2s5KuIdX6gmSY/ss=";
              };
              aarch64-darwin = {
                suffix = "arm64-macos";
                hash = "sha256-no0xc8eGK7DNWLGiYUAirzGO8gBpr/RiIP4Rp6TmW4k=";
              };
            }
            .${system};
          verusRustToolchain = pkgs.rust-bin.stable."1.97.1".default.override {
            extensions = [
              "rustc-dev"
              "llvm-tools"
              "rust-src"
            ];
          };
          # Dynamic loader search path: patchelf'ed rpath on Linux, dyld fallback on macOS.
          verusLibPathVar =
            if pkgs.stdenv.hostPlatform.isDarwin then "DYLD_FALLBACK_LIBRARY_PATH" else "LD_LIBRARY_PATH";
          verus = pkgs.stdenvNoCC.mkDerivation {
            pname = "verus";
            version = verusVersion;

            src = pkgs.fetchzip {
              url = "https://github.com/verus-lang/verus/releases/download/release%2F${verusVersion}/verus-${verusVersion}-${verusArtifact.suffix}.zip";
              hash = verusArtifact.hash;
              stripRoot = false;
            };

            strictDeps = true;

            nativeBuildInputs = [
              pkgs.makeWrapper
            ] ++ pkgs.lib.optionals pkgs.stdenv.hostPlatform.isLinux [ pkgs.autoPatchelfHook ];
            buildInputs = pkgs.lib.optionals pkgs.stdenv.hostPlatform.isLinux [
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
              cp -r verus-${verusArtifact.suffix}/* "$out"/

              mv "$out/verus" "$out/verus-bin"
              mv "$out/cargo-verus" "$out/cargo-verus-bin"

              makeWrapper "$out/rust_verify" "$out/verus" \
                --set VERUS_ROOT "$out" \
                --set VERUS_Z3_PATH "$out/z3" \
                --prefix ${verusLibPathVar} : "${verusRustToolchain}/lib"

              makeWrapper "$out/cargo-verus-bin" "$out/cargo-verus" \
                --set VERUS_ROOT "$out" \
                --set VERUS_Z3_PATH "$out/z3" \
                --prefix ${verusLibPathVar} : "${verusRustToolchain}/lib"

              chmod +x "$out/rust_verify" "$out/z3" "$out/cargo-verus-bin" "$out/verus-bin" || true

              ln -s "$out/verus" "$out/bin/verus"
              ln -s "$out/cargo-verus" "$out/bin/cargo-verus"
              ln -s "$out/rust_verify" "$out/bin/rust_verify"
              ln -s "$out/z3" "$out/bin/z3"

              runHook postInstall
            '';
          };
          verusfmt = pkgs.rustPlatform.buildRustPackage (finalAttrs: {
            pname = "verusfmt";
            version = "0.7.2";

            src = pkgs.fetchFromGitHub {
              owner = "verus-lang";
              repo = "verusfmt";
              tag = "v${finalAttrs.version}";
              hash = "sha256-TE1Qyk5y8G/Kid6/BmUIMZ8Fr+y8GkLkRrzXGFHVe7I=";
            };

            cargoHash = "sha256-QY8Sju3AzfGiSp6V2TsuUlT1EmW3rOdhp3EeU1XM3Bg=";

            nativeCheckInputs = [
              pkgs.cargo
              pkgs.rustfmt
            ];

            doCheck = true;
          });
        in
        {
          packages.verus = verus;
          packages.verusfmt = verusfmt;

          devShells.default = pkgs.mkShell {
            packages = [
              rustToolchain
              pkgs.pkg-config
              pkgs.cargo-fuzz
              llvmPackages.llvm
              pkgs.cargo-binutils
              verus
              verusfmt
            ];
            ASAN_SYMBOLIZER_PATH = "${llvmPackages.llvm}/bin/llvm-symbolizer";
          };
        }
      );
}
