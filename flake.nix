{
  description = "t4";

  inputs = {
    crane.url = "github:ipetkov/crane";
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    rust-overlay.url = "github:oxalica/rust-overlay";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs =
    {
      crane,
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
          rustNightly = pkgs.rust-bin.nightly.latest;
          ciRustToolchain = rustNightly.minimal.override {
            extensions = [
              "clippy"
              "rustfmt"
            ];
          };
          rustToolchain = rustNightly.default.override {
            extensions = [
              "rust-src"
              "rust-analyzer"
              "clippy"
              "llvm-tools-preview"
            ];
          };
          # Keep in sync with the `vstd` pin in Cargo.toml, and
          # verusRustToolchain with the release's rust-toolchain.toml.
          verusVersion = "0.2026.11.02.8bf68ee";
          verusArtifact =
            {
              x86_64-linux = {
                suffix = "x86-linux";
                hash = "sha256-rRdcaRMbXq1Q5SN/JJ0vUzguLmF8i52PBYXgBSFUgtY=";
              };
              aarch64-darwin = {
                suffix = "arm64-macos";
                hash = "sha256-tWKvQlyZhEtCd0HEyeg5RatnmXx1GO9JJcttD6LudCA=";
              };
            }
            .${system};
          verusRustToolchain = pkgs.rust-bin.stable."1.99.1".minimal.override {
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
            ]
            ++ pkgs.lib.optionals pkgs.stdenv.hostPlatform.isLinux [ pkgs.autoPatchelfHook ];
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
          ciChecks =
            let
              craneLib = (crane.mkLib pkgs).overrideToolchain ciRustToolchain;
              src = craneLib.cleanCargoSource ./.;
              commonArgs = {
                inherit src;
                strictDeps = true;
                cargoExtraArgs = "--locked --offline --package t4";
                CARGO_PROFILE = "dev";
                cargoVendorDir = craneLib.vendorCargoDeps { inherit src; };
              };

              cargoArtifacts = craneLib.buildDepsOnly commonArgs;
              # __bench and __fuzz add no dependencies; this covers Clippy and Shuttle.
              shuttleArtifacts = craneLib.buildDepsOnly (
                commonArgs
                // {
                  pname = "t4-shuttle";
                  cargoExtraArgs = "${commonArgs.cargoExtraArgs} --all-features";
                }
              );

              verusArgs = commonArgs // {
                pname = "t4-proofs";
                nativeBuildInputs = [ verus ];
                doCheck = false;
                buildPhaseCargoCommand = "cargo verus focus ${commonArgs.cargoExtraArgs}";
              };
              verusArtifacts = craneLib.buildDepsOnly (
                builtins.removeAttrs verusArgs [ "src" ]
                // {
                  cargoToml = ./Cargo.toml;
                  dummySrc = craneLib.mkDummySrc {
                    inherit src;
                    # Verus needs its prelude even when checking a dependency stub.
                    dummyrs = pkgs.writeText "verus-dummy.rs" ''
                      use vstd::prelude::*;
                      fn main() {}
                    '';
                    # Build scripts have no access to the crate's dependencies.
                    dummyBuildrs = pkgs.writeText "dummy-build.rs" "fn main() {}";
                    # Preserve Verus's opt-in metadata in the dummy manifest.
                    cleanCargoTomlFilter =
                      path:
                      pkgs.lib.take 2 path == [
                        "package"
                        "metadata"
                      ]
                      || craneLib.filters.cargoTomlDefault path;
                  };
                }
              );

              checkArgs = commonArgs // {
                doInstallCargoArtifacts = false;
              };
            in
            {
              fmt = craneLib.cargoFmt {
                inherit src;
                cargoExtraArgs = "--all";
                doInstallCargoArtifacts = false;
              };
              clippy = craneLib.cargoClippy (
                checkArgs
                // {
                  cargoArtifacts = shuttleArtifacts;
                  cargoClippyExtraArgs = "--all-targets --all-features -- -D warnings";
                  # Code used only by feature-gated tests is dead without the features.
                  postBuild = "cargoWithProfile clippy ${commonArgs.cargoExtraArgs} --all-targets -- -D warnings";
                }
              );
              tests = craneLib.cargoTest (checkArgs // { inherit cargoArtifacts; });
              proofs = craneLib.mkCargoDerivation (
                verusArgs
                // {
                  cargoArtifacts = verusArtifacts;
                  doInstallCargoArtifacts = false;
                }
              );
              # Every test, not a name filter: the concurrent ones run only in this build.
              shuttle = craneLib.cargoTest (
                checkArgs
                // {
                  pname = "t4-shuttle";
                  cargoArtifacts = shuttleArtifacts;
                  cargoExtraArgs = "${commonArgs.cargoExtraArgs} --features shuttle";
                }
              );
            };
        in
        {
          checks = ciChecks;

          packages = ciChecks // {
            inherit verus verusfmt;
          };

          # CI checks only the root crate; benchmark and editor tools stay below.
          devShells.ci = pkgs.mkShell {
            packages = [
              ciRustToolchain
              verus
            ];
          };

          devShells.default = pkgs.mkShell {
            packages = [
              rustToolchain
              pkgs.pkg-config
              pkgs.cargo-fuzz
              llvmPackages.llvm
              pkgs.cargo-binutils
              verus
              verusfmt
              # fio, for the drives' baseline (doc/04); Python for plots.
              pkgs.fio
              (pkgs.python3.withPackages (ps: [ ps.matplotlib ]))
            ];
            ASAN_SYMBOLIZER_PATH = "${llvmPackages.llvm}/bin/llvm-symbolizer";
            # bindgen's libclang: the bench's Vortex needs it on Linux
            # (vortex-io's custom-labels), and the system's won't load here.
            LIBCLANG_PATH = "${llvmPackages.libclang.lib}/lib";
          };
        }
      );
}
