{
  pkgs,
  craneLib,
  src,
  verus,
}:
let
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
    verusArgs
    // {
      # Crane normally strips package metadata from its dummy sources.
      # Preserve Verus's opt-in so the dependency build uses the same driver.
      cleanCargoTomlFilter =
        path:
        pkgs.lib.take 2 path == [
          "package"
          "metadata"
        ]
        || craneLib.filters.cargoTomlDefault path;
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
}
// pkgs.lib.optionalAttrs pkgs.stdenv.hostPlatform.isLinux {
  shuttle = craneLib.cargoTest (
    checkArgs
    // {
      pname = "t4-shuttle";
      cargoArtifacts = shuttleArtifacts;
      cargoExtraArgs = "${commonArgs.cargoExtraArgs} --features shuttle";
      cargoTestExtraArgs = "shuttle";
    }
  );
}
