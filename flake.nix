{
  description = "A terminal UI to connect to message/data brokers (Redis, AMQP), browse data, watch realtime activity, and record live streams to disk.";

  inputs.nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";

  outputs =
    { self, nixpkgs }:
    let
      # Cargo.toml is the single source of truth for the package version, so a
      # `cargo release` bump flows into the flake without a second edit here.
      cargoToml = builtins.fromTOML (builtins.readFile ./Cargo.toml);
      inherit (cargoToml.package) version;

      # Linux is the primary target; the Darwin systems are covered as a source
      # build so `nix run`/`nix profile install` also work on macOS (Linuxbrew
      # parity), even though no prebuilt macOS binary is published.
      systems = [
        "x86_64-linux"
        "aarch64-linux"
        "x86_64-darwin"
        "aarch64-darwin"
      ];
      forAllSystems =
        f: nixpkgs.lib.genAttrs systems (system: f nixpkgs.legacyPackages.${system});

      keyhole =
        pkgs:
        pkgs.rustPlatform.buildRustPackage {
          pname = "keyhole";
          inherit version;

          # Build from the working tree, but keep the heavy/irrelevant paths out
          # of the Nix store: the build artifacts (`target/`), any `result`
          # symlinks, and VCS/editor cruft (via cleanSourceFilter).
          src = pkgs.lib.cleanSourceWith {
            name = "keyhole-source";
            src = ./.;
            filter =
              path: type:
              let
                base = baseNameOf (toString path);
              in
              # Drop the cargo build dir and nix `result`/`result-*` symlinks, but
              # never a source file that merely starts with "result".
              !(base == "target" || base == "result" || pkgs.lib.hasPrefix "result-" base)
              && pkgs.lib.cleanSourceFilter path type;
          };

          # Reuse the committed lockfile: a fully reproducible build with no
          # vendored-output hash to keep in sync. All dependencies come from the
          # crates.io registry (no git deps), so no `outputHashes` are needed.
          cargoLock.lockFile = ./Cargo.lock;

          # Default features = the full glibc flavor (keyring + amqp + rabbitmq).
          # cmake builds the bundled C of aws-lc-sys (rustls' crypto backend
          # behind the amqp/rabbitmq TLS support); bindgenHook supplies libclang
          # for its bindings; perl is needed by aws-lc's build scripts. The OS
          # keyring backend is pure-Rust zbus (it speaks the Secret Service
          # D-Bus protocol at runtime), so it pulls in no build inputs.
          nativeBuildInputs = with pkgs; [
            cmake
            perl
            pkg-config
            installShellFiles
            rustPlatform.bindgenHook
          ];

          # The test suite is run as a gate in CI (`cargo test`); skip it here so
          # the package build does not depend on the snapshot/unit suite passing
          # inside the Nix sandbox (no network, restricted $HOME). The
          # broker-backed integration tests are feature-gated and never run here.
          doCheck = false;

          # Generate and install the man page + shell completions from the
          # freshly built binary (a native build, so it runs on the build host).
          # This mirrors what the release tarballs and the AUR/Homebrew packages
          # ship, and keeps the artifacts in lockstep with the actual CLI.
          postInstall = ''
            $out/bin/keyhole gen man --out .
            $out/bin/keyhole gen completions bash --out .
            $out/bin/keyhole gen completions zsh --out .
            $out/bin/keyhole gen completions fish --out .
            installManPage keyhole.1
            installShellCompletion keyhole.bash _keyhole keyhole.fish
          '';

          meta = {
            description = cargoToml.package.description;
            homepage = "https://github.com/AlexKasapis/Keyhole";
            license = with pkgs.lib.licenses; [
              mit
              asl20
            ];
            mainProgram = "keyhole";
            platforms = pkgs.lib.platforms.unix;
          };
        };
    in
    {
      packages = forAllSystems (
        pkgs:
        let
          pkg = keyhole pkgs;
        in
        {
          keyhole = pkg;
          default = pkg;
        }
      );

      apps = forAllSystems (
        pkgs:
        let
          app = {
            type = "app";
            program = "${self.packages.${pkgs.system}.keyhole}/bin/keyhole";
          };
        in
        {
          keyhole = app;
          default = app;
        }
      );

      # `nix develop` shell with the toolchain + the lint/format tooling CI uses.
      devShells = forAllSystems (pkgs: {
        default = pkgs.mkShell {
          inputsFrom = [ self.packages.${pkgs.system}.keyhole ];
          packages = with pkgs; [
            cargo
            rustc
            clippy
            rustfmt
            rust-analyzer
          ];
        };
      });

      formatter = forAllSystems (pkgs: pkgs.nixfmt-rfc-style);
    };
}
