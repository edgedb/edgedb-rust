{
  description = "The EdgeDB CLI";
  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs";
    flake-parts.url = "github:hercules-ci/flake-parts";

    # provides rust toolchain
    fenix = {
      url = "github:nix-community/fenix";
      inputs.nixpkgs.follows = "nixpkgs";
      inputs.rust-analyzer-src.follows = "";
    };

    edgedb = {
      url = "github:edgedb/packages-nix";
      inputs.nixpkgs.follows = "nixpkgs";
      inputs.flake-parts.follows = "flake-parts";
      inputs.fenix.follows = "fenix";
    };
  };

  outputs = inputs@{ flake-parts, fenix, edgedb, ... }:
    flake-parts.lib.mkFlake {inherit inputs;} {
      systems = ["x86_64-linux" "x86_64-darwin" "aarch64-darwin"];
      perSystem = { config, system, pkgs, ... }:
        let
          fenix_pkgs = fenix.packages.${system};

          common = [
            pkgs.just

            # needed for tests
            edgedb.packages.${system}.edgedb-server
            edgedb.packages.${system}.edgedb-cli
          ]
          ++ pkgs.lib.optional pkgs.stdenv.isDarwin [
            pkgs.libiconv
            pkgs.darwin.apple_sdk.frameworks.CoreServices
            pkgs.darwin.apple_sdk.frameworks.SystemConfiguration
          ];
        in {

          # toolchain defined in rust-toolchain.toml
          devShells.default = pkgs.mkShell {
            buildInputs = [
              (fenix_pkgs.fromToolchainFile {
                file = ./rust-toolchain.toml;
                sha256 = "sha256-3jVIIf5XPnUU1CRaTyAiO0XHVbJl12MSx3eucTXCjtE=";
              })
            ] ++ common;
          };

          # minimum supported rust version of this crate
          devShells.minimum = pkgs.mkShell {
            buildInputs = [
              (fenix_pkgs.toolchainOf {
                channel = "1.75"; # keep in sync with ./Cargo.toml rust-version
                sha256 = "sha256-SXRtAuO4IqNOQq+nLbrsDFbVk+3aVA8NNpSZsKlVH/8=";
              }).defaultToolchain
            ] ++ common;
          };

          # rust beta version
          devShells.beta = pkgs.mkShell {
            buildInputs = [
              fenix_pkgs.beta.defaultToolchain
            ] ++ common;
          };
        };
    };
}
