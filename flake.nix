{
  description = "bumps-video dev shell";

  inputs.nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
  inputs.flake-utils.url = "github:numtide/flake-utils";

  outputs = { self, nixpkgs, flake-utils }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = nixpkgs.legacyPackages.${system};
        lib = pkgs.lib;
        gst = pkgs.gst_all_1;
        gstPlugins = [
          gst.gstreamer
          gst.gst-plugins-base
          gst.gst-plugins-good
          gst.gst-plugins-bad
          gst.gst-plugins-ugly
          gst.gst-libav
        ];
        # Intel QSV runtime only exists on Linux. Mac dev shell still gets
        # everything else so `cargo check` works locally.
        linuxOnly = lib.optionals pkgs.stdenv.isLinux [
          pkgs.libvpl
        ];
      in {
        devShells.default = pkgs.mkShell {
          buildInputs = gstPlugins ++ linuxOnly ++ [
            pkgs.glib
          ];
          packages = [
            pkgs.ffmpeg
            pkgs.python3
            pkgs.rustc
            pkgs.cargo
            pkgs.rust-analyzer
            pkgs.clippy
            pkgs.rustfmt
            pkgs.pkg-config
            # For scripts/aws-relay.sh (status/start/stop/urls)
            pkgs.awscli2
            pkgs.jq
          ];
          shellHook = ''
            export LIBVA_DRIVER_NAME=iHD
            export RUST_LOG="''${RUST_LOG:-info,bumps_pipe=debug}"
          '';
        };
      });
}
