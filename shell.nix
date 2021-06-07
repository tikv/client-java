{ pkgs ? import <nixpkgs> {} }:

(
  pkgs.buildFHSUserEnv {
    name = "client-java-shell";
    targetPkgs = pkgs: with pkgs;[ maven openjdk8 git ];
    runScript = "bash";
  }
).env
