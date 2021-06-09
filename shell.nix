{ pkgs ? import <nixpkgs> {} }:

(
  pkgs.buildFHSUserEnv {
    name = "client-java-shell";
    targetPkgs = pkgs: with pkgs;[ git maven openjdk8 ];
    runScript = ''
      env \
       GIT_SSL_CAINFO=/etc/ssl/certs/ca-certificates.crt \
       JAVA_HOME=${pkgs.openjdk8}/lib/openjdk \
       bash
    '';
  }
).env
