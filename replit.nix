{pkgs}: {
  deps = [
    pkgs.xsimd
    pkgs.pkg-config
    pkgs.libxcrypt
    pkgs.sqlite
    pkgs.cacert
    pkgs.glibcLocales
  ];
}
