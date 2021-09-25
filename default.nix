{ mkDerivation, base, bytestring, unix, directory, filepath, hashable, containers, pure-elm, pure-json, aeson, stdenv }:
mkDerivation {
  pname = "pure-sorcerer";
  version = "0.8.0.0";
  src = ./.;
  libraryHaskellDepends = [ base unix bytestring directory filepath hashable containers pure-elm pure-json aeson ];
  license = stdenv.lib.licenses.bsd3;
}
