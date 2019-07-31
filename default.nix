{ mkDerivation, base, bytestring, unix, directory, filepath, hashable, containers, pure-elm, pure-json, aeson, stdenv }:
mkDerivation {
  pname = "sorcerer";
  version = "0.7.0.0";
  src = ./.;
  libraryHaskellDepends = [ base unix bytestring directory filepath hashable containers pure-elm pure-json aeson ];
  license = stdenv.lib.licenses.bsd3;
}
