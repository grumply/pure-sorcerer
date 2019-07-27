{ mkDerivation, base, bytestring, directory, filepath, hashable, containers, pure-elm, aeson, stdenv }:
mkDerivation {
  pname = "sorcerer";
  version = "0.7.0.0";
  src = ./.;
  libraryHaskellDepends = [ base bytestring directory filepath hashable containers pure-elm aeson ];
  license = stdenv.lib.licenses.bsd3;
}
