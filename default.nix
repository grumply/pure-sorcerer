{ mkDerivation, ghc, base, bytestring, unix, directory, filepath, hashable, containers, pure-elm, pure-json, pure-time, pure-txt, aeson, unix-bytestring, stdenv }:
mkDerivation {
  pname = "pure-sorcerer";
  version = "0.8.0.0";
  src = ./.;
  libraryHaskellDepends = 
    [ base unix bytestring directory filepath hashable containers pure-elm pure-json pure-time pure-txt aeson ]
    ++ (if ghc.isGhcjs or false then [ ] else [ unix-bytestring ]);
  license = stdenv.lib.licenses.bsd3;
}
