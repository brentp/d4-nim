# Package

version       = "0.0.3"
author        = "Brent Pedersen"
description   = "bindings to d4 by Hao"
license       = "MIT"


# Dependencies

requires "nim >= 0.19.9"
srcDir = "src"

import os, strutils

task test, "run the tests":
  exec "nim c --mm:refc -d:useSysAssert -d:useGcAssert --lineDir:on --debuginfo -r src/d4pkg/d4.nim"

