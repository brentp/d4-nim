# d4-nim

This is a [nim](https://nim-lang.org) wrapper for Hao Hou's [d4](https://github.com/38/d4-format) which
can be thought of as a way to store integer data in a compressed array. A common use-case will be depth storage
and querying. His library also has a command-line depth extractor that is faster than [mosdepth](https://github.com/brentp/mosdepth).
The extracted (compressed) file can quickly queried for summary info such as mean depth of a region.


Usage in nim looks like:

```Nim
var d4f:D4
doAssert d4f.open("hg002.d4")
echo d4f.chromosomes # ordered table
echo d4f.chromosomes["1"]

for iv in d4f.query("1", 249_200_100):
  echo iv # (tuple of start:uint32, end:uint32, value:int32)

var start = 249_200_100'u32
var stop = 249_240_621'u32
# each entry in vals contains the depth for pos-start
var vals:seq[int32] = d4f.values("1", start, stop)
```

