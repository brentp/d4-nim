import ./d4_sys
import tables
export tables
import strformat

type D4* = object
  c: ptr d4_file_t
  chromosomes*: OrderedTableRef[string, uint32]

type Interval* = d4_interval_t

type task_ctx = object
  count: uint32
  sum: float64
  name: array[20, char]

proc `$`(c:task_ctx): string =
  return &"task_ctx(name:{$c.name}, count: {c.count}, sum: {c.sum})"

proc check(value:cint|csize_t, msg:string) {.inline.} =
  if value >= 0: return
  var buf = newString(256)
  stderr.write_line d4_error_message(buf[0].addr, buf.len.csize_t)
  raise newException(ValueError, msg)

proc init(h:ptr d4_task_part_t, extra_data:pointer): pointer {.cdecl.} =
  var ctx = cast[ptr task_ctx](alloc0(sizeof(task_ctx)))
  check(h.d4_task_chrom(ctx.name[0].addr, ctx.name.len - 1), "d4: error creating task")
  var l:uint32
  var r:uint32
  check(h.d4_task_range(l.addr, r.addr), "d4: error creating task range")
  ctx.count = r - l
  return ctx.pointer

type d4_call_back*[T] = proc(pos:uint32, values: seq[int32]): T

proc process[T](h: ptr d4_task_part_t, task_ctx_p: pointer, extra_data: pointer): cint {.cdecl.} =
  setupForeignThreadGc()
  if task_ctx_p == nil:
    return 1
  var
    pos: uint32
    r: uint32
  check(h.d4_task_range(pos.addr, r.addr), "d4: error creating task range")
  var ctx = cast[ptr task_ctx](task_ctx_p)

  let map_fn = cast[ptr d4_call_back[T]](extra_data)[]
  var buffer = newSeq[int32](10_000)

  while pos < r:
    var count = h.d4_task_read_values(pos, buffer[0].addr, buffer.len)
    check(count >= 0, "d4: error reading task values")
    if count.int != buffer.len: buffer.setLen(count)
    ctx.sum += map_fn(pos, buffer).a
    pos += count.uint32

  tearDownForeignThreadGc()
  return 0

proc map*[T](d4:var D4, map_fn:d4_call_back[T], n_cpus:int|uint32=8, chunk_size:int|uint32=10_000_000): seq[T] =
  var outer_result: seq[T]

  proc clean(d4_tasks: ptr d4_task_part_result_t, task_count: csize_t, extra_data: pointer): cint {.cdecl.} =
    var sum: float64
    var count: float64

    var tasks = cast[ptr UncheckedArray[d4_task_part_result_t]](d4_tasks)
    for i in 0..<task_count.int:
      var tctx = tasks[i].task_context
      let ctx = cast[ptr task_ctx](tctx)
      sum += ctx.sum
      count += ctx.count.float64
      dealloc(tctx)

    echo "mean depth:", sum / count
    return 0

  var task = d4_task_desc_t(mode: D4_TASK_READ,
                            part_size_limit: chunk_size.uint32,
                            num_cpus: n_cpus.uint32,
                            part_context_create_cb: init,
                            part_finalize_cb: clean,
                            part_process_cb: process[T],
                            extra_data: map_fn.unsafeAddr.pointer)

  var res = d4.c.d4_file_run_task(task.addr)
  if res != 0:
    var error = newString(128)
    echo d4_error_message(error, error.len)

proc fill_chromosomes*(d4:var D4): OrderedTableRef[string, uint32] =
  var mt:d4_file_metadata_t
  check(d4.c.d4_file_load_metadata(mt.addr), "d4: error loading metadata")
  result = newOrderedTable[string, uint32]()
  for i in 0..<mt.chrom_count.int:
    result[$mt.chrom_name[i]] = cast[ptr UncheckedArray[uint32]](mt.chrom_size)[i]
  mt.addr.d4_file_metadata_clear

proc open*(d4:var D4, path:string, mode="r"): bool {.discardable.} =
  d4.c = d4_open(path, mode)
  if mode[0] == 'r' and d4.c != nil:
    d4.chromosomes = d4.fill_chromosomes
  return d4.c != nil

proc index_build_sfi*(path: string): bool {.discardable.} =
  return d4_index_build_sfi(path.cstring) == 0

proc set_chromosomes*(d4:var D4, chroms: seq[tuple[name:string, length: int]]) =
  var chrom_list = newSeq[string](chroms.len)
  var size_list = newSeq[uint32](chroms.len)
  d4.chromosomes = newOrderedTable[string, uint32]()
  for i, v in chroms:
    d4.chromosomes[v.name] = v.length.uint32
    chrom_list[i] = v.name
    size_list[i] = (v.length).uint32

  let clist = chrom_list.allocCStringArray
  var hdr = d4_file_metadata_t(
    chrom_count: chroms.len.csize_t,
    chrom_name: clist,
    chrom_size: size_list[0].addr,
    dict_type: D4_DICT_SIMPLE_RANGE,
    denominator: 1,
    dict_data: dict_data_t(simple_range: d4_simple_range_dict_t(`low`: 0'i32, `high`: 128'i32))
    )

  check(d4_file_update_metadata(d4.c, hdr.addr), "d4: error updating metadata")
  clist.deallocCstringArray

proc close*(d4: var D4) =
  ## close d4 file and release memory.
  check(d4.c.d4_close(), "d4: error closing d4 file")
  d4.chromosomes = nil
  d4.c = nil

proc same(a:string, b:string): bool {.inline.} =
  if a.len < b.len: return false
  for i, c in b:
    if a[i] != c: return false
  return true

iterator query*(d4:var D4, chrom:string, start:int|uint32=0, stop:int|uint32=uint32.high): tuple[start: uint32, stop:uint32, value:int32] =
  if chrom notin d4.chromosomes:
    raise newException(KeyError, &"{chrom} not in known chromosomes")

  var stop = min(d4.chromosomes[chrom], stop.uint32)
  var start = start.uint32

  check(d4.c.d4_file_seek(chrom, start), "d4: error in seek")
  var data = newSeq[d4_interval_t](1000)
  var done = false
  var qchrom = newString(20)
  var pos:uint32

  while not done:
    discard d4.c.d4_file_tell(qchrom, qchrom.len, pos.addr)
    if not qchrom.same(chrom): break

    var count = d4.c.d4_file_read_intervals(data[0].addr, data.len)

    for i in 0..<count:
      yield (max(start, data[i].left), min(stop, data[i].right), data[i].value)
      if data[i].right >= stop:
        done = true
        break

    if count.int < data.len: break

proc values*(d4:var D4, chrom:string, start:int|uint32=0, stop:int|uint32=uint32.high): seq[int32] {.noInit.} =
  ## extract values for the requested region.

  var stop = min(d4.chromosomes[chrom], stop.uint32)
  check(d4_file_seek(d4.c, chrom.cstring, start.uint32), &"d4:error seeking to position: {chrom}:{start}")

  result = newSeqUninitialized[int32](stop - start.uint32)
  check(d4.c.d4_file_read_values(result[0].addr, result.len.csize_t), &"d4: error reading values at {chrom}:{start}")

proc values*(d4:var D4, chrom: string, pos:uint32, values: var seq[int32]) =
  # extract values from pos to pos + values.len without allocating memory.
  check(d4_file_seek(d4.c, chrom.cstring, pos), &"d4:error seeking to position: {chrom}:{pos}")
  check(d4.c.d4_file_read_values(values[0].addr, values.len.csize_t), "d4: error reading values")

proc write*(d4:var D4, chrom:string, pos:uint32|int, values:var seq[int32]) =
  # write a dense seq of values starting at pos
  check(d4_file_seek(d4.c, chrom.cstring, pos.uint32), &"d4:error seeking to position: {chrom}:{pos} writes must be in order")
  check(d4_file_write_values(d4.c, values[0].addr, values.len.csize_t), "d4:error writing values to position: " & $pos)

proc write*(d4:var D4, chrom:string, values:seq[Interval]) =
  # write a dense seq of values starting at pos
  check(d4_file_seek(d4.c, chrom.cstring, 0), "error seeking to chrom:" & chrom)
  check(d4_file_write_intervals(d4.c, values[0].unsafeAddr, values.len.csize_t), "d4:error writing values to chrom: " & chrom)


when isMainModule:
  import math
  var d4f:D4
  #[
  doAssert d4f.open("hg002.d4")
  echo d4f.chromosomes # ordered table
  echo d4f.chromosomes["1"]

  #for iv in d4f.query("1", 249_200_100):
  #  echo iv
  var vals = d4f.values("1", 249_200_100, 249240621)
  doAssert vals.len.uint32 == 249240621'u32 - 249_200_100'u32
  #echo vals.len
  #echo vals

  var fn:d4_call_back[tuple[a:float64, b:int]] = proc(pos:uint32, values:seq[int32]): tuple[a:float64, b:int] =
    result.a = values.sum.float64
  d4f.close

  #doAssert d4f.open("hg002.d4")
  #echo d4f.map(fn, n_cpus=8)

  #d4f.close

  ]# 

  doAssert d4f.open("test.d4", mode="w")
  d4f.set_chromosomes(@[(name: "chr1", length: 50), (name: "chr2", length: 60)])

  echo d4f.chromosomes
  var ivs = @[
  Interval(left: 0'u32, right: 10'u32, value: 1'i32),
  Interval(left: 10'u32, right: 20'u32, value: 2'i32)]

  #d4f.write("chr1", ivs)
  #
  var ivals = @[0'i32, 1'i32, 2'i32, 3, 4,5,6,7,8,9,10]
  d4f.write("chr1", 0, ivals)
  d4f.write("chr1", 38, ivals)

  d4f.write("chr2", 10, ivals)

  d4f.close

  doAssert d4f.open("test.d4", mode="r")
  echo d4f.chromosomes

  var r = d4f.values("chr1", 0, 50)
  doAssert r[1] == 1'i32
  doAssert r[40] == 2'i32
  var o = d4f.values("chr2", 0, 60)
  echo o
  doAssert o[12] == 2'i32
  doAssert d4.index_build_sfi("test.d4")
  
  