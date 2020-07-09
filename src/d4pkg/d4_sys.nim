## !< The handle for a D4 file
{.passL: "-ld4binding -lpthread".}
{.pragma: d4, importc, header: "<d4.h>".}

type d4_file_t* = object
type d4_task_part_t* = object

## !< Describes what kind of dictionary this d4 file holds

type                          ## !< The dictionary that is defined by a range of values
  d4_dict_type_t* = enum
    D4_DICT_SIMPLE_RANGE = 0,   ## !< The dictionary describes by a value map
    D4_DICT_VALUE_MAP = 1


## !< The dictionary data for simple ranage dictionary

type
  d4_simple_range_dict_t* = object
    low*: int32
    high*: int32


## !< The dictionary data or value map dictionary

type
  d4_value_map_dict_t* = object
    size*: csize
    values*: ptr int32


## !< The metadata of a D4 file

type
  dict_data_t* = object {.union.}
    simple_range*: d4_simple_range_dict_t
    value_map*: d4_value_map_dict_t

  d4_file_metadata_t* {.bycopy.} = object
    chrom_count*: csize        ## !< Number of chromosomes defined in the file
    ## !< List of chromosome names
    chrom_name*: cstringArray  ## !< List o fchromosome sizes
    chrom_size*: ptr uint32   ## !< Dictionary type
    dict_type*: d4_dict_type_t ## !< Dictionary data
    dict_data*: dict_data_t


## !< A value interval

type
  d4_interval_t* {.bycopy.} = object
    left*: uint32
    right*: uint32
    value*: int32


proc free*(a1: pointer) {.cdecl, importc: "free", header: "<stdlib.h>".}

## !< Open a D4 file, mode can be either "r" or "w"

proc d4_open*(path: cstring; mode: cstring): ptr d4_file_t {.d4.}
## !< Close a opened D4 file

proc d4_close*(handle: ptr d4_file_t): cint {.d4.}
proc d4_file_load_metadata*(handle: ptr d4_file_t; buf: ptr d4_file_metadata_t): cint {.d4.}
proc d4_file_update_metadata*(handle: ptr d4_file_t;
                             metadata: ptr d4_file_metadata_t): cint {.d4.}

proc d4_file_metadata_clear*(meta: ptr d4_file_metadata_t) {.inline.} =
  if nil == meta:
    return
  var i: cint
  i = 0
  while i < meta.chrom_count:
    free(meta.chrom_name[i])
    inc(i)
  free(meta.chrom_name)
  free(meta.chrom_size)
  meta.chrom_name = nil
  meta.chrom_size = nil
  meta.chrom_count = 0
  if meta.dict_type == D4_DICT_VALUE_MAP:
    meta.dict_data.value_map.size = 0
    free(meta.dict_data.value_map.values)
    meta.dict_type = D4_DICT_SIMPLE_RANGE
    meta.dict_data.simple_range.low = 0
    meta.dict_data.simple_range.high = 1

##  The streaming API

proc d4_file_read_values*(handle: ptr d4_file_t; buf: ptr int32; count: csize): csize_t {.d4.}
proc d4_file_read_intervals*(handle: ptr d4_file_t; buf: ptr d4_interval_t;
                            count: csize): csize_t {.d4.}
proc d4_file_write_values*(handle: ptr d4_file_t; buf: ptr int32; count: csize): csize_t {.d4.}
proc d4_file_write_intervals*(handle: ptr d4_file_t; buf: ptr d4_interval_t;
                             count: csize): csize_t {.d4.}
proc d4_file_tell*(handle: ptr d4_file_t; name_buf: cstring; buf_size: csize;
                  pos_buf: ptr uint32): cint {.d4.}
proc d4_file_seek*(handle: ptr d4_file_t; chrom: cstring; pos: uint32): cint {.d4.}
##  The parallel API

type
  d4_task_mode_t* = enum
    D4_TASK_READ, D4_TASK_WRITE
  d4_task_part_result_t* {.bycopy.} = object
    task_context*: pointer
    status*: cint

  d4_task_desc_t* {.bycopy.} = object
    mode*: d4_task_mode_t
    part_size_limit*: uint32
    num_cpus*: uint32
    part_context_create_cb*: proc (handle: ptr d4_task_part_t; extra_data: pointer): pointer {.cdecl.}
    part_process_cb*: proc (handle: ptr d4_task_part_t; task_context: pointer;
                          extra_data: pointer): cint {.cdecl.}
    part_finalize_cb*: proc (tasks: ptr d4_task_part_result_t; count: csize;
                           extra_data: pointer): cint {.cdecl.}
    extra_data*: pointer

proc d4_file_run_task*(handle: ptr d4_file_t; task: ptr d4_task_desc_t): cint {.d4.}
proc d4_task_read_values*(task: ptr d4_task_part_t; offset: uint32;
                         buffer: ptr int32; count: csize): csize_t {.d4.}
proc d4_task_write_values*(task: ptr d4_task_part_t; offset: uint32;
                          data: ptr int32; count: csize): csize_t {.d4.}
proc d4_task_read_intervals*(task: ptr d4_task_part_t; data: ptr d4_interval_t;
                            count: csize): csize_t {.d4.}
proc d4_task_chrom*(task: ptr d4_task_part_t; name_buf: cstring; name_buf_size: csize): cint {.d4.}
proc d4_task_range*(task: ptr d4_task_part_t; left_buf: ptr uint32;
                   right_buf: ptr uint32): cint {.d4.}
##  The highlevel API

proc d4_file_profile_depth_from_bam*(bam_path: cstring; d4_path: cstring;
                                    header: ptr d4_file_metadata_t): cint {.d4.}
##  Error handling

proc d4_error_clear*() {.d4.}
proc d4_error_message*(buf: cstring; size: csize): cstring {.d4.}
proc d4_error_num*(): cint {.d4.}

when isMainModule:
  import strformat

  var path = "hg002.d4"
  var fp = d4_open(path, "r")
  echo fp == nil

  var mt:d4_file_metadata_t
  echo fp.d4_file_load_metadata(mt.addr)

  for i in 0..<mt.chrom_count.int:
    echo mt.chrom_name[i] , " ", cast[ptr UncheckedArray[int32]](mt.chrom_size)[i]

  mt.addr.d4_file_metadata_clear
  var pos:uint32

  var requested_chrom = "12"
  var requested_start = 1_121_343'u32
  var requested_stop =  1_129_343'u32
  var x = newString(20)

  #

  doAssert 0 == fp.d4_file_seek(requested_chrom, requested_start.uint32)
  var data = newSeq[d4_interval_t](2000)
  var done = false

  while not done:
    echo fp.d4_file_tell(x, x.len, pos.addr)
    echo x, " ", requested_chrom
    var same = true
    for i, c in requested_chrom:
      if x[i] != c:
        same = false

    if not same:
      echo "not equal", x.len, requested_chrom.len, " ", ($x).len
      break

    var count = fp.d4_file_read_intervals(data[0].addr, data.len)

    for i in 0..<count:
      echo &"{requested_chrom}\t{data[i].left}\t{data[i].right}\t{data[i].value}"
      if data[i].right >= requested_stop:
        #done = true
        break

  doAssert 0 == fp.d4_close

