package main

import (
	// "bufio"
	"flag"
	"fmt"
	"io"
	_ "net/http/pprof"
	"os"
	"runtime"
	"runtime/pprof"
	"sort"

	"time"

	"bufio"
	"math"
	"strings"
	"sync"
	"sync/atomic"
	"unsafe"
)

var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to `file`")
var memprofile = flag.String("memprofile", "", "write memory profile to `file`")

type Event struct {
  s uint64
  f float64
  consumed bool
}


type Queue struct {
  q chan Event
}

type State struct {
  min float64
  max float64
  sum float64
  count int64
}

type Result struct {
  name string
  result State
}

type GState struct {
  texts chan *[]byte
  results chan Result
  backBufferMutex sync.Mutex
  backBufferCond sync.Cond
}

type NameKey struct {
  name string
  id int
}


type Bucket struct {
  key string
  hash uint64
  value uint64
  used bool
}

type HashMap struct {
  size int
  buckets [][]Bucket
}

func NewHashMap(size int) *HashMap {
  h := &HashMap{size: size, buckets: make([][]Bucket, size)}
  for i := 0; i < size; i++ {
    h.buckets[i] = make([]Bucket, 0)
    for j := 0; j < 6; j++ {
      h.buckets[i] = append(h.buckets[i], Bucket{used: false})
    }
  }
  return h
}


func stupidHash(key *string) uint64 {
  hash := uint64(0)
  hash += uint64((*key)[0]) << 32
  hash += uint64((*key)[1]) << 16
  hash += uint64((*key)[2]) << 8
  hash += uint64(len(*key))
  return uint64(hash) % 10000
}

func (m *HashMap) Get(key *string) *uint64 {
  h := stupidHash(key)

  bucketIndex := 0
  l := len(m.buckets[h])
  for bucketIndex = 0; bucketIndex < l && m.buckets[h][bucketIndex].used; bucketIndex++ {
    b := &m.buckets[h][bucketIndex]
    if b.key == *key {
      return &b.value
    }
  }
  return nil
}

func (m *HashMap) Put(key string, value uint64) {
  // stupid hash
  h := stupidHash(&key)

  bucketIndex := 0
  for bucketIndex = 0; bucketIndex < len(m.buckets[h]) && m.buckets[h][bucketIndex].used; bucketIndex++ {
    b := m.buckets[h][bucketIndex]
    if b.key == key {
      return
    }
  }
  if bucketIndex >= len(m.buckets[h]) {
    println("exeeded bucket len")
    os.Exit(1)
  }
  m.buckets[h][bucketIndex].key = key
  m.buckets[h][bucketIndex].hash = h
  m.buckets[h][bucketIndex].value = value
  m.buckets[h][bucketIndex].used = true
}


func fastFloat(repr *string) float64 {
  i := 0
  minus := false
  if (*repr)[0] == '-' {
    minus = true
    i++
  }

  dot := 0
  for ; i < len(*repr); i++ {
    if (*repr)[i] == '.' {
      dot = i
      break;
    }
  }

  start := 0
  if minus {
    start = 1
  }
  left := 0
  for l := dot-1; l >= start; l-- {
    left *= 10
    left += int((*repr)[l] - '0')
  }

  right := 0
  for l := len(*repr) - 1; l > dot; l-- {
    right *= 10
    right += int((*repr)[l] - '0')
  }
  res := float64(left);
  if right != 0 {
    res += 1.0 / float64(right)
  }
  if minus {
    res *= -1
  }
  return res
}


func Parser(ring *Queue, wg *sync.WaitGroup, writing *atomic.Int64, 
            gstate *GState, processTime *int64, start *time.Time) {
  total := 0
  state_arena := make([]State, 0)
  nameMap := NewHashMap(10000)
  fastMap := make([]*State, 10000)
  sortedNames := make([]string, 0)
  name_id := uint64(0)

  println("starting parser")
  bufferSize := 1024*1024
  scan_buf := make([]byte, 0, 1024*1024)
  for wholeText := range gstate.texts {
    textStr := unsafe.String(&(*wholeText)[0], len(*wholeText))
    parser := bufio.NewScanner(strings.NewReader(textStr))
    parser.Buffer(scan_buf, bufferSize)

    for parser.Scan() {
      text := parser.Bytes()
      if len(text) == 0 {
        continue
      }
      total++

      semiColonIndex := 0;
      for i, c := range text {
        if c == ';' {
          semiColonIndex = i
          break
        }
      }
      if semiColonIndex == 0 {
        break
      }

      keyBytes := text[:semiColonIndex]
      key := unsafe.String(&keyBytes[0], len(keyBytes))
      valueBytes := text[semiColonIndex + 1:]
      valueBytesString := (*string)(unsafe.Pointer(&valueBytes))
      ids := nameMap.Get(&key)
      if ids == nil {
        if cap(state_arena) <= len(state_arena) + 1 {
          state_arena = append(state_arena, State{})
        } else {
          state_arena = state_arena[:len(state_arena) + 1]
        }
        state := &state_arena[len(state_arena) - 1]
        state.min = math.MaxFloat64
        state.max = math.SmallestNonzeroFloat64
        state.sum = 0
        state.count = 0
        fastMap[name_id] = state
        // nameMap[strings.Clone(key)] = name_id
        nameMap.Put(strings.Clone(key), name_id)
        newid := uint64(name_id)
        ids = &newid
        sortedNames = append(sortedNames, strings.Clone(key))
        name_id++
      }

      
      value := fastFloat(valueBytesString)
      state := fastMap[*ids]
      state.count++
      state.sum += value
      if value < state.min {
        state.min = value
      }
      if value > state.max {
        state.max = value
      }
    }
  }

  // println("ending parser ", total)
  totall := 0
  for _, name := range sortedNames {
    id := int(*nameMap.Get(&name))
    state := fastMap[id]
    totall += int(state.count)
    result := Result {
      name: name,
      result: *state,
    }
    gstate.results <- result
  }
  // println("ending parser count ", totall)
  writing.Add(1)
}


func main() {
  // prof stuff
  if *cpuprofile != "" {
    f, err := os.Create("cpu.pprof")
    if err != nil {
      panic(err)
    }
    pprof.StartCPUProfile(f)
    defer pprof.StopCPUProfile()
  }

  file, err := os.OpenFile("measurements-pere.txt", os.O_RDONLY, 0666)
  if err != nil {
    fmt.Println("Error opening file")
    return
  }


  // m := make(map[string]*State)
  keys := make([]NameKey, 0)

  wg := sync.WaitGroup{}
  queue := Queue{q: make(chan Event, 10000)}
  gstate := GState{
    backBufferMutex: sync.Mutex{},
    texts: make(chan *[]byte, 10),
    results: make(chan Result, 10),
  }
  gstate.backBufferCond = sync.Cond{L: &gstate.backBufferMutex}
  bufferSize := 1024*1024
  numThreads := runtime.NumCPU()

  // ringBuffer := NewRingBuffer(10000)
  // ringBuffer, err := locklessgenericringbuffer.CreateBuffer[Event](1 << 16, 1)
  if err != nil {
    fmt.Println("Error creating ring buffer")
    return
  }
  // ringBuffer := ringbuffer.NewSpscRingBuffer(10000)
  writing := atomic.Int64{}
  writing.Store(0)
  processTime := int64(0)
  start := time.Now()

  for i := 0; i < numThreads; i++ {
    go Parser(&queue, &wg, &writing, &gstate, &processTime, &start)
  }

  total := 0

  sendTime := int64(0)
  waitSwapTotal := int64(0)


  offset := int64(0)

  for {
    buf := make([]byte, bufferSize)
    n, e := file.ReadAt(buf, offset)
    if e == io.EOF {
      buf = append(buf, '\n')
      offset += int64(n)
    } else {
      offset += int64(n)
      bufOffset := int64(len(buf) - 1)
      for buf[bufOffset] != '\n' {
        bufOffset--
      }
      back := (int64(len(buf) - 1) - bufOffset)
      offset -= back
      buf = buf[:bufOffset]

    }
    // for buf[offset] != '\n' {
    //   offset -= 1
    // }
    // TODO(pere): create
    // read to a new allocated buffer and send buffer to a pool of threads
    // those threads read line by line to a local copy of state map
    // at the end we aggregate all of them
    startSend := time.Since(start).Nanoseconds()

    gstate.texts <- &buf

    sendTime += time.Since(start).Nanoseconds() - startSend
    if e == io.EOF {
      break
    }
  }
  close(gstate.texts)
  totalTime := time.Since(start).Nanoseconds()


  name_id := uint64(0)
  nameMap := NewHashMap(10000)
  sortedNames := make([]string, 0)
  fastMap := make([]State, 10000)

  for result := range gstate.results {
    // println("results ", result.name, " ", result.result.count)
    total += int(result.result.count)
    ids := nameMap.Get(&result.name)
    if ids == nil {
      nameMap.Put(result.name, name_id)   
      sortedNames = append(sortedNames, result.name)
      new_id := name_id
      savedResult := &fastMap[new_id]
      savedResult.min = math.MaxFloat64
      savedResult.max = math.SmallestNonzeroFloat64
      savedResult.sum = 0
      savedResult.count = 0

      ids = &new_id
      name_id++
    }

    savedResult := &fastMap[*ids]
    if result.result.min < savedResult.min {
      savedResult.min = result.result.min
    }
    
    if result.result.max > savedResult.max {
      savedResult.max = result.result.max
    }
      
    savedResult.sum += result.result.sum
    savedResult.count += result.result.count
    if writing.Load() == int64(numThreads) && len(gstate.results) == 0 {
      break
    }
  }
  sort.Strings(sortedNames)
  fmt.Printf("{")

  for _, name := range sortedNames {
    id := int(*nameMap.Get(&name))
    state := fastMap[id]
    mean := state.sum / float64(state.count)
    fmt.Printf("%s=%f/%f/%f, ", name, state.min, state.max, mean)
    if id == int(name_id - 1) {
      fmt.Printf("\b\b")
    }
  }
  fmt.Printf("}")
  fmt.Println("\ntotal: ", total)
  fmt.Println("\nkeys: ", len(keys))

  wholeTime := time.Since(start).Nanoseconds()
  fmt.Printf("Parse thread time %fs\n", float64(totalTime) / float64(1000000000.0))
  fmt.Printf("\tScan time %fs\n", float64(totalTime - sendTime) / float64(1000000000.0))
  fmt.Printf("\tSend time %fs\n", float64(sendTime) / float64(1000000000.0))
  fmt.Printf("\tSwap wait time %fs\n", float64(waitSwapTotal) / float64(1000000000.0))
  fmt.Printf("Process thread time %fs\n", float64(processTime) / float64(1000000000.0))
  fmt.Printf("Total time %fs\n", float64(wholeTime) / float64(1000000000.0))



  if *memprofile != "" {
    f, err := os.Create("mem.pprof")
    if err != nil {
      println("could not create memory profile: ", err)
    }
    defer f.Close() // error handling omitted for example
    runtime.GC() // get up-to-date statistics
    if err := pprof.WriteHeapProfile(f); err != nil {
      println("could not write memory profile: ", err)
    }

  }
}
