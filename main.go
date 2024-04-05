package main

import (
	"bufio"
	"fmt"
	"math"
	_ "net/http/pprof"
	"os"
	"runtime/pprof"
	"strings"

	"time"

	"sync"
	"unsafe"
)

type Event struct {
  s uint64
  f float64
  consumed bool
}

// type RingBuffer struct {
//   buffer []Event
//   readIndex int
//   writeIndex int
//   m sync.Mutex
//   items int
//   s *sync.Cond
// }
//
// func NewRingBuffer(size int) *RingBuffer {
//
//   ring := &RingBuffer{buffer: make([]Event, size), readIndex: 0, writeIndex: 0}
//   for i := 0; i < size; i++ {
//     ring.buffer[i] = Event{consumed: true}
//   }
//   ring.items = 0
//   ring.s = sync.NewCond(&ring.m)
//   return ring
// }
//
// func (r *RingBuffer) Write(e Event) {
//   for {
//     if r.buffer[r.writeIndex].consumed {
//       break
//     }
//   }
//
//   r.buffer[r.writeIndex].consumed = false
//   r.buffer[r.writeIndex].s = e.s
//   if r.writeIndex == 3 {
//     fmt.Println("writeIndex: ", r.writeIndex)
//     fmt.Println("readIndex: ", r.readIndex)
//     fmt.Println("consumed: ", r.buffer[r.writeIndex].consumed)
//     fmt.Println("state: ", r.buffer[r.writeIndex].s)
//     fmt.Println("value: ", r.buffer[r.writeIndex].f)
//   }
//   r.items++
//   r.buffer[r.writeIndex].f = e.f
//   r.s.Signal()
//
//   r.writeIndex = (r.writeIndex + 1) % len(r.buffer);
// }
//
// func (r *RingBuffer) Read() Event {
//   r.s.Wait()
//   e := r.buffer[r.readIndex]
//   if e.s == nil {
//     fmt.Println("nil state")
//     fmt.Println("writeIndex: ", r.writeIndex)
//     fmt.Println("readIndex: ", r.readIndex)
//     fmt.Println("consumed: ", r.buffer[r.readIndex].consumed)
//     fmt.Println("state: ", r.buffer[r.readIndex].s)
//     fmt.Println("value: ", r.buffer[r.readIndex].f)
//     fmt.Println(r.readIndex)
//     os.Exit(1)
//   }
//   r.buffer[r.readIndex].consumed = true
//   r.readIndex = (r.readIndex + 1) % len(r.buffer);
//   return e
// }
//

type Queue struct {
  q chan Event
}

type State struct {
  min float64
  max float64
  sum float64
  count int64
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
    for j := 0; j < size; j++ {
      h.buckets[i] = append(h.buckets[i], Bucket{used: false})
    }
  }
  return h
}


func stupidHash(key *string) uint64 {
  hash := uint64(0)
  hash = hash + 31 + uint64((*key)[0])
  return uint64(hash) % 1000
}

func (m *HashMap) Get(key *string) *uint64 {
  h := stupidHash(key)

  bucketIndex := 0
  for bucketIndex = 0; bucketIndex < len(m.buckets[h]) && m.buckets[h][bucketIndex].used; bucketIndex++ {
    b := m.buckets[h][bucketIndex]
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


func Parser(ring *Queue, wg *sync.WaitGroup, writing *uint64, fastMap *[]*State, state_arena []State, gstate *GState) {
  // consumer, err := ring.CreateConsumer()
  // if err != nil {
  //   fmt.Println("Error creating consumer")
  //   return
  // }
  for {
    gstate.backBufferMutex.Lock()
    gstate.backBufferCond.Wait()
    for i := 0; i < len(gstate.backBuffer); i++ {
      e := &gstate.backBuffer[i]
      id := e.s
      value := e.f
      state := (*fastMap)[id]
      if state == nil {
        println("state is nil")
        println("id: ", id)
        println("value: ", value)
        os.Exit(1)
      }
      state.count++
      state.sum += value
      if value < state.min {
        state.min = value
      }
      if value > state.max {
        state.max = value
      }
      *writing++
      gstate.backBuffer[i].consumed = true
    }
    gstate.backBufferMutex.Unlock()
  }
}

type GState struct {
  backBuffer []Event
  frontBuffer []Event
  backBufferMutex sync.Mutex
  backBufferCond sync.Cond
}

type NameKey struct {
  name string
  id int
}

func main() {
  // prof stuff
  f, err := os.Create("cpu.pprof")
  if err != nil {
    panic(err)
  }
  pprof.StartCPUProfile(f)
  defer pprof.StopCPUProfile()


  // end prof
  file, err := os.OpenFile("measurements-pere.txt", os.O_RDONLY, 0666)
  if err != nil {
    fmt.Println("Error opening file")
    return
  }


  state_arena := make([]State, 0)
  // m := make(map[string]*State)
  fastMap := make([]*State, 10000)
  keys := make([]NameKey, 0)

  wg := sync.WaitGroup{}
  queue := Queue{q: make(chan Event, 10000)}
  gstate := GState{}
  gstate.backBuffer = make([]Event, 10000)
  gstate.backBuffer[0].consumed = true
  gstate.frontBuffer = make([]Event, 10000)
  gstate.backBufferMutex = sync.Mutex{}
  gstate.backBufferCond = sync.Cond{L: &gstate.backBufferMutex}
  println("len backBuffer", len(gstate.backBuffer))

  // ringBuffer := NewRingBuffer(10000)
  // ringBuffer, err := locklessgenericringbuffer.CreateBuffer[Event](1 << 16, 1)
  if err != nil {
    fmt.Println("Error creating ring buffer")
    return
  }
  // ringBuffer := ringbuffer.NewSpscRingBuffer(10000)
  writing := uint64(0)

  go Parser(&queue, &wg, &writing, &fastMap, state_arena, &gstate)

  fileScanner := bufio.NewScanner(file)
  total := 0
  start := time.Now()
  sendTime := int64(0)
  name_id := uint64(0)
  nameMap := NewHashMap(1000)
  writePos := 0
  for fileScanner.Scan() == true {
    total++
    text := fileScanner.Bytes()


    // detect overflow
    // if math.MaxFloat64 - m[key].sum < value {
    //   fmt.Println("Overflow detected")
    //   return
    // }
    // queue.q <- Event{s: state, f: value}
    // println("add text:", string(text))
    // ringBuffer.Write(Event{s: text, f: 0})
    // startSend := time.Since(start).Nanoseconds()

    semiColonIndex := 0;
    for i, c := range text {
      if c == ';' {
        semiColonIndex = i
        break
      }
    }
    keyBytes := text[:semiColonIndex]
    key := unsafe.String(&keyBytes[0], len(keyBytes))
    valueBytes := text[semiColonIndex + 1:]
    valueBytesString := (*string)(unsafe.Pointer(&valueBytes))
    ids := nameMap.Get(&key)
    // print("key: ", key, " id: ", ids, " name_id ", name_id, "\n")
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
      (*ids) = uint64(name_id)
      // *keys = append(*keys, keyCopy)
      name_id++
    }

    // value, err := strconv.ParseFloat(*valueBytesString, 64)
    value := fastFloat(valueBytesString)

    gstate.frontBuffer[writePos].f = value
    gstate.frontBuffer[writePos].s = uint64(*ids)
    gstate.frontBuffer[writePos].consumed = false
    writePos++
    if (total == 100000000 ) {
      println("break")
      break
    }
    if (writePos == len(gstate.frontBuffer)) {
      gstate.backBufferMutex.Lock()
      // println("swap ", total/1000000)
      gstate.backBuffer, gstate.frontBuffer = gstate.frontBuffer, gstate.backBuffer
      writePos = 0
      gstate.backBufferMutex.Unlock()
      gstate.backBufferCond.Signal()
    }

    // sendTime += time.Since(start).Nanoseconds() - startSend
  }
  {
    gstate.backBufferMutex.Lock()
    gstate.backBuffer, gstate.frontBuffer = gstate.frontBuffer, gstate.backBuffer
    writePos = 0
    gstate.backBufferMutex.Unlock()
    gstate.backBufferCond.Signal()
  }
  totalTime := time.Since(start).Nanoseconds()


  for {
    println("writing: ", writing, " total: ", total)
    if writing == uint64(total) {
      break
    }
  }
  // sort.Strings(keys)
  // TODO: sort keys
  fmt.Printf("{")
  for id, name := range fastMap {
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
  fmt.Printf("Total time %fs\n", float64(wholeTime) / float64(1000000000.0))
}
