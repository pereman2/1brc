package main

import (
	"bufio"
	"fmt"
	"math"
	_ "net/http/pprof"
	"os"
	"runtime/pprof"
	"sort"
	"time"

	// "strconv"
	"strings"
	"sync"
	"unsafe"
	// "github.com/cyub/ringbuffer"
	// "github.com/GavinClarke0/lockless-generic-ring-buffer"
)

type Event struct {
  s []byte
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
  state *State
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
  // h := uint64(0)
  // for i := 0; i < len(*key); i++ {
  //   h += uint64((*key)[i])
  //   h %= 512;
  // }
  return ( uint64(len(*key)) * uint64((*key)[0]) ) % 512
}

func (m *HashMap) Get(key *string) *State {
  h := stupidHash(key)

  bucketIndex := 0
  for bucketIndex = 0; bucketIndex < len(m.buckets[h]) && m.buckets[h][bucketIndex].used; bucketIndex++ {
    b := m.buckets[h][bucketIndex]
    if b.key == *key {
      return b.state
    }
  }
  return nil
}

func (m *HashMap) Put(key *string, state *State) {
  // stupid hash
  h := stupidHash(key)

  bucketIndex := 0
  for bucketIndex = 0; bucketIndex < len(m.buckets[h]) && m.buckets[h][bucketIndex].used; bucketIndex++ {
    b := m.buckets[h][bucketIndex]
    if b.key == *key {
      return
    }
  }
  m.buckets[h][bucketIndex].key = *key
  m.buckets[h][bucketIndex].hash = h
  m.buckets[h][bucketIndex].state = state
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


func Parser(ring *Queue, wg *sync.WaitGroup, writing *uint64, fastMap *HashMap, state_arena []State, keys *[]string) {
  // consumer, err := ring.CreateConsumer()
  // if err != nil {
  //   fmt.Println("Error creating consumer")
  //   return
  // }
  for {
    e := <-ring.q

    text := e.s
    semiColonIndex := 0;
    for i, c := range text {
      if c == ';' {
        semiColonIndex = i
        break
      }
    }
    keyBytes := text[:semiColonIndex]
    key := (*string)(unsafe.Pointer(&keyBytes))
    valueBytes := text[semiColonIndex + 1:]
    valueBytesString := (*string)(unsafe.Pointer(&valueBytes))

    // value, err := strconv.ParseFloat(*valueBytesString, 64)
    value := fastFloat(valueBytesString)
    // if err != nil {
    //   fmt.Println("Error converting to float")
    //   return
    // }

    state := fastMap.Get(key)
    if state == nil {
      if cap(state_arena) <= len(state_arena) + 1 {
        state_arena = append(state_arena, State{})
      } else {
        state_arena = state_arena[:len(state_arena) + 1]
      }
      state = &state_arena[len(state_arena) - 1]
      state.min = math.MaxFloat64
      state.max = math.SmallestNonzeroFloat64
      state.sum = 0
      state.count = 0
      keyCopy := strings.Clone(*key)
      fastMap.Put(&keyCopy, state)
      *keys = append(*keys, keyCopy)
    }
    // state := e.s
    // value := e.f
    state.count++
    state.sum += value
    if value < state.min {
      state.min = value
    }
    if value > state.max {
      state.max = value
    }
    *writing++
  }
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
  fastMap := NewHashMap(512)
  keys := make([]string, 0)

  wg := sync.WaitGroup{}
  queue := Queue{q: make(chan Event, 1000000)}
  // ringBuffer := NewRingBuffer(10000)
  // ringBuffer, err := locklessgenericringbuffer.CreateBuffer[Event](1 << 16, 1)
  if err != nil {
    fmt.Println("Error creating ring buffer")
    return
  }
  // ringBuffer := ringbuffer.NewSpscRingBuffer(10000)
  writing := uint64(0)

  go Parser(&queue, &wg, &writing, fastMap, state_arena, &keys)

  fileScanner := bufio.NewScanner(file)
  total := 0
  start := time.Now()
  sendTime := int64(0)
  for fileScanner.Scan() {
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
    startSend := time.Since(start).Nanoseconds()

    e := Event{f: 0}
    e.s = make([]byte, len(text))
    copy(e.s, text)
    queue.q <- e

    sendTime += time.Since(start).Nanoseconds() - startSend
  }
  totalTime := time.Since(start).Nanoseconds()


  for {
    if writing == uint64(total) {
      break
    }
  }
  sort.Strings(keys)
  fmt.Printf("{")
  for i, key := range keys {
    state := fastMap.Get(&key)
    mean := state.sum / float64(state.count)
    fmt.Printf("%s=%f/%f/%f, ", key, state.min, state.max, mean)
    if i == len(keys) - 1 {
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
