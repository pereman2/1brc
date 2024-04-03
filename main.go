package main

import (
	"bufio"
	"fmt"
	"math"
	_ "net/http/pprof"
	"os"
	"runtime/pprof"
	"sort"
	"strconv"
	"strings"
	"unsafe"
)

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
  h := uint64(0)
  for i := 0; i < len(*key); i++ {
    h += uint64((*key)[i])
    h %= 512;
  }
  return h
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

  fileScanner := bufio.NewScanner(file)
  total := 0
  for fileScanner.Scan() {
    total++
    text := fileScanner.Bytes()
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

    value, err := strconv.ParseFloat(*valueBytesString, 64)
    if err != nil {
      fmt.Println("Error converting to float")
      return
    }

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
      keys = append(keys, keyCopy)
    }

    // detect overflow
    // if math.MaxFloat64 - m[key].sum < value {
    //   fmt.Println("Overflow detected")
    //   return
    // }

    state.count++
    state.sum += value
    if value < state.min {
      state.min = value
    }
    if value > state.max {
      state.max = value
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
}
