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
  m := make(map[string]*State)
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

    if m[*key] == nil {
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
      keyCopy := strings.Clone(*key)
      m[keyCopy] = state
      keys = append(keys, keyCopy)
    }
    // detect overflow
    // if math.MaxFloat64 - m[key].sum < value {
    //   fmt.Println("Overflow detected")
    //   return
    // }
    state := m[*key]
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
    mean := m[key].sum / float64(m[key].count)
    fmt.Printf("%s=%f/%f/%f, ", key, m[key].min, m[key].max, mean)
    if i == len(keys) - 1 {
      fmt.Printf("\b\b")
    }
  }
  fmt.Printf("}")
  fmt.Println("\ntotal: ", total)
}
