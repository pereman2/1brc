package main

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
  "math"
  "sort"
)


type State struct {
  min float64
  max float64
  sum float64
  count int64
}


func main() {
  file, err := os.OpenFile("measurements.txt", os.O_RDONLY, 0666)
  if err != nil {
    fmt.Println("Error opening file")
    return
  }


  m := make(map[string]*State)
  keys := make([]string, 0)

  fileScanner := bufio.NewScanner(file)
  for fileScanner.Scan() {
    text := fileScanner.Text()
    f := strings.Split(text, ";")
    value, err := strconv.ParseFloat(f[1], 64)
    if err != nil {
      fmt.Println("Error converting to float")
      return
    }
    key := f[0]
    if m[key] == nil {
      m[key] = &State{min: math.MaxFloat64, max: math.SmallestNonzeroFloat64, sum: 0, count: 0}
      keys = append(keys, key)
    }
    // detect overflow
    if math.MaxFloat64 - m[key].sum < value {
      fmt.Println("Overflow detected")
      return
    }
    m[key].count++
    m[key].sum += value
    m[key].min = math.Min(m[key].min, value)
    m[key].max = math.Max(m[key].max, value)
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
}
