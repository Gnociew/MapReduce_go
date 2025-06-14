package main

import (
	"mr/mr"
	"strings"
)

// 对 mr.Map 函数的具体实现
var Map mr.Map = func(line string) []*mr.Pair {
	words := strings.Split(line, " ")
	pairs := make([]*mr.Pair, 0, len(words))
	for _, word := range words {
		pairs = append(pairs, &mr.Pair{Key: word, Value: 1})
	}
	return pairs
}
