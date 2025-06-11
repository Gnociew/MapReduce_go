package main

import (
	"mr/mr"
)

var Combine mr.Combine = func(pairs []*mr.Pair) []*mr.Pair {
	// 使用一个字典来存储中间结果
	combined := make(map[string]int)

	// 对所有的 key-value 对进行聚合
	for _, pair := range pairs {
		key, ok1 := pair.Key.(string)
		value, ok2 := pair.Value.(int)
		if !ok1 || !ok2 {
			continue // 或者可以选择处理类型断言失败的情况
		}
		combined[key] += value // 对相同 key 的 value 进行求和
	}

	// 将合并后的结果转换为 mr.Pair 类型的切片
	var result []*mr.Pair
	for key, value := range combined {
		result = append(result, &mr.Pair{Key: key, Value: value})
	}

	return result
}
