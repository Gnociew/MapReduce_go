package main

import "mr/mr"

var Reduce mr.Reduce = func(key any, values []any) *mr.Pair {
	count := 0
	for _, value := range values {
		// 处理float64类型，JSON解析会将数字转为float64
		switch v := value.(type) {
		case int:
			count += v
		case float64:
			count += int(v)
		default:
			// 其他类型尝试转为float64再转为int
			if fv, ok := v.(float64); ok {
				count += int(fv)
			}
		}
	}
	return &mr.Pair{Key: key, Value: count}
}
