package utils

func MapForEach[K comparable, V any](in map[K]V, iteratee func(K, V, int)) {
	idx := 0
	for key, value := range in {
		iteratee(key, value, idx)
		idx++
	}
}
