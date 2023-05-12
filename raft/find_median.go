package raft

import "math/rand"

func findMed(arr []int) int {
	arrc := make([]int, len(arr))
	copy(arrc, arr)
	return process(arr, 0, len(arr)-1, len(arr)/2)
}

func process(arr []int, L int, R int, index int) int {
	if L == R {
		return arr[L]
	}

	pivot := arr[L+rand.Intn(R-L)]
	rang := partition(arr, L, R, pivot)
	if index >= rang[0] && index <= rang[1] {
		return arr[index]
	} else if index < rang[0] {
		return process(arr, L, rang[0]-1, index)
	} else {
		return process(arr, rang[1]+1, R, index)
	}
}

func partition(arr []int, L int, R int, pivot int) []int {
	less := L - 1
	more := R + 1
	cur := L
	for cur < more {
		if arr[cur] < pivot {
			less++
			arr[less], arr[cur] = arr[cur], arr[less]
			cur++
		} else if arr[cur] > pivot {
			more--
			arr[cur], arr[more] = arr[more], arr[cur]
		} else {
			cur++
		}
	}
	return []int{less + 1, more - 1}
}
