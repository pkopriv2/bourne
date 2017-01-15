package common

func Min(l int , others... int) int {
	min := l
	for _, o := range others {
		if o < min {
			min = o
		}
	}
	return min
}

func Max(l int , others... int) int {
	max := l
	for _, o := range others {
		if o > max {
			max = o
		}
	}
	return max
}
