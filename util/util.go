package util

func Check(e error) {
	if e != nil {
		panic(e)
	}
}

func CheckStatus(e bool) {
	if !e {
		panic(e)
	}
}
