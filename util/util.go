package util

type DataNodeInstance struct {
	Host        string //地址
	ServicePort string //端口号
}

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
