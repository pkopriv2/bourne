package scribe


type IntMessage int

func (i IntMessage) Write(w Writer) {
	w.WriteInt("val", int(i))
}

func ReadIntMessage(r Reader) (val int, err error)  {
	err = r.ReadInt("val", &val)
	return
}

func ParseIntMessage(r Reader) (interface{}, error) {
	return ReadIntMessage(r)
}

func NewIntMessage(val int) Message {
	return Write(IntMessage(val))
}
