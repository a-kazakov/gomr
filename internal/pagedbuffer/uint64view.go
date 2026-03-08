package pagedbuffer

type Uint64View struct {
	buffer PagedBuffer
}

func NewUint64View(orchestrator *PagedBufferOrchestrator) Uint64View {
	return Uint64View{
		buffer: NewPagedBuffer(orchestrator),
	}
}

func (v *Uint64View) PushBack(value uint64) {
	v.buffer.PushBackUint64Unsafe(value)
}

func (v *Uint64View) Get(index int) uint64 {
	return v.buffer.ReadUint64AtIndexUnsafe(index)
}

func (v *Uint64View) Len() int {
	return v.buffer.Size >> 3
}

func (v *Uint64View) Swap(i int, j int) {
	t := v.buffer.ReadUint64AtIndexUnsafe(i)
	v.buffer.WriteUint64AtIndexUnsafe(i, v.buffer.ReadUint64AtIndexUnsafe(j))
	v.buffer.WriteUint64AtIndexUnsafe(j, t)
}

func (v *Uint64View) IsFull() bool {
	return !v.buffer.CanTake(8)
}

func (v *Uint64View) Clear() {
	v.buffer.Clear()
}
