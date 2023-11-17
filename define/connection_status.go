package define

type ConnStatusType uint32

const (
	ConnStatusNotCreate uint32 = iota
	ConnStatusCreated
	ConnStatusIdle
	ConnStatusBusy
	ConnStatusClosing
)
