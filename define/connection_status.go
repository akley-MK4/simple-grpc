package define

import "fmt"

type ConnStatusType uint32

const (
	GRPCConnStatusInvalid int = -1
)

const (
	ConnStatusNotCreate uint32 = iota
	ConnStatusCreated
	ConnStatusIdle
	ConnStatusBusy
	ConnStatusClosing
	ConnStatusStopped
)

var (
	connStatusDescMap = map[uint32]string{
		ConnStatusNotCreate: "NotCreate",
		ConnStatusCreated:   "Created",
		ConnStatusIdle:      "Idle",
		ConnStatusBusy:      "Busy",
		ConnStatusClosing:   "Closing",
	}
)

func GetConnStatusDesc(status uint32) string {
	desc, exist := connStatusDescMap[status]
	if !exist {
		return fmt.Sprintf("Unknown status %v", status)
	}

	return desc
}
