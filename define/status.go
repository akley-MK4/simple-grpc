package define

import "fmt"

const (
	GRPCConnStatusInvalid int = -1
)

const (
	NotOpenUsingStatus uintptr = iota
	WaitConnectUsingStatus
	DisconnectedUsingStatus
	IdledUsingStatus
	BusyUsingStatus
	ClosingUsingStatus
	StoppingUsingStatus
	StoppedUsingStatus
)

var (
	usingStatusDescMap = map[uintptr]string{
		NotOpenUsingStatus:      "NotOpen",
		WaitConnectUsingStatus:  "WaitConnect",
		DisconnectedUsingStatus: "Disconnected",
		IdledUsingStatus:        "Idled",
		BusyUsingStatus:         "Busy",
		ClosingUsingStatus:      "Closing",
		StoppingUsingStatus:     "Stopping",
		StoppedUsingStatus:      "Stopped",
	}
)

func GetUsingStatusDesc(status uintptr) string {
	desc, exist := usingStatusDescMap[status]
	if !exist {
		return fmt.Sprintf("Unknown status %v", status)
	}

	return desc
}

const (
	UninitializedPoolStatus uintptr = iota
	InitializedPoolStatus
	StartingPoolStatus
	StartedPoolStatus
	StoppingPoolStatus
	StoppedPoolStatus
)
