package bridge

// EventType enumerates the P model event types for the rescan FSM. These
// correspond to the event declarations in rescan_fsm.p and actor.p.
type EventType string

const (
	// FSM input events.
	EvProcessNextBlock  EventType = "eProcessNextBlockEvent"
	EvBlockConnected    EventType = "eBlockConnectedEvent"
	EvBlockDisconnected EventType = "eBlockDisconnectedEvent"
	EvAddWatchAddrs     EventType = "eAddWatchAddrsEvent"
	EvStop              EventType = "eStopEvent"

	// Outbox events (announced by FSM for monitors).
	EvFilteredBlockOutbox    EventType = "eFilteredBlockOutbox"
	EvBlockConnectedOutbox   EventType = "eBlockConnectedOutbox"
	EvBlockDisconnectedOutbox EventType = "eBlockDisconnectedOutbox"
	EvRescanProgressOutbox   EventType = "eRescanProgressOutbox"
	EvRescanFinishedOutbox   EventType = "eRescanFinishedOutbox"
	EvStartSubscriptionOutbox EventType = "eStartSubscriptionOutbox"
	EvCancelSubscriptionOutbox EventType = "eCancelSubscriptionOutbox"
	EvSelfTellOutbox         EventType = "eSelfTellOutbox"
	EvFSMStateChange         EventType = "eFSMStateChange"
	EvGroundTruth            EventType = "eGroundTruthBlockProcessed"

	// Actor events.
	EvActorStart         EventType = "eActorStart"
	EvActorAddWatchAddrs EventType = "eActorAddWatchAddrs"
	EvActorStop          EventType = "eActorStop"
	EvActorSelfTell      EventType = "eActorSelfTell"
)

// FSMState maps the P model's RescanFSMState enum values to Go strings.
type FSMState int

const (
	StateInitializing FSMState = 0
	StateSyncing      FSMState = 1
	StateCurrent      FSMState = 2
	StateRewinding    FSMState = 3
	StateTerminal     FSMState = 4
)

// String returns the human-readable state name.
func (s FSMState) String() string {
	switch s {
	case StateInitializing:
		return "Initializing"
	case StateSyncing:
		return "Syncing"
	case StateCurrent:
		return "Current"
	case StateRewinding:
		return "Rewinding"
	case StateTerminal:
		return "Terminal"
	default:
		return "Unknown"
	}
}

// FilteredBlockPayload is the Go representation of the eFilteredBlockOutbox
// payload from the P model: (Height, seq[AbstractTx]).
type FilteredBlockPayload struct {
	Height int `json:"0"`
	Txs    []AbstractTxPayload `json:"1"`
}

// AbstractTxPayload is the Go representation of an AbstractTx from the P
// model: (txid, pays_to: set[AddrID]).
type AbstractTxPayload struct {
	TxID   int   `json:"txid"`
	PaysTo []int `json:"pays_to"`
}

// StateChangePayload is the Go representation of the eFSMStateChange payload:
// (from_state, to_state).
type StateChangePayload struct {
	From FSMState `json:"0"`
	To   FSMState `json:"1"`
}

// AddWatchPayload is the Go representation of the eAddWatchAddrsEvent payload:
// (set[AddrID], Height).
type AddWatchPayload struct {
	Addrs    []int `json:"0"`
	RewindTo int   `json:"1"`
}
