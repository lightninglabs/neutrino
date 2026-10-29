// types.p - Shared type definitions for the neutrino rescan FSM P model.
//
// All blockchain primitives are abstracted to integers. Addresses are int IDs,
// block hashes are int IDs, transactions carry a set of AddrIDs they pay to.
// This preserves the semantic relationships needed for watch list matching
// while avoiding any cryptographic detail.

// =============================================================================
// Abstract Blockchain Identifiers
// =============================================================================

// Abstract address identifier (replaces btcutil.Address + pkScript).
type AddrID = int;

// Abstract transaction identifier.
type TxID = int;

// Abstract block hash (replaces chainhash.Hash).
type BlockHash = int;

// Block height (replaces int32/uint32 in Go).
type Height = int;

// =============================================================================
// Blockchain Data Structures
// =============================================================================

// Abstract block header. The prev_block field forms the chain link used for
// reorg detection (mirrors wire.BlockHeader.PrevBlock).
type BlockHeader = (
    hash: BlockHash,
    prev_block: BlockHash,
    height: Height
);

// Abstract transaction. The pays_to set captures which addresses this tx sends
// funds to. In the real code, this is determined by matching pkScripts against
// the watch list via GCS filters then full block inspection.
type AbstractTx = (
    txid: TxID,
    pays_to: set[AddrID]
);

// Abstract block content. Header plus the transactions it contains.
type BlockContent = (
    header: BlockHeader,
    txs: seq[AbstractTx]
);

// =============================================================================
// FSM State Enum
// =============================================================================

// Mirrors the 5 Go states: StateInitializing, StateSyncing, StateCurrent,
// StateRewinding, StateTerminal.
enum RescanFSMState {
    FSMInitializing,
    FSMSyncing,
    FSMCurrent,
    FSMRewinding,
    FSMTerminal
}

// =============================================================================
// Watch State
// =============================================================================

// Abstracts Go's WatchState (Addrs + Inputs + List) to a simple set of
// address IDs. In the real code, WatchState carries btcutil.Address slices,
// InputWithScript slices, and serialized pkScript byte slices. We collapse
// all of these into a single set since the semantic operation is set membership.
type WatchState = set[AddrID];

// =============================================================================
// Init Config Types
// =============================================================================

// RescanFSMInit configures a new RescanFSM machine.
type RescanFSMInit = (
    chain: machine,
    observer: machine,
    start_height: Height,
    start_hash: BlockHash,
    initial_watch: WatchState,
    batch_size: int
);

// RescanActorInit configures a new RescanActor machine.
type RescanActorInit = (
    fsm: machine,
    chain: machine,
    observer: machine
);

// MockChainInit configures a new MockChain machine.
type MockChainInit = (
    observer: machine,
    blocks: seq[BlockContent],
    tip_height: Height
);
