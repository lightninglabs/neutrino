//go:build rpctest

package webtransport_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/btcsuite/btcd/chaincfg/v2"
	"github.com/btcsuite/btcd/integration/rpctest"
	"github.com/btcsuite/btcd/wire/v2"
	"github.com/stretchr/testify/require"
)

const (
	webTransportPath = "/v1/btc-p2p"
	wasmPeerAddress  = "127.0.0.1:18555"

	initialBlockCount = 12
	additionalBlocks  = 3
)

const wasmUserAgent = wire.DefaultUserAgent +
	"neutrino-wasm-itest:0.0.1/"

type syncTarget struct {
	Height           int32  `json:"height"`
	BlockHash        string `json:"block_hash"`
	FilterHeaderHash string `json:"filter_header_hash"`
	FilterData       string `json:"filter_data"`
}

type browserSyncResult struct {
	Status string `json:"status"`
	Error  string `json:"error,omitempty"`
	Phase  string `json:"phase,omitempty"`

	PeerCount          int    `json:"peer_count,omitempty"`
	BlockHeaderHeight  uint32 `json:"block_header_height,omitempty"`
	BlockHeaderHash    string `json:"block_header_hash,omitempty"`
	FilterHeaderHeight uint32 `json:"filter_header_height,omitempty"`
	FilterHeaderHash   string `json:"filter_header_hash,omitempty"`
	FilterData         string `json:"filter_data,omitempty"`
	BestBlockHeight    int32  `json:"best_block_height,omitempty"`
	BestBlockHash      string `json:"best_block_hash,omitempty"`
}

type targetState struct {
	mu     sync.RWMutex
	target syncTarget
}

func (s *targetState) set(target syncTarget) {
	s.mu.Lock()
	s.target = target
	s.mu.Unlock()
}

func (s *targetState) get() syncTarget {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.target
}

type chromeProcess struct {
	cancel  context.CancelFunc
	done    chan struct{}
	logPath string
	errMu   sync.Mutex
	err     error
	once    sync.Once
}

// TestWebTransportBrowserChainServiceSync proves that a complete Neutrino
// ChainService running as Go WASM in Chrome can synchronize block headers and
// regular filter headers from a full btcd node over WebTransport. The test is
// deliberately opt-in because it requires a btcd binary containing the paired
// WebTransport server change and a locally installed Chrome or Chromium.
func TestWebTransportBrowserChainServiceSync(t *testing.T) {
	btcdPath := requireBTCDWebTransportBinary(t)
	chromePath := findChrome(t)
	if chromePath == "" {
		t.Skip("Chrome or Chromium not found; set BTCD_CHROME_BIN to run " +
			"the WebTransport browser integration test")
	}

	repoRoot, fixtureDir := fixturePaths(t)
	testDir := t.TempDir()
	wasmPath := filepath.Join(testDir, "client.wasm")
	buildWASMFixture(t, repoRoot, wasmPath)
	wasmExec := readWASMExec(t)
	indexHTML, err := os.ReadFile(filepath.Join(fixtureDir, "index.html"))
	require.NoError(t, err)
	wasmBinary, err := os.ReadFile(wasmPath)
	require.NoError(t, err)

	browserListener, err := net.Listen("tcp4", "127.0.0.1:0")
	require.NoError(t, err)
	browserOrigin := "http://" + browserListener.Addr().String()

	webTransportAddress := availableUDPAddress(t)
	certificatePath := filepath.Join(testDir, "webtransport.cert")
	keyPath := filepath.Join(testDir, "webtransport.key")
	certificateHash := writeWebTransportCertificate(
		t, certificatePath, keyPath,
	)

	harness, err := rpctest.New(
		&chaincfg.SimNetParams, nil, []string{
			"--webtransportlisten=" + webTransportAddress,
			"--webtransportcert=" + certificatePath,
			"--webtransportkey=" + keyPath,
			"--webtransportpath=" + webTransportPath,
			"--webtransportorigin=" + browserOrigin,
		}, btcdPath,
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		if err := harness.TearDown(); err != nil {
			t.Errorf("tear down btcd harness: %v", err)
		}
	})
	require.NoError(t, harness.SetUp(false, 0))

	_, err = harness.Client.Generate(initialBlockCount)
	require.NoError(t, err)
	initialTarget := currentTarget(t, harness)
	targets := &targetState{target: initialTarget}

	results := make(chan browserSyncResult, 4)
	browserServer := newBrowserServer(
		indexHTML, wasmExec, wasmBinary, targets, results,
	)
	serveError := make(chan error, 1)
	go func() {
		serveError <- browserServer.Serve(browserListener)
	}()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(
			context.Background(), 5*time.Second,
		)
		defer cancel()
		if err := browserServer.Shutdown(ctx); err != nil {
			t.Errorf("shut down browser asset server: %v", err)
		}
		select {
		case err := <-serveError:
			if err != nil && err != http.ErrServerClosed {
				t.Errorf("browser asset server: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Error("browser asset server did not stop")
		}
	})

	endpoint := "https://" + webTransportAddress + webTransportPath
	query := make(url.Values)
	query.Set("endpoint", endpoint)
	query.Set("cert_hash", fmt.Sprintf("%x", certificateHash[:]))
	query.Set("peer_address", wasmPeerAddress)
	pageURL := browserOrigin + "/?" + query.Encode()

	version, err := exec.Command(chromePath, "--version").CombinedOutput()
	if err != nil {
		t.Logf("unable to read Chrome version: %v", err)
	} else {
		t.Logf("browser: %s", strings.TrimSpace(string(version)))
	}

	chrome := startChrome(t, chromePath, pageURL, testDir)
	t.Cleanup(chrome.stop)

	initialResult := waitForBrowserResult(
		t, chrome, results, "initial", 90*time.Second,
	)
	assertSyncResult(t, initialResult, initialTarget)
	assertWebTransportRPCPeer(t, harness, webTransportAddress)

	_, err = harness.Client.Generate(additionalBlocks)
	require.NoError(t, err)
	advancedTarget := currentTarget(t, harness)
	require.Greater(t, advancedTarget.Height, initialTarget.Height)
	targets.set(advancedTarget)

	advancedResult := waitForBrowserResult(
		t, chrome, results, "advanced", 60*time.Second,
	)
	assertSyncResult(t, advancedResult, advancedTarget)
	assertWebTransportRPCPeer(t, harness, webTransportAddress)

	t.Logf("browser ChainService synchronized block and regular filter "+
		"headers and fetched matching compact filters over WebTransport "+
		"from height %d to %d",
		initialTarget.Height, advancedTarget.Height)
}

func currentTarget(t *testing.T, harness *rpctest.Harness) syncTarget {
	t.Helper()

	bestHash, bestHeight, err := harness.Client.GetBestBlock()
	require.NoError(t, err)
	filterHeader, err := harness.Client.GetCFilterHeader(
		bestHash, wire.GCSFilterRegular,
	)
	require.NoError(t, err)
	filter, err := harness.Client.GetCFilter(
		bestHash, wire.GCSFilterRegular,
	)
	require.NoError(t, err)

	return syncTarget{
		Height:           bestHeight,
		BlockHash:        bestHash.String(),
		FilterHeaderHash: filterHeader.PrevFilterHeader.String(),
		FilterData:       hex.EncodeToString(filter.Data),
	}
}

func waitForBrowserResult(t *testing.T, chrome *chromeProcess,
	results <-chan browserSyncResult, phase string,
	timeout time.Duration) browserSyncResult {

	t.Helper()
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case result := <-results:
		if result.Status != "ok" {
			chrome.stop()
			t.Fatalf("browser ChainService failed during %s: %s\n%s",
				result.Phase, result.Error, chrome.logOutput())
		}
		require.Equal(t, phase, result.Phase)
		return result

	case <-chrome.done:
		t.Fatalf("Chrome exited before reporting %s sync: %v\n%s",
			phase, chrome.exitError(), chrome.logOutput())

	case <-timer.C:
		chrome.stop()
		t.Fatalf("timed out waiting for browser %s sync\n%s",
			phase, chrome.logOutput())
	}

	return browserSyncResult{}
}

func assertSyncResult(t *testing.T, result browserSyncResult,
	target syncTarget) {

	t.Helper()
	require.Positive(t, result.PeerCount)
	require.EqualValues(t, target.Height, result.BlockHeaderHeight)
	require.EqualValues(t, target.Height, result.FilterHeaderHeight)
	require.Equal(t, target.Height, result.BestBlockHeight)
	require.Equal(t, target.BlockHash, result.BlockHeaderHash)
	require.Equal(t, target.BlockHash, result.BestBlockHash)
	require.Equal(t, target.FilterHeaderHash, result.FilterHeaderHash)
	require.Equal(t, target.FilterData, result.FilterData)
}

func assertWebTransportRPCPeer(t *testing.T, harness *rpctest.Harness,
	webTransportAddress string) {

	t.Helper()
	require.Eventually(t, func() bool {
		peers, err := harness.Client.GetPeerInfo()
		if err != nil {
			return false
		}
		for _, peer := range peers {
			if peer.SubVer != wasmUserAgent {
				continue
			}

			return peer.Inbound && peer.AddrLocal == webTransportAddress &&
				peer.BytesRecv > 0 && peer.BytesSent > 0
		}

		return false
	}, 10*time.Second, 50*time.Millisecond)
}

func requireBTCDWebTransportBinary(t *testing.T) string {
	t.Helper()

	path := os.Getenv("BTCD_WEBTRANSPORT_BIN")
	if path == "" {
		t.Skip("BTCD_WEBTRANSPORT_BIN is not set; point it at a btcd " +
			"binary containing the WebTransport server change")
	}
	path, err := filepath.Abs(path)
	require.NoError(t, err)
	info, err := os.Stat(path)
	require.NoErrorf(t, err, "stat BTCD_WEBTRANSPORT_BIN %q", path)
	require.Falsef(t, info.IsDir(), "BTCD_WEBTRANSPORT_BIN %q is a directory", path)

	return path
}

func fixturePaths(t *testing.T) (string, string) {
	t.Helper()

	_, file, _, ok := runtime.Caller(0)
	require.True(t, ok)
	itestDir := filepath.Dir(file)
	repoRoot := filepath.Dir(filepath.Dir(itestDir))

	return repoRoot, filepath.Join(itestDir, "client")
}

func buildWASMFixture(t *testing.T, repoRoot, outputPath string) {
	t.Helper()

	cmd := exec.Command(
		"go", "build", "-trimpath", "-o", outputPath,
		"./itest/webtransport/client",
	)
	cmd.Dir = repoRoot
	cmd.Env = append(
		os.Environ(), "GOOS=js", "GOARCH=wasm", "CGO_ENABLED=0",
	)
	output, err := cmd.CombinedOutput()
	require.NoErrorf(t, err, "build Go WASM fixture:\n%s", output)
}

func readWASMExec(t *testing.T) []byte {
	t.Helper()

	paths := []string{
		filepath.Join(runtime.GOROOT(), "lib", "wasm", "wasm_exec.js"),
		filepath.Join(runtime.GOROOT(), "misc", "wasm", "wasm_exec.js"),
	}
	for _, path := range paths {
		contents, err := os.ReadFile(path)
		if err == nil {
			return contents
		}
	}

	t.Fatalf("wasm_exec.js not found below %s", runtime.GOROOT())
	return nil
}

func newBrowserServer(indexHTML, wasmExec, wasmBinary []byte,
	targets *targetState,
	results chan<- browserSyncResult) *http.Server {

	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/" {
			http.NotFound(w, request)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write(indexHTML)
	})
	mux.HandleFunc("/wasm_exec.js", func(w http.ResponseWriter,
		_ *http.Request) {

		w.Header().Set("Content-Type", "text/javascript; charset=utf-8")
		_, _ = w.Write(wasmExec)
	})
	mux.HandleFunc("/client.wasm", func(w http.ResponseWriter,
		_ *http.Request) {

		w.Header().Set("Content-Type", "application/wasm")
		_, _ = w.Write(wasmBinary)
	})
	mux.HandleFunc("/target", func(w http.ResponseWriter,
		request *http.Request) {

		if request.Method != http.MethodGet {
			w.Header().Set("Allow", http.MethodGet)
			http.Error(w, "GET required", http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(targets.get()); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	})
	mux.HandleFunc("/result", func(w http.ResponseWriter,
		request *http.Request) {

		if request.Method != http.MethodPost {
			w.Header().Set("Allow", http.MethodPost)
			http.Error(w, "POST required", http.StatusMethodNotAllowed)
			return
		}
		defer request.Body.Close()
		decoder := json.NewDecoder(io.LimitReader(request.Body, 1<<20))
		var result browserSyncResult
		if err := decoder.Decode(&result); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		select {
		case results <- result:
			w.WriteHeader(http.StatusNoContent)
		default:
			http.Error(w, "result queue is full", http.StatusConflict)
		}
	})

	return &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
}

func availableUDPAddress(t *testing.T) string {
	t.Helper()

	packetConn, err := net.ListenPacket("udp4", "127.0.0.1:0")
	require.NoError(t, err)
	address := packetConn.LocalAddr().String()
	require.NoError(t, packetConn.Close())

	return address
}

func writeWebTransportCertificate(t *testing.T, certPath,
	keyPath string) [sha256.Size]byte {

	t.Helper()
	privateKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	now := time.Now()
	template := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "btcd WebTransport test"},
		NotBefore:             now.Add(-time.Minute),
		NotAfter:              now.Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IPAddresses:           []net.IP{net.IPv4(127, 0, 0, 1)},
	}
	der, err := x509.CreateCertificate(
		rand.Reader, template, template, &privateKey.PublicKey, privateKey,
	)
	require.NoError(t, err)
	keyDER, err := x509.MarshalECPrivateKey(privateKey)
	require.NoError(t, err)

	certPEM := pem.EncodeToMemory(&pem.Block{
		Type: "CERTIFICATE", Bytes: der,
	})
	keyPEM := pem.EncodeToMemory(&pem.Block{
		Type: "EC PRIVATE KEY", Bytes: keyDER,
	})
	require.NoError(t, os.WriteFile(certPath, certPEM, 0600))
	require.NoError(t, os.WriteFile(keyPath, keyPEM, 0600))

	return sha256.Sum256(der)
}

func findChrome(t *testing.T) string {
	t.Helper()

	if configured := os.Getenv("BTCD_CHROME_BIN"); configured != "" {
		info, err := os.Stat(configured)
		require.NoErrorf(t, err, "stat BTCD_CHROME_BIN %q", configured)
		require.Falsef(t, info.IsDir(), "BTCD_CHROME_BIN %q is a directory", configured)
		return configured
	}

	for _, name := range []string{
		"google-chrome", "google-chrome-stable", "chromium",
		"chromium-browser", "chrome",
	} {
		if path, err := exec.LookPath(name); err == nil {
			return path
		}
	}
	for _, path := range []string{
		"/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
		"/Applications/Chromium.app/Contents/MacOS/Chromium",
		`C:\Program Files\Google\Chrome\Application\chrome.exe`,
		`C:\Program Files (x86)\Google\Chrome\Application\chrome.exe`,
	} {
		if info, err := os.Stat(path); err == nil && !info.IsDir() {
			return path
		}
	}

	return ""
}

func startChrome(t *testing.T, chromePath, pageURL,
	testDir string) *chromeProcess {

	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	args := []string{
		"--headless=new",
		"--disable-background-networking",
		"--disable-component-update",
		"--disable-default-apps",
		"--disable-dev-shm-usage",
		"--disable-gpu",
		"--disable-sync",
		"--metrics-recording-only",
		"--no-default-browser-check",
		"--no-first-run",
		"--user-data-dir=" + filepath.Join(testDir, "chrome-profile"),
	}
	if runtime.GOOS == "linux" {
		args = append(args, "--no-sandbox")
	}
	args = append(args, pageURL)

	logPath := filepath.Join(testDir, "chrome.log")
	logFile, err := os.Create(logPath)
	require.NoError(t, err)
	cmd := exec.CommandContext(ctx, chromePath, args...)
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	require.NoError(t, cmd.Start())

	process := &chromeProcess{
		cancel:  cancel,
		done:    make(chan struct{}),
		logPath: logPath,
	}
	go func() {
		err := cmd.Wait()
		_ = logFile.Close()
		process.errMu.Lock()
		process.err = err
		process.errMu.Unlock()
		close(process.done)
	}()

	return process
}

func (p *chromeProcess) stop() {
	if p == nil {
		return
	}
	p.once.Do(func() {
		p.cancel()
		select {
		case <-p.done:
		case <-time.After(10 * time.Second):
		}
	})
}

func (p *chromeProcess) logOutput() string {
	contents, err := os.ReadFile(p.logPath)
	if err != nil {
		return fmt.Sprintf("read Chrome log: %v", err)
	}
	const maxLogBytes = 16 << 10
	if len(contents) > maxLogBytes {
		contents = contents[len(contents)-maxLogBytes:]
	}

	return string(contents)
}

func (p *chromeProcess) exitError() error {
	p.errMu.Lock()
	defer p.errMu.Unlock()

	return p.err
}
