PKG := github.com/lightninglabs/neutrino
TOOLS_DIR := tools

BTCD_PKG := github.com/btcsuite/btcd
LINT_PKG := github.com/golangci/golangci-lint/cmd/golangci-lint
GOACC_PKG := github.com/ory/go-acc
GOIMPORTS_PKG := github.com/rinchsan/gosimports/cmd/gosimports

GO_BIN := ${GOPATH}/bin
LINT_BIN := $(GO_BIN)/golangci-lint
GOACC_BIN := $(GO_BIN)/go-acc

GOBUILD := go build -v
GOINSTALL := go install -v
GOTEST := go test 

GOLIST := go list -deps $(PKG)/... | grep '$(PKG)'
GOLIST_COVER := $$(go list -deps $(PKG)/... | grep '$(PKG)')
GOFILES_NOVENDOR = $(shell find . -type f -name '*.go' -not -path "./vendor/*")

RM := rm -f
CP := cp
MAKE := make
XARGS := xargs -L 1
DOCKER_TOOLS = docker run -v $$(pwd):/build neutrino-tools

# Linting uses a lot of memory, so keep it under control by limiting the number
# of workers if requested.
ifneq ($(workers),)
LINT_WORKERS = --concurrency=$(workers)
endif

GREEN := "\\033[0;32m"
NC := "\\033[0m"
define print
	echo $(GREEN)$1$(NC)
endef

default: build

all: build check

# ============
# DEPENDENCIES
# ============

btcd:
	@$(call print, "Installing btcd.")
	cd $(TOOLS_DIR); go install -trimpath -tags=tools $(BTCD_PKG)

$(GOACC_BIN):
	@$(call print, "Fetching go-acc")
	cd $(TOOLS_DIR); go install -trimpath -tags=tools $(GOACC_PKG)

goimports:
	@$(call print, "Installing goimports.")
	cd $(TOOLS_DIR); go install -trimpath -tags=tools $(GOIMPORTS_PKG)

# ============
# INSTALLATION
# ============

build:
	@$(call print, "Compiling neutrino.")
	$(GOBUILD) $(PKG)/...

# =======
# TESTING
# =======

check: unit

unit: btcd
	@$(call print, "Running unit tests.")
	$(GOLIST) | $(XARGS) env $(GOTEST)

unit-cover: btcd $(GOACC_BIN)
	@$(call print, "Running unit coverage tests.")
	$(GOACC_BIN) $(GOLIST_COVER)

unit-race: btcd
	@$(call print, "Running unit race tests.")
	env CGO_ENABLED=1 GORACE="history_size=7 halt_on_errors=1" $(GOLIST) | $(XARGS) env $(GOTEST) -race

# =========
# UTILITIES
# =========

docker-tools:
	@$(call print, "Building tools docker image.")
	docker build -q -t neutrino-tools $(TOOLS_DIR)

fmt: goimports
	@$(call print, "Fixing imports.")
	gosimports -w $(GOFILES_NOVENDOR)
	@$(call print, "Formatting source.")
	gofmt -l -w -s $(GOFILES_NOVENDOR)

lint: docker-tools
	@$(call print, "Linting source.")
	$(DOCKER_TOOLS) golangci-lint run -v $(LINT_WORKERS)

clean:
	@$(call print, "Cleaning source.$(NC)")
	$(RM) coverage.txt

.PHONY: all \
	btcd \
	default \
	build \
	check \
	unit \
	unit-cover \
	unit-race \
	fmt \
	lint \
	clean
