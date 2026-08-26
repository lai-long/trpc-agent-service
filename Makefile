# TestWeb IM one-click startup
ADDR    ?= :8080
BIN     := bin/testweb
WEB_DIR := cmd/testweb/web
PID     := data/testweb.pid
LOG     := data/testweb.log

.PHONY: testweb testweb-run testweb-stop testweb-build testweb-deps testweb-dev

# One-click startup: install deps -> build frontend -> build backend -> run in background
testweb: testweb-build
	@mkdir -p data
	@if [ -f $(PID) ] && kill -0 $$(cat $(PID)) 2>/dev/null; then \
		echo "already running: pid=$$(cat $(PID))"; \
	else \
		nohup $(BIN) -addr $(ADDR) >$(LOG) 2>&1 & \
		echo $$! > $(PID); \
		echo "testweb started: pid=$$(cat $(PID))  http://localhost$(ADDR)"; \
	fi

# Run in the foreground (for debugging)
testweb-run: testweb-build
	$(BIN) -addr $(ADDR)

testweb-build: testweb-deps
	cd $(WEB_DIR) && npm run build
	@mkdir -p bin
	go build -o $(BIN) ./cmd/testweb
	@echo "built: $(BIN)"

# Install dependencies only when node_modules is missing
testweb-deps:
	@if [ ! -d $(WEB_DIR)/node_modules ]; then \
		cd $(WEB_DIR) && npm install; \
	fi

# Frontend dev mode (hot reload, proxied to :8080)
testweb-dev: testweb-deps
	cd $(WEB_DIR) && npm run dev

testweb-stop:
	@if [ -f $(PID) ] && kill -0 $$(cat $(PID)) 2>/dev/null; then \
		kill $$(cat $(PID)) && echo "stopped: pid=$$(cat $(PID))"; \
	else \
		echo "not running"; \
	fi
	@rm -f $(PID)
