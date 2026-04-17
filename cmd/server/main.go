package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	// Using go-sqlite3 for local edge storage (requires CGO_ENABLED=1)
	_ "github.com/mattn/go-sqlite3"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// --- SRE GOLDEN SIGNALS (MVC EDITION) ---
var (
	// Traffic & Latency: Histograms are the heart of SRE observability
	httpDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "mvc_request_duration_seconds",
		Help:    "Latency of retail operations in seconds.",
		Buckets: []float64{0.01, 0.05, 0.1, 0.5, 1, 2, 5},
	}, []string{"path", "status"})

	// Errors: Tracking 4xx and 5xx responses
	errorCounter = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "mvc_errors_total",
		Help: "Total number of failed retail operations.",
	}, []string{"path", "type"})

	// Saturation: Monitoring the local storage backlog
	backlogGauge = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "mvc_local_backlog_count",
		Help: "Number of transactions stored locally in SQLite.",
	})

	logger = slog.New(slog.NewJSONHandler(os.Stdout, nil))
)

// --- MVC DATABASE HANDLER ---
func initMVCDB() *sql.DB {
	// Connect to local SQLite file
	db, err := sql.Open("sqlite3", "./retail_edge.db")
	if err != nil {
		logger.Error("Failed to open SQLite", "err", err)
		os.Exit(1)
	}

	// SRE Best Practice: Enable WAL mode for better concurrency and crash resilience at the edge
	_, err = db.Exec("PRAGMA journal_mode=WAL;")
	if err != nil {
		logger.Error("Failed to set WAL mode", "err", err)
	}

	// Create minimal schema for the MVC
	schema := `
	CREATE TABLE IF NOT EXISTS sales (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		ean TEXT,
		created_at DATETIME DEFAULT CURRENT_TIMESTAMP
	);`
	db.Exec(schema)
	return db
}

// --- MIDDLEWARE ---
func metricsMiddleware(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()

		// Wrap ResponseWriter to capture the HTTP status code
		interceptedWriter := &statusWriter{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(interceptedWriter, r)

		duration := time.Since(start).Seconds()
		status := fmt.Sprint(interceptedWriter.status)

		// Record Latency and Traffic
		httpDuration.WithLabelValues(r.URL.Path, status).Observe(duration)

		// Record Errors if status code is >= 400
		if interceptedWriter.status >= 400 {
			errorCounter.WithLabelValues(r.URL.Path, "http_error").Inc()
		}
	}
}

type statusWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusWriter) WriteHeader(code int) {
	w.status = code
	w.ResponseWriter.WriteHeader(code)
}

// --- HANDLERS ---

func handleScan(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Only POST allowed", http.StatusMethodNotAllowed)
			return
		}

		var req struct {
			EAN string `json:"ean"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "Bad request", http.StatusBadRequest)
			return
		}

		// Local Persistence: Writing to SQLite ensures offline capability
		_, err := db.Exec("INSERT INTO sales (ean) VALUES (?)", req.EAN)
		if err != nil {
			logger.Error("SQLite write failed", "err", err)
			errorCounter.WithLabelValues("/api/scan", "db_error").Inc()
			http.Error(w, "Storage failed", http.StatusInternalServerError)
			return
		}

		// Update Saturation Metric: Monitor current backlog size
		var count float64
		db.QueryRow("SELECT COUNT(*) FROM sales").Scan(&count)
		backlogGauge.Set(count)

		w.WriteHeader(http.StatusAccepted)
		w.Write([]byte(`{"status":"stored_locally"}`))
	}
}

func main() {
	db := initMVCDB()
	defer db.Close()

	mux := http.NewServeMux()

	// MVC API Core
	mux.HandleFunc("/api/scan", metricsMiddleware(handleScan(db)))

	// Observability Endpoints
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		if err := db.Ping(); err != nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.Write([]byte("MVC_HEALTHY"))
	})

	// Static frontend delivery (later handled by Caddy)
	mux.Handle("/", http.FileServer(http.Dir("./web")))

	server := &http.Server{
		Addr:    ":8080",
		Handler: mux,
	}

	// Graceful Shutdown: Essential SRE practice for data integrity
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	go func() {
		logger.Info("Retail Edge MVC Online", "port", "8080", "storage", "sqlite_wal")
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("Server crash", "err", err)
		}
	}()

	<-stop
	logger.Info("Shutting down MVC...")

	// Allow 5 seconds for active requests to finish
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := server.Shutdown(ctx); err != nil {
		logger.Error("Forced shutdown", "err", err)
	}
	logger.Info("MVC stopped cleanly.")
}
