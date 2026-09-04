// cmd/api runs a tiny HTTP service that creates orders. Each POST /orders
// writes the order row and its outbox event in a single DB transaction -
// see internal/store.CreateOrderWithOutbox for the interesting part.
package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"outbox-demo/internal/model"
	"outbox-demo/internal/store"
)

func main() {
	dsn := getEnv("DATABASE_URL", "postgres://outbox:outbox@localhost:5432/outbox_demo")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		log.Fatalf("connect to postgres: %v", err)
	}
	defer pool.Close()

	if err := pool.Ping(ctx); err != nil {
		log.Fatalf("ping postgres: %v", err)
	}

	s := store.New(pool)

	mux := http.NewServeMux()
	mux.HandleFunc("POST /orders", createOrderHandler(s))
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	addr := getEnv("API_ADDR", ":8081")
	log.Printf("api listening on %s", addr)
	log.Fatal(http.ListenAndServe(addr, mux))
}

type createOrderRequest struct {
	CustomerName string  `json:"customer_name"`
	Item         string  `json:"item"`
	Amount       float64 `json:"amount"`
}

func createOrderHandler(s *store.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req createOrderRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid request body", http.StatusBadRequest)
			return
		}
		if req.CustomerName == "" || req.Item == "" || req.Amount <= 0 {
			http.Error(w, "customer_name, item and amount are required", http.StatusBadRequest)
			return
		}

		order, err := s.CreateOrderWithOutbox(r.Context(), model.Order{
			CustomerName: req.CustomerName,
			Item:         req.Item,
			Amount:       req.Amount,
		})
		if err != nil {
			log.Printf("create order: %v", err)
			http.Error(w, "failed to create order", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		json.NewEncoder(w).Encode(order)
	}
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
