package api

import (
	"encoding/json"
	"log"
	"net/http"
	"simple-kafka-app/cache"
	"simple-kafka-app/database"
	"github.com/gorilla/mux"
)

type Server struct {
	cache *cache.OrderCache
	db    *database.DB
	router *mux.Router
}

func NewServer(cache *cache.OrderCache, db *database.DB) *Server {
	s := &Server{
		cache: cache,
		db:    db,
		router: mux.NewRouter(),
	}
	
	s.routes()
	return s
}

func (s *Server) routes() {
	s.router.HandleFunc("/orders", s.getOrders).Methods("GET")
	s.router.HandleFunc("/orders/{id}", s.getOrderByID).Methods("GET")
	
	s.router.PathPrefix("/").Handler(http.FileServer(http.Dir("./static/")))
}

func (s *Server) getOrders(w http.ResponseWriter, r *http.Request) {
	orders := s.cache.GetAll()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"count": len(orders),
		"orders": orders,
	})
	log.Printf("Returned %d orders", len(orders))
}

func (s *Server) getOrderByID(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	orderID := vars["id"]

	order, exists := s.cache.Get(orderID)
	if exists {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(order)
		log.Printf("Returned order %s from cache", orderID)
		return
	}

	order, err := s.db.GetOrderByID(orderID)
	if err != nil {
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(map[string]string{"error": "Order not found"})
		log.Printf("Order %s not found", orderID)
		return
	}
	s.cache.Add(order)
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(order)
	log.Printf("Returned order %s from database", orderID)
}

func (s *Server) Start(addr string) error {
	log.Printf("Starting API server on %s", addr)
	return http.ListenAndServe(addr, s.router)
}