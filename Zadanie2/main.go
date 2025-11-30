package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	openapi "Zadanie2/go"
	metastore "Zadanie2/metastore"
)

func main() {
	log.Printf("Server starting...")

	// 1) Metastore: load or create empty
	ms := metastore.NewMetastore("metastore.json")
	if err := ms.Load(); err != nil {
		log.Fatalf("failed to load metastore: %v", err)
	}
	if ms.Tables == nil {
		ms.Tables = map[string]*metastore.Table{}
	}

	Proj3Service := openapi.NewProj3APIService(ms)
	Proj3Controller := openapi.NewProj3APIController(Proj3Service)

	SchemaAPIService := openapi.NewSchemaAPIService()
	SchemaAPIController := openapi.NewSchemaAPIController(SchemaAPIService)

	router := openapi.NewRouter(Proj3Controller, SchemaAPIController)

	// 4) Serve OpenAPI spec and Swagger UI
	mux := http.NewServeMux()
	mux.Handle("/", router)

	// Serve your OpenAPI YAML/JSON (put a copy at project root as openapi.yaml)
	mux.HandleFunc("/openapi.yaml", func(w http.ResponseWriter, r *http.Request) {
		http.ServeFile(w, r, "dbmsInterface.yaml")
	})

	// Simple Swagger UI page using CDN
	mux.HandleFunc("/docs", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		fmt.Fprint(w, `
<!doctype html>
<html>
<head>
  <meta charset="utf-8"/>
  <title>API Docs</title>
  <link rel="stylesheet" href="https://unpkg.com/swagger-ui-dist@5/swagger-ui.css">
</head>
<body>
  <div id="swagger-ui"></div>
  <script src="https://unpkg.com/swagger-ui-dist@5/swagger-ui-bundle.js"></script>
  <script>
    window.ui = SwaggerUIBundle({ url: "/openapi.yaml", dom_id: "#swagger-ui" });
  </script>
</body>
</html>`)
	})

	srv := &http.Server{
		Addr:    ":8080",
		Handler: mux,
	}

	// 5) Graceful shutdown; persist metastore once on exit
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		log.Printf("HTTP on %s", srv.Addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("server error: %v", err)
		}
	}()

	<-stop
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := ms.Save(); err != nil {
		log.Printf("metastore save error: %v", err)
	}
	ms.PrintMetadata(os.Stdout)
	_ = srv.Shutdown(ctx)
	log.Println("shutdown complete")
}
