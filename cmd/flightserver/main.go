package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/theRTima/lab14_energy/pkg/flightserver"
)

func main() {
	addr := flag.String("addr", ":8815", "Flight server address")
	flag.Parse()

	_, err := flightserver.StartFlightServer(*addr)
	if err != nil {
		log.Fatalf("Failed to start Flight server: %v", err)
	}

	log.Printf("Flight server running on %s", *addr)

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	<-sigChan
	log.Println("Flight server shutting down")
}
