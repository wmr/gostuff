package main

import (
	"flag"
	"log"
	"net/http"

	"github.com/fasthttp/websocket"
)

var addr = flag.String("addr", "localhost:9090", "http service address")

var upgrader = websocket.Upgrader{} // use default options

var dataBytes = make([]byte, 128 * 1024)

func data(w http.ResponseWriter, r *http.Request) {
	c, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Print("upgrade:", err)
		return
	}
	defer c.Close()
	for {
		mt, message, err := c.ReadMessage()
		if err != nil {
			log.Println("read:", err)
			break
		}
        log.Printf("recv: %s", message)
        log.Printf("msg type: %d", mt)
        for idx := 0; idx < 160000; idx++ {
            err = c.WriteMessage(2, dataBytes)
            if err != nil {
                log.Println("write:", err)
                break
            }
        }
	}
}

func main() {
	flag.Parse()
	log.SetFlags(0)
	http.HandleFunc("/data", data)
	log.Fatal(http.ListenAndServe(*addr, nil))
}

