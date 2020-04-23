package main


import (
  "flag"
  "log"
  "os"
  "os/signal"
  "github.com/gorilla/websocket"
)

var uri = flag.String("uri", "ws://localhost:9090/data", "ws uri")

func makeInterrupt() chan(os.Signal) {
  interrupt := make(chan os.Signal, 1)
  signal.Notify(interrupt, os.Interrupt)
  return interrupt
}

func main() {
  flag.Parse()
  log.SetFlags(0)

  interrupt := makeInterrupt()

  log.Printf("connecting: to %s", *uri)

  c, _, err := websocket.DefaultDialer.Dial(*uri, nil)
  if err != nil {
    log.Fatal("dial:", err)
  }
  defer c.Close()

  done := make(chan struct{})
  // data := make(chan struct{})

  // Handle incoming chunks
  go func() {
    defer close(done)
    idx := 0
    total := 0
    for {
      idx++
      _, msg, err := c.ReadMessage()
      if err != nil {
        log.Println("read:", err)
        return
      }
      if len(msg) == 0 {
        return
      }
      total += len(msg)
      if idx % 1000== 0 {
        log.Println(total)
      }
    }
  }()

  // send request
  go func() {
    err := c.WriteMessage(websocket.TextMessage, []byte("hi"))
    if err != nil {
      log.Println("write:", err)
      return
    }
  }()

  for {
    select {
    case <-done:
      return
    case <-interrupt:
      log.Println("interrupt")
      err := c.WriteMessage(
        websocket.CloseMessage, 
        websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
      if err != nil {
        log.Println("close:", err)
      }
      select {
      case <-done:
      }
      return
    }
  }

}
