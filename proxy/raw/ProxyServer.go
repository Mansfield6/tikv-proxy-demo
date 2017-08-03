package raw

import (
	"flag"
	"fmt"
	redis "github.com/dotcloud/go-redis-server"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb/store/tikv"
	"strings"
)

var (
	serverPort = flag.Int("port", 9999, "server port")
	pdAddr     = flag.String("pd", "localhost:2379", "pd address:localhost:2379")
)

type TikvHandler struct {
	client tikv.RawKVClient
}

// Get override the DefaultHandler's method.
func (h *TikvHandler) GET(key string) ([]byte, error) {
	fmt.Println("get key:", key)
	return h.client.Get([]byte(key))
}

func (h *TikvHandler) SET(key string, val []byte) error {
	fmt.Println("put key:", key)
	return h.client.Put([]byte(key), val)
}

// Test2 implement a new command. Non-redis standard, but it is possible.
// This function needs to be registered.
func Test2() ([]byte, error) {
	return []byte("Awesome custom redis command via function!"), nil
}

func main() {
	flag.Parse()
	fmt.Println("serverPort:", *serverPort)
	fmt.Println("pdAddr:", *pdAddr)

	defer func() {
		if msg := recover(); msg != nil {
			fmt.Printf("Panic: %v\n", msg)
		}
	}()

	cli, err := tikv.NewRawKVClient(strings.Split(*pdAddr, ","))
	if err != nil {
		log.Fatal(err)
	}
	myhandler := &TikvHandler{*cli}
	srv, err := redis.NewServer(redis.DefaultConfig().Port(*serverPort).Handler(myhandler))
	if err != nil {
		panic(err)
	}
	if err := srv.RegisterFct("test2", Test2); err != nil {
		panic(err)
	}
	if err := srv.ListenAndServe(); err != nil {
		panic(err)
	}
}
