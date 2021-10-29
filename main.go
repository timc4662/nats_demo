package main

import (
	"context"
	"flag"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
)

const NatsDemoServer string = "nats://demo.nats.io:4222"

func main() {
	log.Println("connecting to the nats server")
	modePtr := flag.String("mode", "sync", "sync|async|queue")
	helpPtr := flag.Bool("help", false, "prints this usage")

	flag.Parse()
	if *helpPtr {
		flag.Usage()
		return
	}

	// To connect to a cluster add the seed servers here. NATS will randomly connect
	// to one of these and my also discover further servers known by that server automatically.
	// This should be a reasonable list of 'seed servers'.
	servers := []string{NatsDemoServer}

	opts := nats.GetDefaultOptions()
	opts.Pedantic = true // useful for testing, but you need to change connect
	//to opts.Connect (there shiould be a nats.Pedantic() function really)

	// The optional nats name is recommended as useful for monitoring.
	nc, err := nats.Connect(
		strings.Join(servers, ","),
		nats.Name("timc4662"),
		nats.Timeout(5*time.Second),       // timeout applies to each cluster member individually
		nats.PingInterval(20*time.Second), //
		nats.MaxPingsOutstanding(5),       // limits the period of inactivity to 100s (5*20) before reconnect (default 240  - see nats.go))
		// nats.NoEcho(),                     // same as DDS ignoreSelf
		nats.MaxReconnects(5), // sets max number of reconnects
		//nats.NoReconnect(),              // disable reconnects
		//nats.DontRandomize(),  		   // thundering herd (disable)
		nats.ReconnectWait(time.Second*3), // reconnect timeout
		nats.ReconnectBufSize(1000),       // buffers the writes with 1000 bytes (sent after connect)
		nats.ReconnectHandler(func(nc *nats.Conn) {
			log.Println("nats reconnected")
		}),
		nats.ClosedHandler(func(nc *nats.Conn) {
			log.Println("nats closed: " + nc.Opts.Url)
		}),
		nats.ErrorHandler(func(nc *nats.Conn, sub *nats.Subscription, err error) {
			log.Println(" -- error detected in nats error handler -- ")
			log.Println(err)
		}),
	)

	if err != nil {
		log.Fatal(err)
	}
	defer nc.Close()

	// To pass json structured messages, you could use this or even create a new encoder yourselves.
	// Then when we subscribe we don't get a nats.Msg we get a decoded object. Although do we lose the metadata?
	//	nats.NewEncodedConn(nc, nats.JSON_ENCODER)

	// not configurable, but max payload could be used to limit the size
	// of the messages we pass through perhaps.
	mp := nc.MaxPayload()
	log.Printf("Max payload: %v bytes", mp)

	switch *modePtr {
	case "async":
		asyncExample(nc)
	case "sync":
		syncExample(nc)
	case "queue":
		queuedExample(nc)
	default:
		log.Fatal("invalid -mode arg, run -help for options")
	}

	// Drain a connection, which will close it when done
	// Drain allows time for messages in flight to be processed.
	// this will call close when done, so the defer nc.Close is not really needed.
	// Note there is drain on the server and on a subscriber.
	log.Println("draining the connection")
	if err = nc.Drain(); err != nil {
		log.Fatal(err)
	}

}

func queuedExample(nc *nats.Conn) {

	// createa a wait group whoich is released when data arrives
	wg := sync.WaitGroup{}
	wg.Add(1)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*2)
	defer cancel()
	go func() {
		<-ctx.Done()
		log.Println("timeout...")
		wg.Done()
	}()

	log.Println("subscribing for 'updates.*' asychronously")
	// the * is a wildcard. Also there is a > character which can be used.

	/// The only difference to subscribe to a queue topic is the inclusion of the queue
	// name and the use of QueueSubscribe. Otherwise this is dynamic by the server. Queues
	// will deliver message to one of many subscribers. Great for workers and scaling up.
	sub, err := nc.QueueSubscribe("updated", "queue_name", func(m *nats.Msg) {
		log.Printf("data: %s subject: %s", m.Data, m.Subject)
		wg.Done()
	})
	defer func() {
		log.Println("unsubscribing...")
		if err := sub.Unsubscribe(); err != nil {
			log.Fatal(err)
		}
	}()

	if err != nil {
		log.Fatal(err)
	}

	log.Println("waiting...")
	wg.Wait()
}

func asyncExample(nc *nats.Conn) {

	// For this example we send structured json data.
	ec, err := nats.NewEncodedConn(nc, nats.JSON_ENCODER)
	if err != nil {
		log.Fatal(err)
	}

	// createa a wait group whoich is released when data arrives
	wg := sync.WaitGroup{}
	wg.Add(1)

	// this is a bit off topic, but experiment with using a context
	// to provide a timeout on the subscribe.
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*2)
	defer cancel()
	go func() {
		<-ctx.Done()
		log.Println("timeout...")
		wg.Done()
	}()

	// Define the struct we will send
	type person struct {
		Name string
		Age  int
	}

	// Subscribe for updates
	// the * is a wildcard. Also there is a > character which can be used.
	// note that * would not work with this example as there are three parts to the
	// sending subject
	log.Println("Subscribing for 'people'.")
	sub, err := ec.Subscribe("people.>", func(p *person) {
		log.Printf("name: %s age: %d", p.Name, p.Age)
		wg.Done()
	})

	if err != nil {
		log.Fatal(err)
	}

	me := person{
		Name: "tim",
		Age:  41,
	}

	// Publish me
	if err = ec.Publish("people.uk.west_sussex", &me); err != nil {
		log.Fatal(err)
	}

	// the flush causes a roundtrip (ping/pong) to the server, so guaratees
	// that the server will have processed the message.
	ec.Flush()

	// Wait for the sub to get something
	log.Println("waiting...")
	wg.Wait()
	wg.Add(1)

	// Drain on the subscriber unsubscribes, but waits for all pending messages to be handled.
	log.Println("draining the subscription")
	if err = sub.Drain(); err != nil {
		log.Fatal(err)
	}
}

func syncExample(nc *nats.Conn) {
	log.Println("Running the synchronous example. This example uses the request/reply to send the time.")

	// create a unique subject (as this is a public server)
	ib := nats.NewInbox()
	log.Printf("The unique subject is: %q", ib)

	// subscribe to this subject
	sub, err := nc.SubscribeSync(ib)
	if err != nil {
		log.Fatal(err)
	}

	// the auto unsubscribe feature is useful for when we know we are expecting  a
	// single message which we are here. It will un-subscribe and close after 1 message
	if err = sub.AutoUnsubscribe(1); err != nil {
		log.Fatal(err)
	}

	// As we are using the blocking rc.Request to make the request, we do it in a gr
	// and signal the wait group when done. We could use the async version rc.PublishRequest instead
	wg := sync.WaitGroup{}
	wg.Add(1)

	// Set a go routine for making the request.
	go func() {
		time.Sleep(time.Millisecond * 10)
		log.Println("Sending request message with the current time.")
		var msg *nats.Msg
		var err error
		if msg, err = nc.Request(ib, []byte(time.Now().String()), time.Second*2); err != nil {
			log.Fatal(err)
			return
		}
		log.Printf("Reply back: %s", msg.Data)
		wg.Done()
	}()

	log.Println("Waiting for request.")
	m, err := sub.NextMsg(time.Second * 2)
	if err != nil {
		log.Printf("error returned from sub.NextMsg: %v", err)
		return
	}

	log.Printf("The time with the request was: %q", m.Data)

	// Messages have an optional reply-to field which can contain
	// a subject (topic remember) where a reply should be sent.
	// the following code replys with the current time
	if len(m.Reply) > 0 {
		log.Printf("Sending reply to subject: %q", m.Reply)
		m.Respond([]byte(time.Now().String()))
	}

	// Finally wait for the go routine to quit.
	wg.Wait()
}
