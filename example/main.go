package main

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/a-h/pregel"
)

func main() {
	s, err := pregel.NewStore("eu-west-2", "pregelStoreLocal")
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	// Create a computer.
	fmt.Println("Creating computer node")
	err = s.Put(pregel.NewNode("adrian's mac").WithData(computer{
		Brand:         "Apple",
		YearPurchased: 2015,
	}))
	if err != nil {
		fmt.Println("error creating node", err)
		os.Exit(1)
	}

	// Create a router and a connection to the mac.
	fmt.Println("Creating router node")
	routerData := router{
		SSID: "VM675321",
	}
	routerToMac := pregel.NewEdge("adrian's mac", connection{
		Type: "wifi",
	})
	err = s.Put(pregel.NewNode("router").
		WithData(routerData).
		WithChildren(routerToMac))
	if err != nil {
		fmt.Println("error creating router", err)
		os.Exit(1)
	}

	// Create a PS4 (without any metadata).
	fmt.Println("Creating ps4")
	err = s.Put(pregel.NewNode("ps4").
		WithData(computer{}))
	if err != nil {
		fmt.Println("error creating ps4", err)
		os.Exit(1)
	}

	fmt.Println("Adding router to ps4 edges")
	err = s.PutEdges("router", pregel.NewEdge("ps4", connection{Type: "wired"}))
	if err != nil {
		fmt.Println("error creating a wired connection from router to ps4", err)
		os.Exit(1)
	}

	// Create a Nintendo Wii-U.
	fmt.Println("Creating wii node")
	err = s.Put(pregel.NewNode("wii").WithData(computer{}))
	if err != nil {
		fmt.Println("error creating wii", err)
		os.Exit(1)
	}
	fmt.Println("Creating router to wii edges")
	err = s.PutEdges("router", pregel.NewEdge("wii", connection{Type: "wifi"}))
	if err != nil {
		fmt.Println("error creating a connection from router to wii", err)
		os.Exit(1)
	}

	// Delete it.
	fmt.Println("Deleting wii node")
	err = s.Delete("wii")
	if err != nil {
		fmt.Println("error deleting wii", err)
		os.Exit(1)
	}

	// Retrieve router data.
	fmt.Println("Getting router data")
	n, _, err := s.GetWithTypedData("router", &router{})
	if err != nil {
		fmt.Println("error getting router", err)
		os.Exit(1)
	}
	r := n.Data.(*router)
	fmt.Println("SSID of router:", r.SSID)

	// Just get the PS4 data.
	fmt.Println("Getting ps4 data")
	ps4, ok, err := s.Get("ps4")
	if err != nil {
		fmt.Println("error finding ps4", err)
		os.Exit(1)
	}
	if !ok {
		fmt.Println("could not find ps4")
		os.Exit(1)
	}
	bytes, _ := json.Marshal(ps4)
	fmt.Println(string(bytes))

	// Now disconnect the PS4 from the router.
	// The deletion doesn't currently remove parent edges.
	fmt.Println("Deleting router to ps4 edge")
	err = s.DeleteEdge("router", "ps4")
	if err != nil {
		fmt.Println("error deleting relationship between router and ps4", err)
		os.Exit(1)
	}

	// Get PS4 data again.
	fmt.Println("Getting ps4 data")
	ps4, ok, err = s.Get("ps4")
	if err != nil {
		fmt.Println("error finding ps4", err)
		os.Exit(1)
	}
	if !ok {
		fmt.Println("could not find ps4")
		os.Exit(1)
	}
	bytes, _ = json.Marshal(ps4)
	fmt.Println(string(bytes))

	// Check the router has been disconnected too.
	router, ok, err := s.Get("router")
	if err != nil {
		fmt.Println("error finding router", err)
		os.Exit(1)
	}
	if !ok {
		fmt.Println("could not find router")
		os.Exit(1)
	}
	bytes, _ = json.Marshal(router)
	fmt.Println(string(bytes))

	fmt.Printf("Capacity units consumed - total: %v, read: %v, write: %v\n", s.ConsumedCapacity, s.ConsumedReadCapacity, s.ConsumedWriteCapacity)
}

type computer struct {
	Brand         string `json:"brand"`
	YearPurchased int    `json:"yearPurchased"`
}

type router struct {
	SSID string `json:"ssid"`
}

type connection struct {
	Type string `json:"connectionType"`
}
