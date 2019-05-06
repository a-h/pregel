# Pregel

A graph-oriented store for DynamoDB.

```go
s, err := pregel.NewStore("eu-west-2", "pregelStoreLocal")
if err != nil {
  fmt.Println(err)
  os.Exit(1)
}

// Register the store's types.
s.RegisterDataType(func() interface{} {
  return &computer{}
})
s.RegisterDataType(func() interface{} {
  return &router{}
})
s.RegisterDataType(func() interface{} {
  return &connection{}
})
s.RegisterDataType(func() interface{} {
  return &Location{}
})

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
routerToMac := pregel.NewEdge("adrian's mac").
  WithData(connection{
    Type: "wifi",
  })
err = s.Put(pregel.NewNode("router").
  WithData(router{
    SSID: "VM675321",
  }).
  WithData(Location{
    Lat: 48.864716,
    Lng: 2.349014,
  }).
  WithChildren(routerToMac))
if err != nil {
  fmt.Println("error creating router", err)
  os.Exit(1)
}
```

# Graph

GraphQL API on the top of Pregel.

Example, get a router, including its location.

```graphql
{
  get(id: "router") {
    id
    children(first: 100) {
      edges {
        node {
          id
          children(first: 100) {
            edges {
              node {
                id
              }
            }
          }
          data {
						... on Location {
            	lng
              lat
          	}
          }
        }
      }
    }
    parents(first: 100) {
      edges {
        node {
          id
        }
      }
    }
    data {
      ... on Location {
        lng
        lat
      }
    }
  }
}
```

Example: move the router to Paris.

```graphql
mutation moveRouterToParis {
  setNodeFields(input: {
    id: "router"
    location: {
      lat: 48.864716
      lng: 2.349014
    }
  }) {
		set
  }
}
```