# yakc-go [![Build Status](https://travis-ci.org/informationsea/yakc-go.svg)](https://travis-ci.org/informationsea/yakc-go)

Yet Another Kyoto Cabinet Binding for GO

## Warning

API of this library is not stable.

## Get code

````
go get github.com/informationsea/yakc-go
````

## Basic use
````go
package main

import "fmt"
import "github.com/informationsea/yakc-go"

func main() {
	kdb, _ := yakc.Open("newdb.kch")
	kdb.Set("A", "B")
	kdb.Set("1", "2")
	kdb.Set("Hello", "World")

	value, _ := kdb.Get("A")
	fmt.Printf("Value for A = %s\n", value)

	value, _ = kdb.Get("1")
	fmt.Printf("Value for 1 = %s\n", value)

	value, _ = kdb.Get("Hello")
	fmt.Printf("Value for Hello = %s\n", value)
}
````
