package pipeline

import(
    "testing"
    "strings"
    "time"
    "runtime"
)

func TestGenericBufferedPipe(t *testing.T){
    runtime.GOMAXPROCS(runtime.NumCPU())
    // fake source
    input := make(chan interface{})
    go func(){
        for i:=0; i<1000; i++{
            input <- "hello"
        }
        close(input)
    }()
    processor := func(input interface{}) (output interface{}, err error){
        return strings.ToUpper(input.(string)), nil
    }
    gbp := NewGenericBufferedPipe(processor, 4, 200)
	// test it does implements Pipe
	var in interface{} = gbp
	_, ok := in.(Pipe)
	if ! ok {
		t.Errorf("*GenericBufferedPipe doesn't implement Pipe")
	}
    output, _ := gbp.ConnectPipe(input)
    gbp.Run()
    o1 := <- output
    // test that the processor was called on 
    if o1 != "HELLO"{
        t.Errorf(o1.(string))
    }
    if gbp.Status() != RUNNING{
        t.Errorf("should be running...")
    }
    // wait for the output to fill
    <- time.After(100*1e5)
    gbp.Pause()
    if gbp.Status() != PAUSED{
        t.Errorf("should be paused...")
    }
    if len(output) != 200 {
        t.Errorf("output should be full...")
    }
    <- output
    // wait for the output to fill. It should not as it is paused
    <- time.After(100*1e5)
    if len(output) == 200 {
        t.Errorf("output should not be full...")
    }
    gbp.Resume()
    // count the remaining items
    i:=0
    for _ = range output{ i++ }
    if i != 998{
        t.Errorf("expected 998 items, got %d", i)
    }
}

func TestGenericBufferedPipeOrder(t *testing.T){
    runtime.GOMAXPROCS(runtime.NumCPU())
    // fake source
    input := make(chan interface{})
    go func(){
        for i:=0; i<1000000; i++{
            input <- i
        }
        close(input)
    }()
    processor := func(in interface{}) (out interface{}, err error){
        return in, nil
    }
    gbp := NewGenericBufferedPipe(processor, 4, 200)
    output, _ := gbp.ConnectPipe(input)
    gbp.Run()
    last := -1
    for o := range output{
        if o.(int) != last + 1{
            t.Errorf("expected %d, got %d", last + 1, last)
        }
        last = o.(int)
    }
    if last != 999999{
        t.Errorf("expected 999999, got %d", last)
    }
}
