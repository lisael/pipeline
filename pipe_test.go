package pipeline

import(
    "testing"
)


// a simple regular Pipe
type PassThrough struct{
    pause chan bool
	status PipeStatus
}

func NewPassThrough() (p *PassThrough){
	p = new(PassThrough)
	p.pause = make(chan bool)
	return
}

func (p *PassThrough) Pause() error{
    p.pause <- true
    p.status = PAUSED
	return nil
}

func (p *PassThrough) Resume() error{
    p.pause <- true
    p.status = RUNNING
	return nil
}

func (p *PassThrough) Status() PipeStatus{
	return p.status
}


func (p *PassThrough) Run(input chan interface{}) (output chan interface{}){
    output = make(chan interface{})
    go func(){
        for {
            select{
            case <- p.pause:
                <- p.pause
            default:
            }
            stuff := <-input
			if stuff == nil{ break }
            stufs := stuff.(string)
			_ = stufs
            output <- stuff
        }
		close(output)
    }()
	p.status = RUNNING
    return
}

// a simple Source
type DummySource struct{
	msg string
	size int
	status PipeStatus
	pause  chan	bool
}

func NewDummySource(msg string, size int)(ds *DummySource){
	ds = new(DummySource)
	ds.size = size
	ds.msg = msg
	ds.status = WAITING
	return
}


func (self *DummySource) Pause() error{
	if self.status == PAUSED{return nil}
    self.pause <- true
    self.status = PAUSED
	return nil
}

func (self *DummySource) Resume() error{
	if self.status == RUNNING{return nil}
    self.pause <- true
    self.status = RUNNING
	return nil
}

func (self *DummySource) Status() PipeStatus{
	return self.status
}

func (self *DummySource) Pump() (output chan interface{}){
	output = make(chan interface{})
	go func(){
		for i := 0; i<self.size; i++{
			select{
			case <- self.pause:
				<- self.pause
			default:
				output <- self.msg
			}
		}
		close(output)
	}()
	return
}

// a simple counting blackhole Sink 
type NullSink struct{
	status PipeStatus
	pause  chan	bool
	items int
}

func NewNullSink()(ns *NullSink){
	ns = new(NullSink)
	ns.status = WAITING
	ns.items = 0
	return
}


func (self *NullSink) Pause() error{
    self.pause <- true
    self.status = PAUSED
	return nil
}

func (self *NullSink) Resume() error{
    self.pause <- true
    self.status = RUNNING
	return nil
}

func (self *NullSink) Status() PipeStatus{
	return self.status
}

func (self *NullSink) Flush(input chan interface{}) (stop chan bool){
	stop = make(chan bool)
	go func(){
        for {
            select{
            case <- self.pause:
                <- self.pause
            default:
            }
            stuff := <-input
			if stuff == nil{ break }
			self.items ++
        }
        stop <- true
		close(stop)
	}()
	return
}



// we first test our testing structs to be sure they were correctly implemented

func TestPassThrough(t *testing.T){
    pt := NewPassThrough()
	// test it does implements Pipe
	var i interface{} = pt
	_, ok := i.(Pipe)
	if ! ok {
		t.Errorf("*PassThrough doesn't implement Pipe")
	}
	// mock a source
    input := make(chan interface{}) 
	inputs := "hello"
    go func(){
        input <- inputs
		close(input)
    }()
	// does what we expect. Passing through.
    o := pt.Run(input)
    res := <- o
	if res != inputs{
		t.Errorf("Expected %s on output", inputs)
    }
}


func TestDummySource(t *testing.T){
	ds := NewDummySource("hello", 100)
	// test it does implements Source
	var in interface{} = ds
	_, ok := in.(Source)
	if ! ok {
		t.Errorf("*DummySource doesn't implement Source")
	}
}

func TestNullSink(t *testing.T){
	ns := NewNullSink()
	// test it does implements Sink
	var in interface{} = ns
	_, ok := in.(Sink)
	if ! ok {
		t.Errorf("*NullSink doesn't implement Sink")
	}
}

// let's test the pipeline...
func TestPipeLine(t *testing.T){
	pl := NewPipeLine()
	pl.AddSource(NewDummySource("hello", 1000))
	pl.AddPipe(NewPassThrough())
	pl.AddPipe(NewPassThrough())
	sink := NewNullSink()
	pl.AddSink(sink)
	stop := pl.Stream()
	<- stop
    if sink.items != 1000 {
        t.Errorf("expected 1000 items in sink. got %d", sink.items)
	}
}

