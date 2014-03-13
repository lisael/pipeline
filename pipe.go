package pipeline

import(
	"errors"
    "fmt"
)

type PipeStatus int

type PipeError struct{
    msg string
}

func (self *PipeError) Error() string {
    return self.msg
}

func PipeErrorf(format string, chunks ...interface{}) (err *PipeError) {
    err = new(PipeError)
    err.msg = fmt.Sprintf(format, chunks...)
    return
}

const(
    ERROR PipeStatus = iota // something bad append. blocked
    WAITING // not connected yet
    READY // ready to go
    PAUSED // paused, i.e. doesn't read or write on ports
    CLOSED // finished. ports are all closed
    RUNNING // well... running.
)

type Runable interface{
    Run()
}

type Pausable interface{
    Runable
    // Pause the flow in that pipe. Must Block until it stopped reading its
    // input chan, writing its output chan, and Status() returns PAUSED
    Pause() error
    // Resume after a pause. Must block until the input chan is read. Status()
    // returns RUNNING then.
    Resume() error
    Status() PipeStatus
}

type Pipe interface{
    Pausable
    ConnectPipe(input chan interface{}) (output chan interface{}, err error)
}

type Source interface{
    Pausable
    ConnectSource() (output chan interface{}, err error)
}

type Sink interface{
    Pausable
    ConnectSink(input chan interface{}) (stop chan bool, err error)
}

type Managed interface{
	// Scale implementation is let to clients. It must decide weather it has
	// to scale (usually watching how bloated are input and output buffered
	// chans). It is also reponsible of actual scaling (usually, for CPU
	// bound tasks by adding/removing workers). It returns true if an action
	// was taken. See GenericBufferedPipe for a CPU bound example implementation
	Scale() (bool, error)
}

// Unordered is just a marker interface that unordered pipes (output is not
// quaranteed to be in the input order) MUST implement. The method itself
// is never called (at least by pipeline code. Do whatever you like!)
type Unordered interface{
	Unordered()
}

type ManagedPipe struct{
	p Pipe
	output chan interface{}
}

// PipeLine implements Pipe, Source and Sink, so subpipelines are
// runable/pausable/pumps/flushers too
type PipeLine struct{
	status PipeStatus
	source Source
	pipes []Pipe
	sink Sink
    all []Runable
}

func NewPipeLine() (pl *PipeLine){
	pl = new(PipeLine)
	pl.status = WAITING
	pl.pipes = []Pipe{}
	return
}

// implements Runable
// Run starts the whole pipeline.
func (self *PipeLine) Run(){
    self.source.Run()
	for _, p := range self.pipes{
        p.Run()
    }
    self.sink.Run()
}

// Connect makes internal plumbing. The returned channel is written when all
// Sinks are closed
// TODO: handle errors
func (self *PipeLine) Connect() (stop chan bool){
	var output chan interface{}
	output, _ = self.source.ConnectPump()
	for _, p := range self.pipes{
		output, _ = p.ConnectPipe(output)
	}
	stop, _ = self.sink.ConnectFlush(output)
    self.status = READY
	return
}

// implements Pipe
// TODO
func (self *PipeLine) ConnectPipe(input chan interface{}) (output chan interface{}){
	output = make(chan interface{})
	return
}

// implements Source
// TODO
func (self *PipeLine) ConnectPump() (output chan interface{}){
	output = make(chan interface{})
	return
}

// implements Sink
// TODO
func (self *PipeLine) ConnectFlush(input chan interface{}) (stop chan bool){
	stop = make(chan bool)
	return
}

// implements Pausable
// TODO
func (self *PipeLine) Pause() error{return nil}
// TODO
func (self *PipeLine) Resume() error{return nil}
// TODO
func (self *PipeLine) Status() PipeStatus{return WAITING}

func (self *PipeLine) AddSource(src Source) error{
	if self.status > WAITING{
		return errors.New("Can't add a source after intialization phase\n")
	}
	if self.source != nil{
		return errors.New("A source is already set")
	}
	self.source = src
	return nil
}

func (self *PipeLine) AddPipe(p Pipe) error{
	if self.status > WAITING{
		return errors.New("Can't add a pipe after intialization phase")
	}
	self.pipes = append(self.pipes, p)
	return nil
}

func (self *PipeLine) AddSink(sk Sink) error{
	if self.status > WAITING{
		return errors.New("Can't add a sink after intialization phase")
	}
	if self.sink != nil{
		return errors.New("A sink is already set")
	}
	self.sink = sk
	return nil
}

