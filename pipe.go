package pipeline

import(
	"errors"
)

type PipeStatus int

const(
    WAITING PipeStatus = iota
    RUNNING
    PAUSED
    CLOSED
    ERROR
)

type Pausable interface{
    // Pause the flow in that pipe. Must Block until it stoped reading its
    // input chan, writing its output chan, and Status() must return PAUSED
    Pause() error
    // Resume after a pause. Must block until the input chan is read. Status()
    // must return RUNNING
    Resume() error
    // Status returns a PipeStatus
    Status() PipeStatus
}

type Pipe interface{
    Pausable
    Run(input chan interface{}) (output chan interface{})
}

type Source interface{
    Pausable
    Pump() (output chan interface{})
}

type Sink interface{
    Pausable
    Flush(input chan interface{}) (stop chan bool)
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
}

func NewPipeLine() (pl *PipeLine){
	pl = new(PipeLine)
	pl.status = WAITING
	pl.pipes = []Pipe{}
	return
}

// implements Pipe
func (self *PipeLine) Run(input chan interface{}) (output chan interface{}){
	output = make(chan interface{})
	return
}

// implements Source
func (self *PipeLine) Pump() (output chan interface{}){
	output = make(chan interface{})
	return
}

// implements Sink
func (self *PipeLine) Flush(input chan interface{}) (stop chan bool){
	stop = make(chan bool)
	return
}

// implements Pausable
func (self *PipeLine) Pause() error{return nil}
func (self *PipeLine) Resume() error{return nil}
func (self *PipeLine) Status() PipeStatus{return WAITING}

// Stream runs the whole pipeline. The returned channel is written when all
// Sinks are closed
func (self *PipeLine) Stream() (stop chan bool){
	var output chan interface{}
	output = self.source.Pump()
	for _, p := range self.pipes{
		output = p.Run(output)
	}
	stop = self.sink.Flush(output)
	return
}

func (self *PipeLine) AddSource(src Source) error{
	if self.status != WAITING{
		return errors.New("Can't add a source after intialization phase\n")
	}
	if self.source != nil{
		return errors.New("A source is already set")
	}
	self.source = src
	return nil
}

func (self *PipeLine) AddPipe(p Pipe) error{
	if self.status != WAITING{
		return errors.New("Can't add a source after intialization phase")
	}
	self.pipes = append(self.pipes, p)
	return nil
}

func (self *PipeLine) AddSink(sk Sink) error{
	if self.status != WAITING{
		return errors.New("Can't add a sink after intialization phase")
	}
	if self.sink != nil{
		return errors.New("A sink is already set")
	}
	self.sink = sk
	return nil
}

