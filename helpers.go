package pipeline

import(
	"runtime"
)

type task struct{
	data interface{}
	result chan interface{}
}

type PipeProcessor func(input interface{}) (output interface{}, err error)

// GenericBufferedPipe applies a function to elements of its input chan and
// sends the result on its output. It is a buffered pipe for CPU bound tasks
type GenericBufferedPipe struct{
	status		PipeStatus
	processor	PipeProcessor
	workers		int // max number of workers
	running		int // running workers
	tasks		chan *task // work queue
	results		chan chan interface{} // pending results
	bufferSize	int
	stop		chan bool // tells one worker to stop
    errors      chan error

    // control input and output read/write
	pausei	    chan bool
	resumei	    chan bool
	pauseo	    chan bool
	resumeo     chan bool
}

// NewGenericBufferedPipe is the standard constructor. It is scalable up to `workers`
// workers. If 0, it defaults to GOMAXPROCS (must be set before...). The buffer size
// is fixed by `buffer`. 1000 seems a good trade-off to avoid both starvation and bloating
// while keeping RAM usage low (for typical result structs).
func NewGenericBufferedPipe(processor PipeProcessor, workers int, buffer int) (p *GenericBufferedPipe){
	p = new(GenericBufferedPipe)
	p.status = WAITING
	p.processor = processor
	if workers < 1{ workers = runtime.GOMAXPROCS(0) }
	p.workers = workers
	if buffer < 0 { buffer = 0}
	p.bufferSize = buffer
	p.tasks = make(chan *task, buffer)
	p.results = make(chan chan interface{}, buffer)
	p.stop = make(chan bool)
	p.errors = make(chan error)
	p.pausei = make(chan bool)
	p.pauseo= make(chan bool)
	p.resumei = make(chan bool)
	p.resumeo = make(chan bool)
	return
}

func (self *GenericBufferedPipe) worker(){
	for{
		select{
		case <- self.stop:
			return
		case t, ok := <-self.tasks:
            if !ok{return}
			result, _ := self.processor(t.data)
			t.result <- result
		}
	}
}

func (self *GenericBufferedPipe) stopWorkers(){
	for self.running > 0{
		self.stop <- true
		self.running --
	}
}

func (self *GenericBufferedPipe) startAllWorkers(){
	for i:=0; i<self.workers; i++ {
		go self.worker()
		self.running ++
	}
}

func (self *GenericBufferedPipe) Pause() error{
    // TODO: return an error here
	if self.status != RUNNING{ return nil }
	// stop the read/write goroutines (launched in Run())
	self.pausei <- true
	self.pauseo <- true
	self.stopWorkers()
	self.status = PAUSED
	return nil
}

func (self *GenericBufferedPipe) Resume() error{
    // TODO: return an error here
	if self.status != PAUSED{ return nil }
    self.startAllWorkers()
    self.resumei <- true
    self.resumeo <- true
    self.status = RUNNING
	return nil
}

func (self *GenericBufferedPipe) Status() PipeStatus{
	return self.status
}


func (self *GenericBufferedPipe) Run(input chan interface{}) (output chan interface{}){
    output = make(chan interface{}, self.bufferSize)
    self.startAllWorkers()
	// read the input
	go func(){
        var ok bool
        outer1:
		for {
            var i interface{}
            select{
                case <- self.pausei:
                    <- self.resumei
                case i, ok = <- input:
                    if !ok {
                        break outer1
                    }
            }
			result := make(chan interface{},1)
            inner1:
            for {
                select{
                case <- self.pausei:
                    <- self.resumei
                case self.results <- result:
                    break inner1
                }
            }
            t := new(task)
			t.data = i
			t.result = result
            inner2:
            for {
                select{
                case <- self.pausei:
                    <- self.resumei
                case self.tasks <- t:
                    break inner2
                }
            }
		}
        close(self.results)
        close(self.tasks)
	}()
	// read the ordered results and fill the output
	go func(){
        var ok bool
        outer2:
		for {
            var r interface{}
            var result chan interface{}
            select{
                case <- self.pauseo:
                    <- self.resumeo
                case result, ok = <- self.results:
                    if !ok {
                        break outer2
                    }
            }
            inner3:
            for {
                select{
                case <- self.pauseo:
                    <- self.resumeo
                case r = <- result:
                    break inner3
                }
            }
            inner4:
            for {
                select{
                case <- self.pauseo:
                    <- self.resumeo
                case output <- r:
                    break inner4
                }
            }
		}
		close(output)
	}()
	self.status = RUNNING
    return
}

