package buffered

import(
	"runtime"
    "github.com/lisael/pipeline"
    "github.com/lisael/pipeline/generic"
)

type task struct{
	data interface{}
	result chan interface{}
}


// Pipe applies a function to elements of its input chan and
// sends the result on its output. It is a buffered pipe for CPU bound tasks
type Pipe struct{
	status		pipeline.PipeStatus
	processor	generic.PipeProcessor
	workers		int // max number of workers
	running		int // running workers
	tasks		chan *task // work queue
	results		chan chan interface{} // pending results
	stop		chan bool // tells one worker to stop
    errors      chan error
    input       chan interface{}
    output       chan interface{}

    // control input and output read/write
	pausei	    chan bool
	resumei	    chan bool
	pauseo	    chan bool
	resumeo     chan bool
}

// NewPipe is the standard constructor. It is scalable up to `workers`
// workers. If 0, it defaults to GOMAXPROCS (must be set before...). The buffer size
// is fixed by `buffer`. 1000 seems a good trade-off to avoid both starvation and bloating
// while keeping RAM usage low (for typical result structs).
func NewPipe(processor generic.PipeProcessor, workers int, buffer int) (p *Pipe){
	p = new(Pipe)
	p.status = pipeline.WAITING
	p.processor = processor
	if workers < 1{ workers = runtime.GOMAXPROCS(0) }
	p.workers = workers
	if buffer < 0 { buffer = 0}
	p.tasks = make(chan *task, buffer)
	p.results = make(chan chan interface{}, buffer)
    p.output = make(chan interface{}, buffer)
	p.stop = make(chan bool)
	p.errors = make(chan error)
	p.pausei = make(chan bool)
	p.pauseo= make(chan bool)
	p.resumei = make(chan bool)
	p.resumeo = make(chan bool)
	return
}

func (self *Pipe) worker(){
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

func (self *Pipe) stopWorkers(){
	for self.running > 0{
		self.stop <- true
		self.running --
	}
}

func (self *Pipe) startAllWorkers(){
	for i:=0; i<self.workers; i++ {
		go self.worker()
		self.running ++
	}
}

func (self *Pipe) Pause() error{
    // TODO: return an error here
	if self.status != pipeline.RUNNING{ return nil }
	// stop the read/write goroutines (launched in Run())
	self.pausei <- true
	self.pauseo <- true
	self.stopWorkers()
	self.status = pipeline.PAUSED
	return nil
}

func (self *Pipe) Resume() error{
    // TODO: return an error here
	if self.status != pipeline.PAUSED{ return nil }
    self.startAllWorkers()
    self.resumei <- true
    self.resumeo <- true
    self.status = pipeline.RUNNING
	return nil
}

func (self *Pipe) Status() pipeline.PipeStatus{
	return self.status
}

func (self *Pipe) ConnectPipe(input chan interface{}) (output chan interface{}, err error){
    self.input = input
    self.status = pipeline.READY
    return self.output, nil
}

func (self *Pipe) Run(){
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
                case i, ok = <- self.input:
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
                case self.output <- r:
                    break inner4
                }
            }
		}
		close(self.output)
	}()
	self.status = pipeline.RUNNING
    return
}

