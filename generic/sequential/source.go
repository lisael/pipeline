package sequential

import(
    "github.com/lisael/pipeline"
    "github.com/lisael/pipeline/generic"
)

type task struct{
	data interface{}
	result chan interface{}
}

type SourceProcessor func(
    config interface{},
    state interface{},
    errors chan error,
    output chan interface{})
) (endState interface{})

type Source struct{
	status		pipeline.PipeStatus
	processor	SourceProcessor
    errors      chan error
    results     chan interface{}
    output      chan interface{}
    config      interface{}
    state       interface{}
	pause	    chan bool
	resume	    chan bool
}

func NewSource(processor SourceProcessor, config interface{}, buffer int) (p *Source){
	p = new(Source)
	p.status = pipeline.WAITING
	p.processor = processor
	if buffer < 0 { buffer = 0}
    p.config = config
    p.output = make(chan interface{}, buffer)
    p.results = make(chan interface{})
	p.errors = make(chan error)
	p.pause = make(chan bool)
	p.resume = make(chan bool)
	return
}

func (self *Source) Status() pipeline.PipeStatus{
	return self.status
}

func (self *Source) ConnectSource() (output chan interface{}, err error){
    self.input = input
    self.status = pipeline.READY
    return self.output, nil
}

func (self *Source) writeOutput(
    forever:
    var result interface{}
    var ok bool
    for{
        select{
        case <- self.pause:
            <- self.resume
        case result, ok =<- self.results:
            if !ok {
                break forever
            }
        }
        sendloop:
        for{
            select{
            case self.output <- result:
                break sendloop
            case self.pause:
                <- self.resume
            }
        }
    }
    close(self.output)
)

func (self *Source) run(endState chan interface{}){
    endState <- self.processor(self.config, self.state, self.errors, self.results)
}

func (self *Source) Run(){
    es := make(chan interface{})
    go self.run(es)
    go self.writeOutput{}
    self.state <- es
}
