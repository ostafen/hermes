package projections

import (
	"encoding/json"
	"fmt"
	"sync"

	"github.com/dop251/goja"
)

type ProjectionFunc func(state any, e Event) (any, bool)

const (
	initFunc   = "$init"
	anyHandler = "$any"
)

type Options struct {
	ResultStream  string `json:"resultStreamName"`
	IncludeLinks  bool   `json:"$includeLinks"`
	ReorderEvents bool   `json:"reorderEvents"`
	ProcessingLag int    `json:"processingLag"`
}

type Event struct {
	IsJson          bool              `json:"isJson"`
	Data            any               `json:"data"`
	Body            any               `json:"body"`
	BodyRaw         string            `json:"bodyRaw"`
	SequenceNumber  int64             `json:"sequenceNumber"`
	MetadataRaw     map[string]string `json:"metadataRaw"`
	LinkMetadataRaw string            `json:"linkMetadataRaw"`
	Partition       string            `json:"partition"`
	Type            string            `json:"eventType"`
	StreamId        string            `json:"streamId"`
}

type PartitionFunc func(e Event) string

type Projection struct {
	mtx     sync.Mutex
	runtime *goja.Runtime

	Name         string
	InputStreams []string
	Options      Options

	currState   any
	Operations  []ProjectionFunc
	partitionBy PartitionFunc
}

func (p *Projection) ResultStream() string {
	if p.Options.ResultStream != "" {
		return p.Options.ResultStream
	}
	return fmt.Sprintf("projections-%s-result", p.Name)
}

func (p *Projection) GetPartition(e Event) string {
	p.mtx.Lock()
	defer p.mtx.Unlock()
	return p.partitionBy(e)
}

type FromStreamsRes struct {
	partitionBy
	when
	outputState
}

type FromAllRes struct {
	partitionBy
	when
	outputState
}

type FromStreamsMatchingRes struct {
	when
}

type outputState struct {
	p *Projection
}

func (o *outputState) OutputState() {
}

type OutputStateRes struct {
	transformBy
	filterBy
	outputTo
}

type partitionBy struct {
	p *Projection
}

type when struct {
	p *Projection
}

type WhenRes struct {
	transformBy
	filterBy
	outputTo
	outputState
}

func (w *when) getHandler(handlers map[string]gojaFunc, eventType string) gojaFunc {
	h := handlers[eventType]
	if h == nil {
		return handlers[anyHandler]
	}
	return h
}

func (w *when) When(handlers map[string]gojaFunc) WhenRes {
	w.p.Operations = append(w.p.Operations, func(state any, e Event) (any, bool) {
		if state == nil {
			initFunc, hasInit := handlers[initFunc]
			if hasInit {
				state = initFunc.Call(w.p.runtime)
			}
		}

		handlerFunc := w.getHandler(handlers, e.Type)
		if handlerFunc != nil {
			handlerFunc.Call(w.p.runtime, state, e)
		}
		w.p.currState = state
		return state, true
	})

	return WhenRes{
		transformBy: transformBy{p: w.p},
		filterBy:    filterBy{p: w.p},
		outputTo:    outputTo{p: w.p},
		outputState: outputState{p: w.p},
	}
}

type filterBy struct {
	p *Projection
}

type FilterByRes struct {
	filterBy
	transformBy
	outputState
	outputTo
}

func (t *filterBy) FilterBy(filterFunc gojaFunc) FilterByRes {
	t.p.Operations = append(t.p.Operations, func(state any, e Event) (any, bool) {
		forward, _ := filterFunc.Call(t.p.runtime, state).(bool)
		return state, forward
	})

	return FilterByRes{
		transformBy: transformBy{p: t.p},
		filterBy:    filterBy{p: t.p},
		outputTo:    outputTo{p: t.p},
		outputState: outputState{p: t.p},
	}
}

type transformBy struct {
	p *Projection
}

type TransformByRes struct {
	transformBy
	filterBy
	outputState
	outputTo
}

func (t *transformBy) TransformBy(transformFunc gojaFunc) TransformByRes {
	t.p.Operations = append(t.p.Operations, func(state any, e Event) (any, bool) {
		out := transformFunc.Call(t.p.runtime, state)
		return out, true
	})

	return TransformByRes{
		transformBy: transformBy{p: t.p},
		filterBy:    filterBy{p: t.p},
		outputTo:    outputTo{p: t.p},
		outputState: outputState{p: t.p},
	}
}

type outputTo struct {
	p *Projection
}

func (o *outputTo) OutputTo(stream string) {
	o.p.Options.ResultStream = stream
}

func (p *Projection) fromStreams(streams ...string) FromStreamsRes {
	p.InputStreams = streams

	return FromStreamsRes{
		when:        when{p: p},
		partitionBy: partitionBy{p: p},
		outputState: outputState{p: p},
	}
}

type PartitionByRes struct {
	when
}

func (p *partitionBy) PartitionBy(partitionFunc gojaFunc) PartitionByRes {
	p.p.partitionBy = func(e Event) string {
		partition, _ := partitionFunc.Call(p.p.runtime, e).(string)
		return partition
	}

	return PartitionByRes{
		when: when{p: p.p},
	}
}

func (p *Projection) IsPartitioned() bool {
	return p.partitionBy != nil
}

func (p *Projection) updateFunc(state any, e Event) (any, bool) {
	var currState = state
	for _, op := range p.Operations {
		state, forward := op(currState, e)
		if !forward {
			return state, false
		}
		currState = state
	}
	return currState, true
}

func (p *Projection) State() any {
	return p.currState
}

func (p *Projection) Update(state any, e Event) any {
	p.mtx.Lock()
	defer p.mtx.Unlock()

	newState, forward := p.updateFunc(state, e)
	if forward {
		return newState
	}
	return nil
}

func (p *Projection) options(opts Options) {
	p.Options = opts
}

func (p *Projection) fromStream(stream string) FromStreamsRes {
	return p.fromStreams(stream)
}

func panicIfErr(err error) {
	if err != nil {
		panic(err)
	}
}

func (p *Projection) setupRuntime() {
	p.runtime.SetFieldNameMapper(goja.TagFieldNameMapper("json", true))
}

func (p *Projection) setup() {
	p.setupRuntime()
	p.setupHandlers()
}

func logFunc(elems ...any) {
	indented := make([]any, 0, len(elems))
	for _, e := range elems {
		data, _ := json.MarshalIndent(e, "", "\t")
		indented = append(indented, string(data))
	}
	fmt.Println(indented...)
}

func (p *Projection) setupHandlers() {
	err := p.runtime.Set("options", p.options)
	panicIfErr(err)

	err = p.runtime.Set("fromStream", p.fromStream)
	panicIfErr(err)

	err = p.runtime.Set("fromStreams", p.fromStreams)
	panicIfErr(err)

	err = p.runtime.Set("log", logFunc)
	panicIfErr(err)
}

func Compile(name, query string) (*Projection, error) {
	p := &Projection{
		Name:    name,
		runtime: goja.New(),
	}
	p.setup()

	_, err := p.runtime.RunString(query)
	return p, err
}
