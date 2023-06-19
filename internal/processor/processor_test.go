package processor_test

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"testing"

	"github.com/lovoo/goka"
	"github.com/lovoo/goka/codec"
	"github.com/ostafen/hermes/internal/event"
	"github.com/ostafen/hermes/internal/processor"
	"github.com/ostafen/hermes/internal/projections"
	"github.com/stretchr/testify/suite"
	"github.com/testcontainers/testcontainers-go/modules/redpanda"
)

type ProcessorSuite struct {
	suite.Suite

	conf      processor.Config
	container *redpanda.Container
	brokers   []string
}

func TestProcessorSuite(t *testing.T) {
	suite.Run(t, &ProcessorSuite{})
}

type KafakConfig struct {
	Port int
}

func (s *ProcessorSuite) SetupSuite() {
	ctx := context.TODO()

	container, err := redpanda.RunContainer(ctx)
	s.NoError(err)
	s.container = container

	broker, err := s.container.KafkaSeedBroker(ctx)
	s.NoError(err)

	s.brokers = []string{broker}

	s.conf = processor.DefaultConfig(s.brokers)
	s.conf.StoragePath = processor.InMemoryStorage
}

func (s *ProcessorSuite) TearDownSuite() {
	s.NoError(s.container.Terminate(context.Background()))
}

func (s *ProcessorSuite) TestPartitionedProjection() {
	projection, err := projections.Compile("my-projection", `
		fromStream('my-stream').
		partitionBy(e => e.eventType).
		when({
			$init: function() {
				return { count: 0 }
			},
			$any: function(state, e) {
				state.count += 1
			}
		}).
		filterBy(s => s.count == 10).
		transformBy(function(state) {
			return { Total: state.count }
		}).
		outputTo('out-stream')
	`)

	s.NoError(err)

	processor, err := processor.BuildProcessor(projection, s.conf)
	s.NoError(err)

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		processor.Start(ctx)
	}()

	processor.WaitReady(ctx)

	emitter, err := goka.NewEmitter(s.brokers, "my-stream", new(codec.Bytes))
	s.NoError(err)
	defer emitter.Finish()

	x := shuffledSlice(100)

	var count int
	s.outputStreamProcessor(ctx, projection.ResultStream(), func(ctx goka.Context, msg interface{}) {
		payload := &struct {
			Total int
		}{}

		in := event.EventData{
			Data: payload,
		}

		err := json.Unmarshal(msg.([]byte), &in)
		s.NoError(err)

		if payload.Total == 10 {
			count++
		}

		if count == 10 {
			cancel()
		}
	})

	for _, i := range x {
		e := event.EventData{
			Metadata: event.Metadata{
				event.MetadataKeyEventType: fmt.Sprintf("my-type-%d", i/10),
			},
		}
		data, err := json.Marshal(e)
		s.NoError(err)

		_, err = emitter.Emit("", data)
		s.NoError(err)
	}
	processor.WaitShutdown()
}

func shuffledSlice(n int) []int {
	x := make([]int, n)
	for i := 0; i < n; i++ {
		x[i] = i
	}
	rand.Shuffle(n, func(i, j int) {
		x[i], x[j] = x[j], x[i]
	})
	return x
}

func (s *ProcessorSuite) outputStreamProcessor(ctx context.Context, stream string, cbk goka.ProcessCallback) *goka.Processor {
	processor, err := goka.NewProcessor(s.brokers,
		goka.DefineGroup(goka.Group("test-group"),
			goka.Input(goka.Stream(stream), &codec.Bytes{}, cbk)))
	s.NoError(err)

	go func() {
		s.NoError(processor.Run(ctx))
	}()
	processor.WaitForReady()
	return processor
}
