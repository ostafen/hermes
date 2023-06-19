package processor

import (
	"context"
	"encoding/json"
	"os"
	"path"
	"strconv"
	"sync"

	"github.com/Shopify/sarama"
	"github.com/google/uuid"
	"github.com/lovoo/goka"
	"github.com/lovoo/goka/codec"
	"github.com/lovoo/goka/storage"
	"github.com/ostafen/hermes/internal/event"
	"github.com/ostafen/hermes/internal/projections"
	log "github.com/sirupsen/logrus"
)

func init() {
	initGoka()
}

var kafkaCfg *sarama.Config

func initGoka() {
	kafkaCfg = goka.DefaultConfig()
	kafkaCfg.Version = sarama.V2_4_0_0
	goka.ReplaceGlobalConfig(kafkaCfg)
}

type Config struct {
	Brokers     []string
	Replication int
	Partitions  int
	StoragePath string
}

const (
	DefaultReplicationFactor = 3
	DefaultPartitions        = 1
	InMemoryStorage          = ":in-memory:"
)

func DefaultConfig(brokers []string) Config {
	replication := DefaultReplicationFactor
	if len(brokers) < DefaultReplicationFactor {
		replication = len(brokers)
	}

	return Config{
		Brokers:     brokers,
		Replication: replication,
		Partitions:  DefaultPartitions,
	}
}

type Processor struct {
	cfg Config

	client             sarama.Client
	wg                 sync.WaitGroup
	tpm                goka.TopicManager
	mainProcessor      *goka.Processor
	partitionProcessor *goka.Processor
}

func BuildProcessor(p *projections.Projection, cfg Config) (*Processor, error) {
	client, err := sarama.NewClient(cfg.Brokers, kafkaCfg)
	if err != nil {
		return nil, err
	}

	tpm, err := newTopicManager(cfg)
	if err != nil {
		return nil, err
	}

	processor := &Processor{
		cfg:    cfg,
		client: client,
		tpm:    tpm,
	}

	if err := processor.inheritConfigFromInputStreams(p.InputStreams); err != nil {
		return nil, err
	}

	var partitionProcessor *goka.Processor
	if p.IsPartitioned() {
		partitionProcessor, err = processor.buildPartitionProcessor(p)
	}

	if err != nil {
		return nil, err
	}

	inputStreams := p.InputStreams
	if p.IsPartitioned() {
		inputStreams = []string{
			partitionByTopic(p.Name),
		}
	}

	mainProcessor, err := processor.buildIputProcessor(p, inputStreams)

	processor.mainProcessor = mainProcessor
	processor.partitionProcessor = partitionProcessor

	return processor, err
}

func (p *Processor) inheritConfigFromInputStreams(topics []string) error {
	b := p.client.Brokers()[0]
	b, err := p.client.Broker(b.ID())
	if err != nil {
		return err
	}

	resp, err := b.GetMetadata(&sarama.MetadataRequest{
		Version: 5,
		Topics:  topics,
	})
	if err != nil {
		return err
	}

	var partitions int = p.cfg.Partitions
	var replication int = p.cfg.Replication
	for _, tpMeta := range resp.Topics {
		if len(tpMeta.Partitions) == 0 {
			continue
		}

		p := tpMeta.Partitions[0]
		if len(p.Replicas) > replication {
			replication = len(p.Replicas)
		}

		if len(tpMeta.Partitions) > partitions {
			partitions = len(tpMeta.Partitions)
		}
	}

	p.cfg.Partitions = partitions
	p.cfg.Replication = replication
	return nil
}

func (p *Processor) run(ctx context.Context, proc *goka.Processor) error {
	p.wg.Add(1)

	var err error
	go func() {
		defer p.wg.Done()

		err = proc.Run(ctx)
	}()

	if err := proc.WaitForReadyContext(ctx); err != nil {
		return err
	}
	return err
}

func (p *Processor) Start(ctx context.Context) error {
	err := p.run(ctx, p.mainProcessor)

	if err == nil && p.partitionProcessor != nil {
		return p.run(ctx, p.partitionProcessor)
	}
	return err
}

func (p *Processor) WaitReady(ctx context.Context) error {
	err := p.mainProcessor.WaitForReadyContext(ctx)

	if err == nil && p.partitionProcessor != nil {
		return p.partitionProcessor.WaitForReadyContext(ctx)
	}
	return err
}

func (p *Processor) WaitShutdown() {
	p.wg.Wait()
}

func newTopicManager(cfg Config) (goka.TopicManager, error) {
	return goka.NewTopicManager(cfg.Brokers, kafkaCfg, topicManagerConfig(cfg))
}

func topicManagerConfig(cfg Config) *goka.TopicManagerConfig {
	conf := goka.NewTopicManagerConfig()
	conf.Stream.Replication = cfg.Replication
	conf.Table.Replication = cfg.Replication
	return conf
}

func getState(ctx goka.Context) (any, error) {
	val, _ := ctx.Value().([]byte)

	if val == nil {
		return nil, nil
	}

	var state any
	err := json.Unmarshal(val, &state)
	return state, err
}

func setState(ctx goka.Context, state any) error {
	data, err := json.Marshal(state)
	if err == nil {
		ctx.SetValue(data)
	}
	return err
}

const (
	MetadataKeyTopicPartition = "topicPartition"
	MetadataKeyTimestamp      = "timestamp"
)

func NewEventFrom(ctx goka.Context, in event.EventData) projections.Event {
	metadata := map[string]string{
		MetadataKeyTopicPartition: strconv.FormatInt(int64(ctx.Partition()), 10),
		MetadataKeyTimestamp:      strconv.FormatInt(ctx.Timestamp().UnixNano(), 10),
	}

	for k, v := range in.Metadata {
		metadata[k] = v
	}

	return projections.Event{
		IsJson:         true,
		Partition:      ctx.Key(),
		SequenceNumber: ctx.Offset(),
		Body:           in.Data,
		Data:           in.Data,
		MetadataRaw:    metadata,
		StreamId:       string(ctx.Topic()),
		Type:           in.Metadata.EventType(),
	}
}

func (proc *Processor) buildIputProcessor(p *projections.Projection, streams []string) (*goka.Processor, error) {
	cb := func(ctx goka.Context, msg any) {
		rawMessage := msg.([]byte)

		var inData event.EventData
		if err := json.Unmarshal(rawMessage, &inData); err != nil {
			log.Error(err)
			return
		}

		e := NewEventFrom(ctx, inData)

		currState, err := getState(ctx)
		if err != nil {
			log.Error(err)
			return
		}

		output, err := p.Update(currState, e)
		if err != nil {
			log.Error(err)
			return
		}

		setState(ctx, p.State())

		if output != nil {
			outData := newOutputEvent(output)

			data, err := json.Marshal(outData) // TODO: Remove NaN values from output
			if err != nil {
				log.Error(err)
				return
			}

			ctx.Emit(goka.Stream(p.ResultStream()), "", data)
		}
	}

	group, err := proc.defineGroupGraph(streams, p.ResultStream(), p.Name+"-group", cb)
	if err != nil {
		return nil, err
	}
	return proc.newGokaProcessor(group)
}

func (proc *Processor) newGokaProcessor(group *goka.GroupGraph) (*goka.Processor, error) {
	return goka.NewProcessor(
		proc.cfg.Brokers,
		group,
		goka.WithTopicManagerBuilder(goka.TopicManagerBuilderWithTopicManagerConfig(topicManagerConfig(proc.cfg))),
		goka.WithStorageBuilder(proc.storageBuilder(string(group.Group()))),
	)
}

func (proc *Processor) storageBuilder(projectionName string) storage.Builder {
	if proc.cfg.StoragePath == InMemoryStorage {
		return storage.MemoryBuilder()
	}
	return storage.DefaultBuilder(path.Join(os.TempDir(), projectionName))
}

const (
	EventTypeProjectionResult = "Result"
)

func newOutputEvent(data any) event.EventData {
	return event.EventData{
		EventID:     uuid.NewString(),
		ContentType: event.ContentTypeJson,
		Metadata: event.Metadata{
			event.MetadataKeyEventType: EventTypeProjectionResult,
		},
		Data: data,
	}
}
func partitionByTopic(name string) string {
	return name + "-partition-by-output"
}

func (proc *Processor) buildPartitionProcessor(p *projections.Projection) (*goka.Processor, error) {
	outputTopic := partitionByTopic(p.Name)

	cb := func(ctx goka.Context, msg any) {
		rawMessage := msg.([]byte)

		var inData event.EventData
		if err := json.Unmarshal(rawMessage, &inData); err != nil {
			panic(err)
		}

		e := NewEventFrom(ctx, inData)
		partition := p.GetPartition(e)
		ctx.Emit(goka.Stream(outputTopic), partition, rawMessage)
	}

	group, err := proc.defineGroupGraph(p.InputStreams, outputTopic, p.Name+"-partition-by-group", cb)
	if err != nil {
		return nil, err
	}
	return proc.newGokaProcessor(group)
}

func (proc *Processor) defineGroupGraph(inputStreams []string, outputStream string, groupName string, callback goka.ProcessCallback) (*goka.GroupGraph, error) {
	inputs := make([]goka.Edge, 0, len(inputStreams))
	for _, stream := range inputStreams {
		if err := proc.tpm.EnsureStreamExists(stream, proc.cfg.Partitions); err != nil {
			return nil, err
		}
		inputs = append(inputs, goka.Input(goka.Stream(stream), &codec.Bytes{}, callback))
	}

	if err := proc.tpm.EnsureStreamExists(outputStream, proc.cfg.Partitions); err != nil {
		return nil, err
	}

	return goka.DefineGroup(goka.Group(groupName),
		append(inputs,
			goka.Output(goka.Stream(outputStream), &codec.Bytes{}),
			goka.Persist(&codec.Bytes{}),
		)...), nil
}
