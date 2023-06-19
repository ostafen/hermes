package service

import (
	"context"
	"errors"
	"sync"

	"github.com/ostafen/hermes/internal/processor"
	"github.com/ostafen/hermes/internal/projections"
)

var (
	ErrProjectionExist    = errors.New("projection already exist")
	ErrProjectionNotExist = errors.New("projection not exist")
)

type CreateProjectionInput struct {
	Name  string `json:"name" validate:"required"`
	Query string `json:"query" validate:"required"`
}

type DeleteProjectionInput struct {
	Name string `json:"name" validate:"required"`
}

type ProjectionService interface {
	Create(ctx context.Context, in CreateProjectionInput) error
	Delete(ctx context.Context, in DeleteProjectionInput) error
	Shutdown() error
}

type projectionData struct {
	projection *projections.Projection
	processor  *processor.Processor
	ctx        context.Context
	cancel     func()
}

type projectionService struct {
	mtx sync.Mutex

	cfg         processor.Config
	projections map[string]projectionData
}

func (s *projectionService) Create(ctx context.Context, in CreateProjectionInput) error {
	s.mtx.Lock()
	defer s.mtx.Unlock()

	if _, has := s.projections[in.Name]; has {
		return ErrProjectionExist
	}

	proj, err := projections.Compile(in.Name, in.Query)
	if err != nil {
		return err
	}

	proc, err := processor.BuildProcessor(proj, s.cfg)
	if err != nil {
		return err
	}

	procCtx, cancel := context.WithCancel(context.Background())
	if err := proc.Start(procCtx); err != nil {
		cancel()
		return err
	}

	s.projections[in.Name] = projectionData{
		ctx:        ctx,
		cancel:     cancel,
		projection: proj,
		processor:  proc,
	}
	return nil
}

func (p *projectionService) Delete(ctx context.Context, in DeleteProjectionInput) error {
	p.mtx.Lock()
	defer p.mtx.Unlock()

	data, has := p.projections[in.Name]
	if !has {
		return ErrProjectionNotExist
	}

	data.cancel()
	data.processor.WaitShutdown() // TODO: take ctx

	delete(p.projections, in.Name)

	return nil
}

func (p *projectionService) Shutdown() error {
	for _, data := range p.projections {
		data.cancel()
	}

	for _, data := range p.projections {
		data.processor.WaitShutdown()
	}
	return nil
}

func NewProjectionService(cfg processor.Config) ProjectionService {
	return &projectionService{
		cfg:         cfg,
		projections: make(map[string]projectionData),
	}
}
