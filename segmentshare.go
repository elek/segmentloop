package main

import (
	"context"
	"encoding/hex"
	"fmt"
	"github.com/zeebo/errs/v2"
	"io/ioutil"
	"storj.io/common/storj"
	"storj.io/storj/satellite/metabase/segmentloop"
	"strings"
)

type SegmentShare struct {
	Nodes map[storj.NodeID]bool

	//number of pieces per segment  --> number of segments with this number of pieces remaining after Nodes are down
	RemainingPieces map[string]int
	Name            string
}

func NewSegmentShare() *SegmentShare {
	return &SegmentShare{
		Nodes:           make(map[storj.NodeID]bool),
		RemainingPieces: make(map[string]int),
	}
}

func SegmentShareFromFile(file string) (*SegmentShare, error) {
	s := NewSegmentShare()
	s.Name = file
	nodes, err := ioutil.ReadFile(file)
	if err != nil {
		return nil, errs.Wrap(err)
	}
	for _, line := range strings.Split(string(nodes), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		line = strings.TrimPrefix(line, "\\x")

		nodeHex, err := hex.DecodeString(line)
		if err != nil {
			return nil, errs.Wrap(err)
		}

		n, err := storj.NodeIDFromBytes(nodeHex)
		if err != nil {
			return nil, err
		}
		s.Nodes[n] = true
	}
	return s, nil
}

func (s *SegmentShare) LoopStarted(ctx context.Context, info segmentloop.LoopInfo) error {
	return nil
}

func (s *SegmentShare) RemoteSegment(ctx context.Context, segment *segmentloop.Segment) error {
	remaining := len(segment.Pieces)
	for _, piece := range segment.Pieces {
		if _, found := s.Nodes[piece.StorageNode]; found {
			remaining--
		}
	}
	s.RemainingPieces[fmt.Sprintf("%d,%d,%d", len(segment.Pieces), len(segment.Pieces)-remaining, remaining)]++

	return nil
}

func (s *SegmentShare) InlineSegment(ctx context.Context, segment *segmentloop.Segment) error {
	return nil
}

var _ segmentloop.Observer = &SegmentShare{}

func (s *SegmentShare) PrintResults() {

	fmt.Printf("all pieces,pieces on %s,remaining,# of such segments", s.Name)
	for k, v := range s.RemainingPieces {
		fmt.Printf("%s,%d\n", k, v)
	}
	fmt.Println()
	fmt.Println()
}
