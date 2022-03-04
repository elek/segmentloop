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
	Nodes            map[storj.NodeID]bool
	SegmentOwnership map[int]int
	Name             string
}

func NewSegmentShare() *SegmentShare {
	return &SegmentShare{
		Nodes:            make(map[storj.NodeID]bool),
		SegmentOwnership: make(map[int]int),
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
	share := 0
	for _, piece := range segment.Pieces {
		if _, found := s.Nodes[piece.StorageNode]; found {
			share++
		}
	}
	if share > 0 {
		s.SegmentOwnership[share]++
	}
	return nil
}

func (s *SegmentShare) InlineSegment(ctx context.Context, segment *segmentloop.Segment) error {
	return nil
}

var _ segmentloop.Observer = &SegmentShare{}

func (s *SegmentShare) PrintResults() {
	max := 0
	for k := range s.SegmentOwnership {
		if k > max {
			max = k
		}
	}

	fmt.Printf("Nodes from %s (%d)\n", s.Name, len(s.Nodes))
	fmt.Println()
	fmt.Println("owned pieces,number of segments")
	for i := 0; i <= max; i++ {
		fmt.Printf("%d,%d\n", i, s.SegmentOwnership[i])
	}
}
