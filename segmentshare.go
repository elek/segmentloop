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
	Nodes       map[storj.NodeID]bool
	ActiveNodes map[storj.NodeID]bool
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

func SegmentShareFromFile(file string) (s *SegmentShare, err error) {
	s = NewSegmentShare()
	s.Name = file
	s.Nodes, err = readNodes(file)
	if err != nil {
		return s, err
	}
	s.ActiveNodes, err = readNodes("active.txt")
	if err != nil {
		return s, err
	}
	return s, err
}

func readNodes(file string) (map[storj.NodeID]bool, error) {
	result := make(map[storj.NodeID]bool)
	nodes, err := ioutil.ReadFile(file)
	if err != nil {
		return result, err
	}
	for _, line := range strings.Split(string(nodes), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		line = strings.TrimPrefix(line, "\\x")

		nodeHex, err := hex.DecodeString(line)
		if err != nil {
			return result, errs.Wrap(err)
		}

		n, err := storj.NodeIDFromBytes(nodeHex)
		if err != nil {
			return result, errs.Wrap(err)
		}
		result[n] = true
	}
	return result, nil
}

func (s *SegmentShare) LoopStarted(ctx context.Context, info segmentloop.LoopInfo) error {
	return nil
}

func (s *SegmentShare) RemoteSegment(ctx context.Context, segment *segmentloop.Segment) error {
	usablePieces := len(segment.Pieces)
	activePieces := len(segment.Pieces)

	for _, piece := range segment.Pieces {
		_, active := s.ActiveNodes[piece.StorageNode]
		_, inGroup := s.Nodes[piece.StorageNode]
		if !active {
			activePieces--
			usablePieces--
		} else if inGroup {
			usablePieces--
		}

	}
	s.RemainingPieces[fmt.Sprintf("%d,%d,%d", len(segment.Pieces), activePieces, usablePieces)]++

	return nil
}

func (s *SegmentShare) InlineSegment(ctx context.Context, segment *segmentloop.Segment) error {
	return nil
}

var _ segmentloop.Observer = &SegmentShare{}

func (s *SegmentShare) GetResults() string {
	out := strings.Builder{}
	out.WriteString(fmt.Sprintf("all,active,remaining,segments\n", s.Name))
	for k, v := range s.RemainingPieces {
		out.WriteString(fmt.Sprintf("%s,%d\n", k, v))
	}
	return out.String()
}
