package main

import (
	"context"
	"fmt"
	"github.com/spf13/cobra"
	"github.com/zeebo/errs/v2"
	"go.uber.org/zap"
	"io/ioutil"
	"os"
	"storj.io/private/process"
	"storj.io/storj/satellite/metabase"
	"storj.io/storj/satellite/metabase/segmentloop"
	"storj.io/storj/satellite/metrics"
	"strings"
	"time"
)

func main() {

	cmd := cobra.Command{
		Use: "segmentloop",
	}
	connection := cmd.Flags().String("db", "postgres://root@localhost:26257/metainfo?sslmode=disable", "Database connection string")
	progressFrequency := cmd.Flags().Int64("progress", 1000, "Frequency of printing out progress status")
	cmd.RunE = func(cmd *cobra.Command, args []string) error {
		return run(*connection, *progressFrequency)
	}

	process.Exec(&cmd)
}

func run(connection string, i int64) error {
	log, err := zap.NewDevelopment()
	if err != nil {
		return errs.Wrap(err)
	}

	ctx := context.Background()

	db, err := metabase.Open(ctx, log, connection, metabase.Config{
		ApplicationName: "segmentloop-cli",
	})
	if err != nil {
		return errs.Wrap(err)
	}

	cfg := segmentloop.Config{
		CoalesceDuration: 1 * time.Second,
		ListLimit:        10000,
	}
	service := segmentloop.New(log, cfg, db)
	counter := metrics.NewCounter()

	go func() {
		err = service.Join(ctx, counter)
		if err != nil {
			log.Error("metric observer is failed", zap.Error(err))
		}
	}()

	go func() {
		err = service.Join(ctx, &ProgressObserver{
			Log:                    log,
			ProgressPrintFrequency: i,
		})
		if err != nil {
			log.Error("metric observer is failed", zap.Error(err))
		}
	}()

	observers := make([]*SegmentShare, 0)
	files, err := os.ReadDir(".")
	if err != nil {
		return errs.Wrap(err)
	}
	for _, f := range files {
		if !f.IsDir() && strings.HasPrefix(f.Name(), "nodes") {
			log.Debug("Reading node definition", zap.String("file", f.Name()))
			s, err := SegmentShareFromFile(f.Name())
			if err != nil {
				return err
			}
			observers = append(observers, s)
			go func() {
				err = service.Join(ctx, s)
				if err != nil {
					log.Error("metric observer is failed", zap.Error(err))
				}
			}()
		}
	}

	err = service.RunOnce(ctx)
	if err != nil {
		return errs.Wrap(err)
	}
	fmt.Printf("Total segments: %d\n", counter.TotalRemoteSegments)
	fmt.Printf("Total bytes: %d\n", counter.TotalRemoteBytes)
	fmt.Println()
	for _, o := range observers {
		res := o.GetResults()
		fmt.Println(res)
		_ = ioutil.WriteFile(o.Name+".out", []byte(res), 0644)
	}

	return nil
}
