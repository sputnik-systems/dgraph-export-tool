package main

import (
	"context"
	"flag"
	"net/http"
	"os"
	"time"

	"github.com/preved911/resourcelock/ydb"
	ydbenv "github.com/ydb-platform/ydb-go-sdk-auth-environ"
	ydbsdk "github.com/ydb-platform/ydb-go-sdk/v3"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/klog"

	"github.com/sputnik-systems/dgraph-backup-tool/internal/dgraph/backup"
)

func main() {
	klog.InitFlags(nil)

	dgraphEndpointURL := flag.String("dgraph.endpoint-url", "http://localhost:8080/admin", "Dgraph instance admin endpoint")
	dgraphBackupDest := flag.String("dgraph.backup-dest", "", "Dgraph backup export destination url")
	dgraphBackupPeriod := flag.Duration("dgraph.backup-period", time.Hour, "Dgraph backup period")
	ydbDatabaseName := flag.String("ydb.database-name", "", "YDB database name for init connection")
	ydbTableName := flag.String("ydb.table-name", "", "YDB table name")
	ydbLeaseName := flag.String("ydb.lease-name", "", "YDB lease name")
	leaseDuration := flag.Duration("leaderelection.lease-duration", 15*time.Second, "LeaderElection lease duration")
	renewDeadline := flag.Duration("leaderelection.renew-deadline", 10*time.Second, "LeaderElection renew deadline")
	retryPeriod := flag.Duration("leaderelection.retry-period", 2*time.Second, "LeaderElection retry period")

	flag.Parse()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	db, err := ydbsdk.Open(ctx, "grpcs://ydb.serverless.yandexcloud.net:2135",
		ydbenv.WithEnvironCredentials(ctx),
		ydbsdk.WithDatabase(*ydbDatabaseName),
	)
	if err != nil {
		klog.Fatal(err)
	}
	defer db.Close(ctx)

	identity, err := os.Hostname()
	if err != nil {
		klog.Fatal(err)
	}
	lock := ydb.New(db, *ydbTableName, *ydbLeaseName, identity)
	lec := leaderelection.LeaderElectionConfig{
		Lock:          lock,
		LeaseDuration: *leaseDuration,
		RenewDeadline: *renewDeadline,
		RetryPeriod:   *retryPeriod,
		Callbacks: leaderelection.LeaderCallbacks{
			OnStartedLeading: func(ctx context.Context) {
				accessKey := os.Getenv("AWS_ACCESS_KEY_ID")
				secretKey := os.Getenv("AWS_SECRET_ACCESS_KEY")
				backupLoop(ctx, *dgraphEndpointURL, *dgraphBackupDest, accessKey, secretKey, *dgraphBackupPeriod)
			},
			OnStoppedLeading: func() {
				klog.V(3).Infof("stopped leading")
			},
			OnNewLeader: func(identity string) {
				klog.Infof("%s is leader now", identity)
			},
		},
		Name: "Dgraph Backup Tool",
	}
	le, err := leaderelection.NewLeaderElector(lec)
	if err != nil {
		klog.Fatal(err)
	}

	if err := lock.CreateTable(ctx); err != nil {
		klog.Fatal(err)
	}

	go func(cancel context.CancelFunc) {
		http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {})
		if err := http.ListenAndServe(":8081", nil); err != nil {
			klog.Error(err)
		}

		klog.Info("http handler finished")

		cancel()
	}(cancel)

	le.Run(ctx)
}

func backupLoop(ctx context.Context, endpoint, dest, accessKey, secretKey string, period time.Duration) {
	klog.V(3).Info("started backup loop")

	b, err := backup.NewClient(endpoint, dest,
		backup.WithAccessKey(accessKey),
		backup.WithSecretKey(secretKey),
	)
	if err != nil {
		klog.Fatal(err)
	}

	for ticker := time.NewTicker(period); ; {
		select {
		case <-ticker.C:
			klog.Info("make backup export request")

			resp, err := b.Export(ctx)
			if err != nil {
				klog.Error(err)
				continue
			}

			klog.Infof("exported files: %v", resp.GetFiles())
		case <-ctx.Done():
			return
		}
	}
}
