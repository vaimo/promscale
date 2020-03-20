package pgmodel

import (
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v4/pgxpool"

	"github.com/docker/go-connections/nat"
	"github.com/jackc/pgx/v4"
	_ "github.com/jackc/pgx/v4/stdlib"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/prompb"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

var (
	database           = flag.String("database", "tmp_db_timescale_migrate_test", "database to run integration tests on")
	useDocker          = flag.Bool("use-docker", true, "start database using a docker container")
	pgHost             = "localhost"
	pgPort    nat.Port = "5432/tcp"
)

const (
	expectedVersion = 1
	defaultDB       = "postgres"
)

func TestMigrate(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	withDB(t, *database, func(db *pgxpool.Pool, t *testing.T) {
		var version int64
		var dirty bool
		err := db.QueryRow(context.Background(), "SELECT version, dirty FROM schema_migrations").Scan(&version, &dirty)
		if err != nil {
			t.Fatal(err)
		}
		if version != expectedVersion {
			t.Errorf("Version unexpected:\ngot\n%d\nwanted\n%d", version, expectedVersion)
		}
		if dirty {
			t.Error("Dirty is true")
		}

	})
}

func testConcurrentMetricTable(t *testing.T, db *pgxpool.Pool, metricName string) int64 {
	var id *int64 = nil
	var name *string = nil
	err := db.QueryRow(context.Background(), "SELECT id, table_name FROM _prom_internal.create_metric_table($1)", metricName).Scan(&id, &name)
	if err != nil {
		t.Fatal(err)
	}
	if id == nil || name == nil {
		t.Fatalf("NULL found")
	}
	return *id
}

func testConcurrentNewLabel(t *testing.T, db *pgxpool.Pool, labelName string) int64 {
	var id *int64 = nil
	err := db.QueryRow(context.Background(), "SELECT _prom_internal.get_new_label_id($1, $1)", labelName).Scan(&id)
	if err != nil {
		t.Fatal(err)
	}
	if id == nil {
		t.Fatalf("NULL found")
	}
	return *id
}

func testConcurrentCreateSeries(t *testing.T, db *pgxpool.Pool, index int) int64 {
	var id *int64 = nil
	err := db.QueryRow(context.Background(), "SELECT _prom_internal.create_series($1, array[$1::int])", index).Scan(&id)
	if err != nil {
		t.Fatal(err)
	}
	if id == nil {
		t.Fatalf("NULL found")
	}
	return *id
}

func TestConcurrentSQL(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	withDB(t, *database, func(db *pgxpool.Pool, t *testing.T) {
		for i := 0; i < 10; i++ {
			name := fmt.Sprintf("metric_%d", i)
			var id1, id2 int64
			var wg sync.WaitGroup
			wg.Add(2)
			go func() {
				defer wg.Done()
				id1 = testConcurrentMetricTable(t, db, name)
			}()
			go func() {
				defer wg.Done()
				id2 = testConcurrentMetricTable(t, db, name)
			}()
			wg.Wait()

			if id1 != id2 {
				t.Fatalf("ids aren't equal: %d != %d", id1, id2)
			}

			wg.Add(2)
			go func() {
				defer wg.Done()
				id1 = testConcurrentNewLabel(t, db, name)
			}()
			go func() {
				defer wg.Done()
				id2 = testConcurrentNewLabel(t, db, name)
			}()
			wg.Wait()

			if id1 != id2 {
				t.Fatalf("ids aren't equal: %d != %d", id1, id2)
			}

			wg.Add(2)
			go func() {
				defer wg.Done()
				id1 = testConcurrentCreateSeries(t, db, i)
			}()
			go func() {
				defer wg.Done()
				id2 = testConcurrentCreateSeries(t, db, i)
			}()
			wg.Wait()

			if id1 != id2 {
				t.Fatalf("ids aren't equal: %d != %d", id1, id2)
			}
		}
	})
}

func TestPGConnection(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	db, err := pgx.Connect(context.Background(), PGConnectURL(t, defaultDB))
	defer db.Close(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	var res int
	err = db.QueryRow(context.Background(), "SELECT 1").Scan(&res)
	if err != nil {
		t.Fatal(err)
	}
	if res != 1 {
		t.Errorf("Res is not 1 but %d", res)
	}
}

func TestSQLGetOrCreateMetricTableName(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	withDB(t, *database, func(db *pgxpool.Pool, t *testing.T) {
		metricName := "test_metric_1"
		var metricID int
		var tableName string
		err := db.QueryRow(context.Background(), "SELECT * FROM get_or_create_metric_table_name(metric_name => $1)", metricName).Scan(&metricID, &tableName)
		if err != nil {
			t.Fatal(err)
		}
		if metricName != tableName {
			t.Errorf("expected metric and table name to be the same: got %v wanted %v", metricName, tableName)
		}
		if metricID <= 0 {
			t.Errorf("metric_id should be >= 0:\ngot:%v", metricID)
		}
		savedMetricID := metricID

		//query for same name should give same result
		err = db.QueryRow(context.Background(), "SELECT * FROM get_or_create_metric_table_name($1)", metricName).Scan(&metricID, &tableName)
		if err != nil {
			t.Fatal(err)
		}
		if metricName != tableName {
			t.Errorf("expected metric and table name to be the same unexpected:\ngot\n%v\nwanted\n%v", metricName, tableName)
		}
		if metricID != savedMetricID {
			t.Errorf("metric_id should be same:\nexpected:%v\ngot:%v", savedMetricID, metricID)
		}

		//different metric id should give new result
		metricName = "test_metric_2"
		err = db.QueryRow(context.Background(), "SELECT * FROM get_or_create_metric_table_name($1)", metricName).Scan(&metricID, &tableName)
		if err != nil {
			t.Fatal(err)
		}
		if metricName != tableName {
			t.Errorf("expected metric and table name to be the same unexpected:\ngot\n%v\nwanted\n%v", metricName, tableName)
		}
		if metricID == savedMetricID {
			t.Errorf("metric_id should not be same:\nexpected: != %v\ngot:%v", savedMetricID, metricID)
		}
		savedMetricID = metricID

		//test long names that don't fit as table names
		metricName = "test_metric_very_very_long_name_have_to_truncate_it_longer_than_64_chars_1"
		err = db.QueryRow(context.Background(), "SELECT * FROM get_or_create_metric_table_name($1)", metricName).Scan(&metricID, &tableName)
		if err != nil {
			t.Fatal(err)
		}
		if metricName == tableName {
			t.Errorf("expected metric and table name to not be the same unexpected:\ngot\n%v", tableName)
		}
		if metricID == savedMetricID {
			t.Errorf("metric_id should not be same:\nexpected: != %v\ngot:%v", savedMetricID, metricID)
		}
		savedTableName := tableName
		savedMetricID = metricID

		//another call return same info
		err = db.QueryRow(context.Background(), "SELECT * FROM get_or_create_metric_table_name($1)", metricName).Scan(&metricID, &tableName)
		if err != nil {
			t.Fatal(err)
		}
		if savedTableName != tableName {
			t.Errorf("expected table name to be the same:\ngot\n%v\nexpected\n%v", tableName, savedTableName)
		}
		if metricID != savedMetricID {
			t.Errorf("metric_id should be same:\nexpected:%v\ngot:%v", savedMetricID, metricID)
		}

		//changing just ending returns new table
		metricName = "test_metric_very_very_long_name_have_to_truncate_it_longer_than_64_chars_2"
		err = db.QueryRow(context.Background(), "SELECT * FROM get_or_create_metric_table_name($1)", metricName).Scan(&metricID, &tableName)
		if err != nil {
			t.Fatal(err)
		}
		if savedTableName == tableName {
			t.Errorf("expected table name to not be the same:\ngot\n%v\nnot =\n%v", tableName, savedTableName)
		}
		if metricID == savedMetricID {
			t.Errorf("metric_id should not be same:\nexpected:%v\ngot:%v", savedMetricID, metricID)
		}
	})
}

func verifyChunkInterval(t *testing.T, db *pgxpool.Pool, tableName string, expectedDuration time.Duration) {
	var intervalLength int64

	err := db.QueryRow(context.Background(),
		`SELECT d.interval_length
	 FROM _timescaledb_catalog.hypertable h
	 INNER JOIN LATERAL
	 (SELECT dim.interval_length FROM _timescaledb_catalog.dimension dim WHERE dim.hypertable_id = h.id ORDER BY dim.id LIMIT 1) d
	    ON (true)
	 WHERE table_name = $1`,
		tableName).Scan(&intervalLength)
	if err != nil {
		t.Error(err)
	}

	dur := time.Duration(time.Duration(intervalLength) * time.Microsecond)
	if dur != expectedDuration {
		t.Errorf("Unexpected chunk interval for table %v: got %v want %v", tableName, dur, expectedDuration)
	}
}

func TestSQLChunkInterval(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}
	withDB(t, *database, func(db *pgxpool.Pool, t *testing.T) {
		ts := []prompb.TimeSeries{
			{
				Labels: []prompb.Label{
					{Name: metricNameLabelName, Value: "test"},
					{Name: "test", Value: "test"},
				},
				Samples: []prompb.Sample{
					{Timestamp: 1, Value: 0.1},
					{Timestamp: 2, Value: 0.2},
				},
			},
			{
				Labels: []prompb.Label{
					{Name: metricNameLabelName, Value: "test2"},
					{Name: "test", Value: "test"},
				},
				Samples: []prompb.Sample{
					{Timestamp: 1, Value: 0.1},
					{Timestamp: 2, Value: 0.2},
				},
			},
		}
		ingestor := NewPgxIngestor(db)
		_, err := ingestor.Ingest(ts)
		if err != nil {
			t.Error(err)
		}
		verifyChunkInterval(t, db, "test", time.Duration(8*time.Hour))
		_, err = db.Exec(context.Background(), "SELECT prom.set_metric_chunk_interval('test2', INTERVAL '7 hours')")
		if err != nil {
			t.Error(err)
		}
		verifyChunkInterval(t, db, "test2", time.Duration(7*time.Hour))
		_, err = db.Exec(context.Background(), "SELECT prom.set_default_chunk_interval(INTERVAL '6 hours')")
		if err != nil {
			t.Error(err)
		}
		verifyChunkInterval(t, db, "test", time.Duration(6*time.Hour))
		verifyChunkInterval(t, db, "test2", time.Duration(7*time.Hour))
		_, err = db.Exec(context.Background(), "SELECT prom.reset_metric_chunk_interval('test2')")
		if err != nil {
			t.Error(err)
		}
		verifyChunkInterval(t, db, "test2", time.Duration(6*time.Hour))

		//set on a metric that doesn't exist should create the metric and set the parameter
		_, err = db.Exec(context.Background(), "SELECT prom.set_metric_chunk_interval('test_new_metric1', INTERVAL '7 hours')")
		if err != nil {
			t.Error(err)
		}
		verifyChunkInterval(t, db, "test_new_metric1", time.Duration(7*time.Hour))
	})
}

func TestSQLJsonLabelArray(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	testCases := []struct {
		name        string
		metrics     []prompb.TimeSeries
		arrayLength map[string]int
	}{
		{
			name: "One metric",
			metrics: []prompb.TimeSeries{
				{
					Labels: []prompb.Label{
						{Name: "__name__", Value: "metric1"},
						{Name: "test", Value: "test"},
					},
				},
			},
			arrayLength: map[string]int{"metric1": 2},
		},
		{
			name: "Long keys and values",
			metrics: []prompb.TimeSeries{
				{
					Labels: []prompb.Label{
						{Name: "__name__", Value: strings.Repeat("val", 60)},
						{Name: strings.Repeat("key", 60), Value: strings.Repeat("val2", 60)},
					},
				},
			},
		},
		{
			name: "New keys and values",
			metrics: []prompb.TimeSeries{
				{
					Labels: []prompb.Label{
						{Name: "__name__", Value: "metric1"},
						{Name: "test", Value: "test"},
					},
				},
				{
					Labels: []prompb.Label{
						{Name: "__name__", Value: "metric1"},
						{Name: "test1", Value: "test"},
					},
				},
				{
					Labels: []prompb.Label{
						{Name: "__name__", Value: "metric1"},
						{Name: "test", Value: "test"},
						{Name: "test1", Value: "test"},
					},
				},
				{
					Labels: []prompb.Label{
						{Name: "__name__", Value: "metric1"},
						{Name: "test", Value: "val1"},
						{Name: "test1", Value: "val2"},
					},
				},
				{
					Labels: []prompb.Label{
						{Name: "__name__", Value: "metric1"},
						{Name: "test", Value: "test"},
						{Name: "test1", Value: "val2"},
					},
				},
			},
		},
		{
			name: "Multiple metrics",
			metrics: []prompb.TimeSeries{
				{
					Labels: []prompb.Label{
						{Name: "__name__", Value: "m1"},
						{Name: "test1", Value: "val1"},
						{Name: "test2", Value: "val1"},
						{Name: "test3", Value: "val1"},
						{Name: "test4", Value: "val1"},
					},
				},
				{
					Labels: []prompb.Label{
						{Name: "__name__", Value: "m2"},
						{Name: "test", Value: "test"},
					},
				},
				{
					Labels: []prompb.Label{
						{Name: "__name__", Value: "m1"},
						{Name: "test1", Value: "val2"},
						{Name: "test2", Value: "val2"},
						{Name: "test3", Value: "val2"},
						{Name: "test4", Value: "val2"},
					},
				},
				{
					Labels: []prompb.Label{
						{Name: "__name__", Value: "m2"},
						{Name: "test", Value: "test2"},
					},
				},
			},
			//make sure each metric's array is compact
			arrayLength: map[string]int{"m1": 5, "m2": 2},
		},
	}

	for tcIndex, c := range testCases {
		databaseName := fmt.Sprintf("%s_%d", *database, tcIndex)
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			withDB(t, databaseName, func(db *pgxpool.Pool, t *testing.T) {
				for _, ts := range c.metrics {
					labelSet := make(model.LabelSet, len(ts.Labels))
					metricName := ""
					keys := make([]string, 0)
					values := make([]string, 0)
					for _, l := range ts.Labels {
						if l.Name == "__name__" {
							metricName = l.Value
						}
						labelSet[model.LabelName(l.Name)] = model.LabelValue(l.Value)
						keys = append(keys, l.Name)
						values = append(values, l.Value)
					}

					jsonOrig, err := json.Marshal(labelSet)
					if err != nil {
						t.Fatal(err)
					}
					var labelArray []int
					err = db.QueryRow(context.Background(), "SELECT * FROM jsonb_to_label_array($1)", jsonOrig).Scan(&labelArray)
					if err != nil {
						t.Fatal(err)
					}
					if c.arrayLength != nil {
						expected, ok := c.arrayLength[metricName]
						if ok && expected != len(labelArray) {
							t.Fatalf("Unexpected label array length: got\n%v\nexpected\n%v", len(labelArray), expected)
						}
					}

					var labelArrayKV []int
					err = db.QueryRow(context.Background(), "SELECT * FROM key_value_array_to_label_array($1, $2, $3)", metricName, keys, values).Scan(&labelArrayKV)
					if err != nil {
						t.Fatal(err)
					}
					if c.arrayLength != nil {
						expected, ok := c.arrayLength[metricName]
						if ok && expected != len(labelArrayKV) {
							t.Fatalf("Unexpected label array length: got\n%v\nexpected\n%v", len(labelArrayKV), expected)
						}
					}

					if !reflect.DeepEqual(labelArray, labelArrayKV) {
						t.Fatalf("Expected label arrays to be equal: %v != %v", labelArray, labelArrayKV)
					}

					var jsonres []byte
					err = db.QueryRow(context.Background(), "SELECT * FROM label_array_to_jsonb($1)", labelArray).Scan(&jsonres)
					if err != nil {
						t.Fatal(err)
					}
					labelSetRes := make(model.LabelSet, len(ts.Labels))
					err = json.Unmarshal(jsonres, &labelSetRes)
					if err != nil {
						t.Fatal(err)
					}
					if labelSet.Fingerprint() != labelSetRes.Fingerprint() {
						t.Fatalf("Json not equal: got\n%v\nexpected\n%v", string(jsonres), string(jsonOrig))

					}

					// Check the series_id logic
					var seriesID int
					err = db.QueryRow(context.Background(), "SELECT get_series_id_for_label($1)", jsonOrig).Scan(&seriesID)
					if err != nil {
						t.Fatal(err)
					}
					var seriesIDKeyVal int
					err = db.QueryRow(context.Background(), "SELECT get_series_id_for_key_value_array($1, $2, $3)", metricName, keys, values).Scan(&seriesIDKeyVal)
					if err != nil {
						t.Fatal(err)
					}
					if seriesID != seriesIDKeyVal {
						t.Fatalf("Expected the series ids to be equal: %v != %v", seriesID, seriesIDKeyVal)
					}
					err = db.QueryRow(context.Background(), "SELECT label_array_to_jsonb(labels) FROM _prom_catalog.series WHERE id=$1",
						seriesID).Scan(&jsonres)
					if err != nil {
						t.Fatal(err)
					}
					labelSetRes = make(model.LabelSet, len(ts.Labels))
					err = json.Unmarshal(jsonres, &labelSetRes)
					if err != nil {
						t.Fatal(err)
					}
					if labelSet.Fingerprint() != labelSetRes.Fingerprint() {
						t.Fatalf("Json not equal: got\n%v\nexpected\n%v", string(jsonres), string(jsonOrig))

					}
				}
			})
		})
	}
}

func TestSQLIngest(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	testCases := []struct {
		name        string
		metrics     []prompb.TimeSeries
		count       uint64
		countSeries int
		expectErr   error
	}{
		{
			name:    "Zero metrics",
			metrics: []prompb.TimeSeries{},
		},
		{
			name: "One metric",
			metrics: []prompb.TimeSeries{
				{
					Labels: []prompb.Label{
						{Name: metricNameLabelName, Value: "test"},
					},
					Samples: []prompb.Sample{
						{Timestamp: 1, Value: 0.1},
					},
				},
			},
			count:       1,
			countSeries: 1,
		},
		{
			name: "One metric, no sample",
			metrics: []prompb.TimeSeries{
				{
					Labels: []prompb.Label{
						{Name: metricNameLabelName, Value: "test"},
						{Name: "test", Value: "test"},
					},
				},
			},
		},
		{
			name: "Two timeseries",
			metrics: []prompb.TimeSeries{
				{
					Labels: []prompb.Label{
						{Name: metricNameLabelName, Value: "test"},
						{Name: "foo", Value: "bar"},
					},
					Samples: []prompb.Sample{
						{Timestamp: 1, Value: 0.1},
					},
				},
				{
					Labels: []prompb.Label{
						{Name: metricNameLabelName, Value: "test"},
						{Name: "test", Value: "test"},
					},
					Samples: []prompb.Sample{
						{Timestamp: 1, Value: 0.1},
					},
				},
			},
			count:       2,
			countSeries: 2,
		},
		{
			name: "Two samples",
			metrics: []prompb.TimeSeries{
				{
					Labels: []prompb.Label{
						{Name: metricNameLabelName, Value: "test"},
						{Name: "test", Value: "test"},
					},
					Samples: []prompb.Sample{
						{Timestamp: 1, Value: 0.1},
						{Timestamp: 2, Value: 0.2},
					},
				},
			},
			count:       2,
			countSeries: 1,
		},
		{
			name: "Two samples that are complete duplicates",
			metrics: []prompb.TimeSeries{
				{
					Labels: []prompb.Label{
						{Name: metricNameLabelName, Value: "test"},
						{Name: "test", Value: "test"},
					},
					Samples: []prompb.Sample{
						{Timestamp: 1, Value: 0.1},
						{Timestamp: 1, Value: 0.1},
					},
				},
			},
			count:       2,
			countSeries: 1,
		},
		{
			name: "Two timeseries, one series",
			metrics: []prompb.TimeSeries{
				{
					Labels: []prompb.Label{
						{Name: metricNameLabelName, Value: "test"},
						{Name: "test", Value: "test"},
					},
					Samples: []prompb.Sample{
						{Timestamp: 1, Value: 0.1},
					},
				},
				{
					Labels: []prompb.Label{
						{Name: metricNameLabelName, Value: "test"},
						{Name: "test", Value: "test"},
					},
					Samples: []prompb.Sample{
						{Timestamp: 2, Value: 0.2},
					},
				},
			},
			count:       2,
			countSeries: 1,
		},
		{
			name: "Two metric names , one series",
			metrics: []prompb.TimeSeries{
				{
					Labels: []prompb.Label{
						{Name: metricNameLabelName, Value: "test1"},
						{Name: "commonkey", Value: "test"},
						{Name: "key1", Value: "test"},
						{Name: "key2", Value: "val1"},
					},
					Samples: []prompb.Sample{
						{Timestamp: 1, Value: 0.1},
					},
				},
				{
					Labels: []prompb.Label{
						{Name: metricNameLabelName, Value: "test2"},
						{Name: "commonkey", Value: "test"},
						{Name: "key1", Value: "val2"},
						{Name: "key3", Value: "val3"},
					},
					Samples: []prompb.Sample{
						{Timestamp: 2, Value: 0.2},
					},
				},
			},
			count:       2,
			countSeries: 2,
		},
		{
			name: "Missing metric name",
			metrics: []prompb.TimeSeries{
				{
					Samples: []prompb.Sample{
						{Timestamp: 1, Value: 0.1},
					},
				},
			},
			count:       0,
			countSeries: 0,
			expectErr:   errNoMetricName,
		},
	}
	for tcIndex, c := range testCases {
		databaseName := fmt.Sprintf("%s_%d", *database, tcIndex)
		tcase := c
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			withDB(t, databaseName, func(db *pgxpool.Pool, t *testing.T) {
				ingestor := NewPgxIngestor(db)
				cnt, err := ingestor.Ingest(tcase.metrics)
				if cnt != tcase.count {
					t.Fatalf("counts not equal: got %v expected %v\n", cnt, tcase.count)
				}
				if err != nil && err != tcase.expectErr {
					t.Fatalf("got an unexpected error %v", err)
				}

				if err != nil {
					return
				}

				tables := make(map[string]bool)
				for _, ts := range tcase.metrics {
					for _, l := range ts.Labels {
						if l.Name == metricNameLabelName {
							tables[l.Value] = true
						}
					}
				}
				totalRows := 0
				for table := range tables {
					var rowsInTable int
					db.QueryRow(context.Background(), fmt.Sprintf("SELECT count(*) FROM prom.%s", table)).Scan(&rowsInTable)
					totalRows += rowsInTable
				}
				if totalRows != int(cnt) {
					t.Fatalf("counts not equal: got %v expected %v\n", totalRows, cnt)
				}

				var numberSeries int
				db.QueryRow(context.Background(), "SELECT count(*) FROM _prom_catalog.series").Scan(&numberSeries)
				if numberSeries != tcase.countSeries {
					t.Fatalf("unexpected number of series: got %v expected %v\n", numberSeries, tcase.countSeries)
				}
			})
		})
	}
}

func TestMain(m *testing.M) {
	flag.Parse()
	ctx := context.Background()
	if !testing.Short() && *useDocker {
		container, err := startContainer(ctx)
		if err != nil {
			fmt.Println("Error setting up container", err)
			os.Exit(1)
		}
		defer container.Terminate(ctx)
	}
	code := m.Run()
	os.Exit(code)
}

func PGConnectURL(t *testing.T, dbName string) string {
	template := "postgres://postgres:password@%s:%d/%s"
	return fmt.Sprintf(template, pgHost, pgPort.Int(), dbName)
}

func startContainer(ctx context.Context) (testcontainers.Container, error) {
	containerPort := nat.Port("5432/tcp")
	req := testcontainers.ContainerRequest{
		Image:        "timescale/timescaledb:latest-pg11",
		ExposedPorts: []string{string(containerPort)},
		WaitingFor:   wait.NewHostPortStrategy(containerPort),
		Env: map[string]string{
			"POSTGRES_PASSWORD": "password",
		},
	}
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		return nil, err
	}

	pgHost, err = container.Host(ctx)
	if err != nil {
		return nil, err
	}

	pgPort, err = container.MappedPort(ctx, containerPort)
	if err != nil {
		return nil, err
	}

	return container, nil
}

func withDB(t *testing.T, DBName string, f func(db *pgxpool.Pool, t *testing.T)) {
	db := dbSetup(t, DBName)
	defer func() {
		db.Close()
	}()
	performMigrate(t, DBName)
	f(db, t)
}

func performMigrate(t *testing.T, DBName string) {
	dbStd, err := sql.Open("pgx", PGConnectURL(t, DBName))
	defer func() {
		err := dbStd.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()
	if err != nil {
		t.Fatal(err)
	}
	err = Migrate(dbStd)
	if err != nil {
		t.Fatal(err)
	}
}

func dbSetup(t *testing.T, DBName string) *pgxpool.Pool {
	if len(*database) == 0 {
		t.Skip()
	}
	db, err := pgx.Connect(context.Background(), PGConnectURL(t, defaultDB))
	if err != nil {
		t.Fatal(err)
	}

	_, err = db.Exec(context.Background(), fmt.Sprintf("DROP DATABASE IF EXISTS %s", DBName))
	if err != nil {
		t.Fatal(err)
	}

	_, err = db.Exec(context.Background(), fmt.Sprintf("CREATE DATABASE %s", DBName))
	if err != nil {
		t.Fatal(err)
	}
	err = db.Close(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	dbPool, err := pgxpool.Connect(context.Background(), PGConnectURL(t, DBName))
	if err != nil {
		t.Fatal(err)
	}
	return dbPool
}
