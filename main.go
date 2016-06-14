package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/HolmesProcessing/Holmes-Presentation/context"
	"github.com/HolmesProcessing/Holmes-Presentation/listners/http"

	"github.com/gocql/gocql"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

type DBConnector struct {
	IP       string
	Port     int
	User     string
	Password string
	Database string
}

type ObjDBConnector struct {
	IP         string
	Port       int
	Region     string
	Key        string
	Secret     string
	Bucket     string
	DisableSSL bool
}

type config struct {
	Storage     string
	Database    []*DBConnector
	ObjStorage  string
	ObjDatabase []*ObjDBConnector
	LogFile     string
	LogLevel    string

	AMQP          string
	Queue         string
	RoutingKey    string
	PrefetchCount int

	HTTP string
}

func main() {
	var (
		confPath string
		err      error
	)

	ctx := &context.Ctx{}
	ctx.SetLogging("", "debug")

	// load config
	flag.StringVar(&confPath, "config", "", "Path to the config file")
	flag.Parse()

	if confPath == "" {
		confPath, _ = filepath.Abs(filepath.Dir(os.Args[0]))
		confPath += "/config.json"
	}

	conf := &config{}
	cfile, _ := os.Open(confPath)
	if err = json.NewDecoder(cfile).Decode(&conf); err != nil {
		ctx.Warning.Panicln("Couldn't decode config file without errors!", err.Error())
	}

	// reload logging with parameters from config
	ctx.SetLogging(conf.LogFile, conf.LogLevel)

	// connect to Cassandra
	ctx.C, err = initCassandra(conf.Database)
	if err != nil {
		ctx.Warning.Panicln("Cassandra initialization failed!", err.Error())
	}
	ctx.Info.Println("Connected to Cassandra:", conf.Storage)

	// connect to S3
	ctx.S3, ctx.Bucket, err = initS3(conf.ObjDatabase)
	if err != nil {
		ctx.Warning.Panicln("S3 initialization failed!", err.Error())
	}
	ctx.Info.Println("Connected to S3:", conf.ObjStorage)

	// start HTTP server
	http.Start(ctx, conf.HTTP)

	// start to listen via AMQP
	//initAMQP(conf.AMQP, conf.Queue, conf.RoutingKey, conf.PrefetchCount)
}

func initCassandra(c []*DBConnector) (*gocql.Session, error) {
	if len(c) < 1 {
		return nil, errors.New("Supply at least one node to connect to!")
	}

	connStrings := make([]string, len(c))
	for i, elem := range c {
		connStrings[i] = fmt.Sprintf("%s:%d", elem.IP, elem.Port)
	}

	if c[0].Database == "" {
		return nil, errors.New("Please supply a database/keyspace to use!")
	}

	cluster := gocql.NewCluster(connStrings...)
	cluster.Authenticator = gocql.PasswordAuthenticator{
		Username: c[0].User,
		Password: c[0].Password,
	}
	cluster.ProtoVersion = 4
	cluster.Timeout = time.Minute * 5
	cluster.Keyspace = c[0].Database
	cluster.Consistency = gocql.Quorum
	return cluster.CreateSession()
}

func initS3(c []*ObjDBConnector) (*s3.S3, string, error) {
	if len(c) < 1 {
		return nil, "", errors.New("Supply at least one node to connect to!")
	}

	conn := s3.New(session.New(&aws.Config{
		Credentials: credentials.NewStaticCredentials(
			c[0].Key,
			c[0].Secret,
			""),
		Endpoint:         aws.String(c[0].IP + ":" + strconv.Itoa(c[0].Port)),
		Region:           aws.String(c[0].Region),
		S3ForcePathStyle: aws.Bool(true),
		DisableSSL:       aws.Bool(c[0].DisableSSL),
	}))

	// since there is no definit way to test the connection
	// we are just doint a dummy request here to see if the
	// connection is stable
	_, err := conn.ListBuckets(&s3.ListBucketsInput{})

	return conn, c[0].Bucket, err
}
