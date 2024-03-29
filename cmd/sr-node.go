package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"runtime"

	"github.com/golang/glog"
	"wwwin-github.cisco.com/brmcdoug/sr-node/pkg/arangodb"
	"wwwin-github.cisco.com/brmcdoug/sr-node/pkg/kafkamessenger"

	_ "net/http/pprof"
)

const (
	// userFile defines the name of file containing base64 encoded user name
	userFile = "./credentials/.username"
	// passFile defines the name of file containing base64 encoded password
	passFile = "./credentials/.password"
	// MAXUSERNAME defines maximum length of ArangoDB user name
	MAXUSERNAME = 256
	// MAXPASS defines maximum length of ArangoDB password
	MAXPASS = 256
)

var (
	msgSrvAddr string
	dbSrvAddr  string
	dbName     string
	dbUser     string
	dbPass     string
	lsprefix   string
	lssrv6sid  string
	lsnode     string
	srnode     string
)

func init() {
	runtime.GOMAXPROCS(1)
	// flag.StringVar(&msgSrvAddr, "message-server", "", "URL to the messages supplying server")
	// flag.StringVar(&dbSrvAddr, "database-server", "", "{dns name}:port or X.X.X.X:port of the graph database")
	// flag.StringVar(&dbName, "database-name", "", "DB name")
	// flag.StringVar(&dbUser, "database-user", "", "DB User name")
	// flag.StringVar(&dbPass, "database-pass", "", "DB User's password")
	flag.StringVar(&msgSrvAddr, "message-server", "10.200.99.27:30092", "URL to the messages supplying server")
	flag.StringVar(&dbSrvAddr, "database-server", "http://10.200.99.27:32748", "{dns name}:port or X.X.X.X:port of the graph database")
	flag.StringVar(&dbName, "database-name", "jalapeno", "DB name")
	flag.StringVar(&dbUser, "database-user", "root", "DB User name")
	flag.StringVar(&dbPass, "database-pass", "jalapeno", "DB User's password")

	flag.StringVar(&lsprefix, "ls_prefix", "ls_prefix", "ls_prefix Collection name, default: \"ls_prefix\"")
	flag.StringVar(&lssrv6sid, "ls_srv6_sid", "ls_srv6_sid", "ls_srv6_sid Collection name, default: \"ls_srv6_sid\"")
	flag.StringVar(&lsnode, "ls_node", "ls_node", "ls_node Collection name, default \"ls_node\"")
	flag.StringVar(&srnode, "sr_node", "sr_node", "sr_node Collection name, default \"sr_node\"")
}

var (
	onlyOneSignalHandler = make(chan struct{})
	shutdownSignals      = []os.Signal{os.Interrupt}
)

func setupSignalHandler() (stopCh <-chan struct{}) {
	close(onlyOneSignalHandler) // panics when called twice

	stop := make(chan struct{})
	c := make(chan os.Signal, 2)
	signal.Notify(c, shutdownSignals...)
	go func() {
		<-c
		close(stop)
		<-c
		os.Exit(1) // second signal. Exit directly.
	}()

	return stop
}

func main() {
	flag.Parse()
	_ = flag.Set("logtostderr", "true")

	// validateDBCreds check if the user name and the password are provided either as
	// command line parameters or via files. If both are provided command line parameters
	// will be used, if neither, processor will fail.
	if err := validateDBCreds(); err != nil {
		glog.Errorf("failed to validate the database credentials with error: %+v", err)
		os.Exit(1)
	}
	dbSrv, err := arangodb.NewDBSrvClient(dbSrvAddr, dbUser, dbPass, dbName, lsprefix, lssrv6sid, lsnode, srnode)
	if err != nil {
		glog.Errorf("failed to initialize database client with error: %+v", err)
		os.Exit(1)
	}

	if err := dbSrv.Start(); err != nil {
		if err != nil {
			glog.Errorf("failed to connect to database with error: %+v", err)
			os.Exit(1)
		}
	}

	// Initializing messenger process
	msgSrv, err := kafkamessenger.NewKafkaMessenger(msgSrvAddr, dbSrv.GetInterface())
	if err != nil {
		glog.Errorf("failed to initialize message server with error: %+v", err)
		os.Exit(1)
	}

	msgSrv.Start()

	stopCh := setupSignalHandler()
	<-stopCh

	msgSrv.Stop()
	dbSrv.Stop()

	os.Exit(0)
}

func validateDBCreds() error {
	// Attempting to access username and password files.
	u, err := readAndDecode(userFile, MAXUSERNAME)
	if err != nil {
		if dbUser != "" && dbPass != "" {
			return nil
		}
		return fmt.Errorf("failed to access %s with error: %+v and no username and password provided via command line arguments", userFile, err)
	}
	p, err := readAndDecode(passFile, MAXPASS)
	if err != nil {
		if dbUser != "" && dbPass != "" {
			return nil
		}
		return fmt.Errorf("failed to access %s with error: %+v and no username and password provided via command line arguments", passFile, err)
	}
	dbUser, dbPass = u, p

	return nil
}

func readAndDecode(fn string, max int) (string, error) {
	f, err := os.Open(fn)
	if err != nil {
		return "", err
	}
	defer f.Close()
	l, err := f.Stat()
	if err != nil {
		return "", err
	}
	b := make([]byte, int(l.Size()))
	n, err := io.ReadFull(f, b)
	if err != nil {
		return "", err
	}
	if n > max {
		return "", fmt.Errorf("length of data %d exceeds maximum acceptable length: %d", n, max)
	}
	b = b[:n]

	return string(b), nil
}
