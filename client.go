package hdfs

import (
	"io"
	"io/ioutil"
	"os"
	"os/user"

	hdfs "github.com/colinmarc/hdfs/protocol/hadoop_hdfs"
	"github.com/colinmarc/hdfs/rpc"
)

// A Client represents a connection to an HDFS cluster
type Client struct {
	namenode *rpc.NamenodeConnection
	defaults *hdfs.FsServerDefaultsProto
}

// Username returns the value of HADOOP_USER_NAME in the environment, or
// the current system user if it is not set.
func Username() (string, error) {
	username := os.Getenv("HADOOP_USER_NAME")
	if username != "" {
		return username, nil
	}
	currentUser, err := user.Current()
	if err != nil {
		return "", err
	}
	return currentUser.Username, nil
}

// New returns a connected Client, or an error if it can't connect. The user
// will be the user the code is running under. If addresses is an empty slice
// it will try and get the namenode addresses from the hadoop configuration
// files.
func New(addresses []string) (*Client, error) {
	username, err := Username()
	if err != nil {
		return nil, err
	}

	if len(addresses) == 0 {
		var nnErr error
		addresses, nnErr = getNamenodesFromConf()
		if nnErr != nil {
			return nil, nnErr
		}
	}

	return NewForUser(addresses, username)
}

// getNamenodesFromConf returns namenode addresses from the system hadoop
// configuration
func getNamenodesFromConf() ([]string, error) {
	hadoopConf := LoadHadoopConf("")

	namenodes, nnErr := hadoopConf.Namenodes()
	if nnErr != nil {
		return nil, nnErr
	}
	return namenodes, nil
}

// NewForUser returns a connected Client with the user specified, or an error if
// it can't connect.
func NewForUser(addresses []string, user string) (*Client, error) {
	namenode, err := rpc.NewNamenodeConnection(addresses, user)
	if err != nil {
		return nil, err
	}

	return &Client{namenode: namenode}, nil
}

// NewForConnection returns Client with the specified, underlying rpc.NamenodeConnection.
// You can use rpc.WrapNamenodeConnection to wrap your own net.Conn.
func NewForConnection(namenode *rpc.NamenodeConnection) *Client {
	return &Client{namenode: namenode}
}

// ReadFile reads the file named by filename and returns the contents.
func (c *Client) ReadFile(filename string) ([]byte, error) {
	f, err := c.Open(filename)
	if err != nil {
		return nil, err
	}

	defer f.Close()
	return ioutil.ReadAll(f)
}

// CopyToLocal copies the HDFS file specified by src to the local file at dst.
// If dst already exists, it will be overwritten.
func (c *Client) CopyToLocal(src string, dst string) error {
	remote, err := c.Open(src)
	if err != nil {
		return err
	}
	defer remote.Close()

	local, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer local.Close()

	_, err = io.Copy(local, remote)
	return err
}

// CopyToRemote copies the local file specified by src to the HDFS file at dst.
// If dst already exists, it will be overwritten.
func (c *Client) CopyToRemote(src string, dst string) error {
	local, err := os.Open(src)
	if err != nil {
		return err
	}
	defer local.Close()

	remote, err := c.Create(dst)
	if err != nil {
		return err
	}
	defer remote.Close()

	_, err = io.Copy(remote, local)
	return err
}

func (c *Client) fetchDefaults() (*hdfs.FsServerDefaultsProto, error) {
	if c.defaults != nil {
		return c.defaults, nil
	}

	req := &hdfs.GetServerDefaultsRequestProto{}
	resp := &hdfs.GetServerDefaultsResponseProto{}

	err := c.namenode.Execute("getServerDefaults", req, resp)
	if err != nil {
		return nil, err
	}

	c.defaults = resp.GetServerDefaults()
	return c.defaults, nil
}

// Close terminates all underlying socket connections to remote server.
func (c *Client) Close() error {
	return c.namenode.Close()
}
