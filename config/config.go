package config

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"strings"
)

var (
	// Simple error for duplicate listener definitions
	errDuplicateListener = errors.New("duplicate listener definition")
	// Simple error for missing BOS listeners
	errNoBOSListeners = errors.New("at least one BOS listener is required")
)

// Custom error types for URI-related errors
type uriFormatError struct {
	URI string
	Err error
}

func (e uriFormatError) Error() string {
	return fmt.Sprintf("invalid listener URI %q: %v. Valid format: SCHEME://HOST:PORT (e.g., LOCAL://0.0.0.0:5190)", e.URI, e.Err)
}

type Build struct {
	Version string `json:"version"`
	Commit  string `json:"commit"`
	Date    string `json:"date"`
}

type Listener struct {
	BOSListenAddress       string
	BOSAdvertisedHostPlain string
	BOSAdvertisedHostSSL   string
	KerberosListenAddress  string
	HasSSL                 bool
}

//go:generate go run ../cmd/config_generator unix settings.env ssl
type Config struct {
	BOSListeners            []string `envconfig:"OSCAR_LISTENERS" required:"true" basic:"LOCAL://0.0.0.0:5190" ssl:"LOCAL://0.0.0.0:5190" description:"Network listeners for core OSCAR services. For multi-homed servers, allows users to connect from multiple networks. For example, you can allow both LAN and Internet clients to connect to the same server using different connection settings.\n\nFormat:\n\t- Comma-separated list of [NAME]://[HOSTNAME]:[PORT]\n\t- Listener names and ports must be unique\n\t- Listener names are user-defined\n\t- Each listener needs a listener in OSCAR_ADVERTISED_LISTENERS_PLAIN\n\nExamples:\n\t// Listen on all interfaces\n\tLAN://0.0.0.0:5190\n\t// Separate Internet and LAN config\n\tWAN://142.250.176.206:5190,LAN://192.168.1.10:5191"`
	BOSAdvertisedHostsPlain []string `envconfig:"OSCAR_ADVERTISED_LISTENERS_PLAIN" required:"true" basic:"LOCAL://127.0.0.1:5190" ssl:"LOCAL://127.0.0.1:5190" description:"Hostnames published by the server that clients connect to for accessing various OSCAR services. These hostnames are NOT the bind addresses. For multi-homed use servers, allows clients to connect using separate hostnames per network.\n\nFormat:\n\t- Comma-separated list of [NAME]://[HOSTNAME]:[PORT]\n\t- Each listener config must correspond to a config in OSCAR_LISTENERS\n\t- Clients MUST be able to connect to these hostnames\n\nExamples:\n\t// Local LAN config, server behind NAT\n\tLAN://192.168.1.10:5190\n\t// Separate Internet and LAN config\n\tWAN://aim.example.com:5190,LAN://192.168.1.10:5191"`
	BOSAdvertisedHostsSSL   []string `envconfig:"OSCAR_ADVERTISED_LISTENERS_SSL" required:"false" basic:"" ssl:"LOCAL://ras.dev:5193" description:"Same as OSCAR_ADVERTISED_LISTENERS_PLAIN, except the hostname is for the server that terminates SSL."`
	KerberosListeners       []string `envconfig:"KERBEROS_LISTENERS" required:"false" basic:"" ssl:"LOCAL://0.0.0.0:1088" description:"Network listeners for Kerberos authentication. See OSCAR_LISTENERS doc for more details.\n\nExamples:\n\t// Listen on all interfaces\n\tLAN://0.0.0.0:1088\n\t// Separate Internet and LAN config\n\tWAN://142.250.176.206:1088,LAN://192.168.1.10:1087"`
	TOCListeners            []string `envconfig:"TOC_LISTENERS" required:"true" basic:"0.0.0.0:9898" ssl:"0.0.0.0:9898" description:"Network listeners for TOC protocol service.\n\nFormat: Comma-separated list of hostname:port pairs.\n\nExamples:\n\t// All interfaces\n\t0.0.0.0:9898\n\t// Multiple listeners\n\t0.0.0.0:9898,192.168.1.10:9899"`
	APIListener             string   `envconfig:"API_LISTENER" required:"true" basic:"127.0.0.1:8080" ssl:"127.0.0.1:8080" description:"Network listener for management API binds to. Only 1 listener can be specified. (Default 127.0.0.1 restricts to same machine only)."`

	DBPath                 string `envconfig:"DB_PATH" required:"true" basic:"oscar.sqlite" ssl:"oscar.sqlite" description:"The path to the SQLite database file. The file and DB schema are auto-created if they doesn't exist."`
	DisableAuth            bool   `envconfig:"DISABLE_AUTH" required:"true" basic:"true" ssl:"true" description:"Disable password check and auto-create new users at login time. Useful for quickly creating new accounts during development without having to register new users via the management API."`
	DisableMultiLoginNotif bool   `envconfig:"DISABLE_MULTI_LOGIN_NOTIF" required:"false" basic:"true" ssl:"true" description:"Disable notification sent when another client signs in with the same screen name."`
	LogLevel               string `envconfig:"LOG_LEVEL" required:"true" basic:"info" ssl:"info" description:"Set logging granularity. Possible values: 'trace', 'debug', 'info', 'warn', 'error'."`

	FederationNetworkName string   `envconfig:"FEDERATION_NETWORK_NAME" required:"false" description:"Unique network name for this server instance used in federation. When set, enables federation support. Users on federated servers are addressed as screenname@networkname (e.g., user@retra.im)."`
	FederationListener    string   `envconfig:"FEDERATION_LISTENER" required:"false" description:"Network listener address for incoming federation peer connections.\n\nFormat: HOST:PORT\n\nExample: 0.0.0.0:5195"`
	FederationPeers       []string `envconfig:"FEDERATION_PEERS" required:"false" description:"Comma-separated list of federation peer definitions.\n\nFormat: NAME@HOST:PORT:SECRET\n\nExample: chivanet.org@peering.chivanet.org:5195:mysharedsecret"`

	FederationGossipInterval int `envconfig:"FEDERATION_GOSSIP_INTERVAL" required:"false" basic:"2" ssl:"2" description:"Interval in seconds between gossip rounds for federation state synchronization. Lower values converge faster but generate more traffic."`
	FederationMaxTTL         int `envconfig:"FEDERATION_MAX_TTL" required:"false" basic:"10" ssl:"10" description:"Maximum hop count for forwarded federation messages. Messages exceeding this TTL are dropped to prevent routing loops."`
}

// FederationPeerConfig holds the parsed configuration for a single federation peer.
type FederationPeerConfig struct {
	NetworkName string
	Address     string // host:port
	Secret      string
}

// FederationEnabled returns true if federation is configured.
func (c *Config) FederationEnabled() bool {
	return c.FederationNetworkName != ""
}

// ParseFederationPeers parses the FEDERATION_PEERS configuration entries into
// FederationPeerConfig structs. Each entry has the format NAME@HOST:PORT:SECRET.
func (c *Config) ParseFederationPeers() ([]FederationPeerConfig, error) {
	var peers []FederationPeerConfig
	seen := make(map[string]bool)

	for _, raw := range c.FederationPeers {
		raw = strings.TrimSpace(raw)
		if raw == "" {
			continue
		}

		atIdx := strings.Index(raw, "@")
		if atIdx < 1 {
			return nil, fmt.Errorf("invalid federation peer %q: missing network name before '@'. Format: NAME@HOST:PORT:SECRET", raw)
		}
		networkName := strings.ToLower(raw[:atIdx])
		rest := raw[atIdx+1:]

		lastColon := strings.LastIndex(rest, ":")
		if lastColon < 0 {
			return nil, fmt.Errorf("invalid federation peer %q: missing secret. Format: NAME@HOST:PORT:SECRET", raw)
		}
		secret := rest[lastColon+1:]
		hostPort := rest[:lastColon]

		if secret == "" {
			return nil, fmt.Errorf("invalid federation peer %q: secret cannot be empty", raw)
		}

		host, port, err := net.SplitHostPort(hostPort)
		if err != nil {
			return nil, fmt.Errorf("invalid federation peer %q: invalid address %q: %v", raw, hostPort, err)
		}
		if host == "" || port == "" {
			return nil, fmt.Errorf("invalid federation peer %q: host and port are required", raw)
		}

		if seen[networkName] {
			return nil, fmt.Errorf("duplicate federation peer network name: %s", networkName)
		}
		seen[networkName] = true

		if strings.EqualFold(networkName, c.FederationNetworkName) {
			return nil, fmt.Errorf("federation peer network name %q collides with local network name", networkName)
		}

		peers = append(peers, FederationPeerConfig{
			NetworkName: networkName,
			Address:     net.JoinHostPort(host, port),
			Secret:      secret,
		})
	}

	return peers, nil
}

func (c *Config) ParseListenersCfg() ([]Listener, error) {
	// Helper function to parse and validate a single URI
	parseURI := func(uriStr string) (*url.URL, error) {
		uriStr = strings.TrimSpace(uriStr)
		if uriStr == "" {
			return nil, nil
		}

		u, err := url.Parse(uriStr)
		if err != nil {
			return nil, uriFormatError{URI: uriStr, Err: err}
		}
		switch {
		case u.Scheme == "":
			return nil, uriFormatError{URI: uriStr, Err: errors.New("missing scheme")}
		case u.Hostname() == "":
			return nil, uriFormatError{URI: uriStr, Err: errors.New("missing host")}
		case u.Port() == "":
			return nil, uriFormatError{URI: uriStr, Err: errors.New("missing port")}
		}

		return u, nil
	}

	m := make(map[string]*Listener)

	// Parse BOS listeners
	for _, uriStr := range c.BOSListeners {
		u, err := parseURI(uriStr)
		if err != nil {
			return nil, err
		}
		if u == nil {
			continue
		}

		if _, ok := m[u.Scheme]; !ok {
			m[u.Scheme] = &Listener{}
		}
		if m[u.Scheme].BOSListenAddress != "" {
			return nil, errDuplicateListener
		}
		m[u.Scheme].BOSListenAddress = net.JoinHostPort(u.Hostname(), u.Port())
	}

	// Parse plaintext BOS advertised listeners
	for _, uriStr := range c.BOSAdvertisedHostsPlain {
		u, err := parseURI(uriStr)
		if err != nil {
			return nil, err
		}
		if u == nil {
			continue
		}

		if _, ok := m[u.Scheme]; !ok {
			m[u.Scheme] = &Listener{}
		}
		if m[u.Scheme].BOSAdvertisedHostPlain != "" {
			return nil, errDuplicateListener
		}
		m[u.Scheme].BOSAdvertisedHostPlain = net.JoinHostPort(u.Hostname(), u.Port())
	}

	// Parse SSL BOS advertised listeners
	for _, uriStr := range c.BOSAdvertisedHostsSSL {
		u, err := parseURI(uriStr)
		if err != nil {
			return nil, err
		}
		if u == nil {
			continue
		}

		if _, ok := m[u.Scheme]; !ok {
			m[u.Scheme] = &Listener{}
		}
		if m[u.Scheme].BOSAdvertisedHostSSL != "" {
			return nil, errDuplicateListener
		}
		m[u.Scheme].HasSSL = true
		m[u.Scheme].BOSAdvertisedHostSSL = net.JoinHostPort(u.Hostname(), u.Port())
	}

	// Parse Kerberos listeners
	for _, uriStr := range c.KerberosListeners {
		u, err := parseURI(uriStr)
		if err != nil {
			return nil, err
		}
		if u == nil {
			continue
		}

		if _, ok := m[u.Scheme]; !ok {
			m[u.Scheme] = &Listener{}
		}
		if m[u.Scheme].KerberosListenAddress != "" {
			return nil, errDuplicateListener
		}
		m[u.Scheme].KerberosListenAddress = net.JoinHostPort(u.Hostname(), u.Port())
	}

	ret := make([]Listener, 0, len(m))

	for k, v := range m {
		switch {
		case v.BOSAdvertisedHostPlain == "":
			return nil, fmt.Errorf("missing BOS advertise address for listener `%s://`", k)
		case v.BOSListenAddress == "":
			return nil, fmt.Errorf("missing BOS listen address for listener `%s://`", k)
		}
		ret = append(ret, *v)
	}

	if len(ret) == 0 {
		return nil, errNoBOSListeners
	}

	return ret, nil
}

func (c *Config) Validate() error {
	// Validate TOCListeners (format: hostname:port pairs)
	for _, listener := range c.TOCListeners {
		listener = strings.TrimSpace(listener)
		if listener == "" {
			continue
		}

		host, port, err := net.SplitHostPort(listener)
		if err != nil {
			return fmt.Errorf("invalid TOC listener %q: %v. Valid format: HOST:PORT (e.g., 0.0.0.0:9898)", listener, err)
		}

		if host == "" {
			return fmt.Errorf("invalid TOC listener %q: missing host. Valid format: HOST:PORT (e.g., 0.0.0.0:9898)", listener)
		}

		if port == "" {
			return fmt.Errorf("invalid TOC listener %q: missing port. Valid format: HOST:PORT (e.g., 0.0.0.0:9898)", listener)
		}
	}

	// Validate APIListener (format: hostname:port pair, no scheme)
	apiListener := strings.TrimSpace(c.APIListener)
	if apiListener == "" {
		return fmt.Errorf("APIListener is required and cannot be empty")
	}

	host, port, err := net.SplitHostPort(apiListener)
	if err != nil {
		return fmt.Errorf("invalid API listener %q: %v. Valid format: HOST:PORT (e.g., 127.0.0.1:8080)", c.APIListener, err)
	}

	if host == "" {
		return fmt.Errorf("invalid API listener %q: missing host. Valid format: HOST:PORT (e.g., 127.0.0.1:8080)", c.APIListener)
	}

	if port == "" {
		return fmt.Errorf("invalid API listener %q: missing port. Valid format: HOST:PORT (e.g., 127.0.0.1:8080)", c.APIListener)
	}

	// Validate federation config
	if c.FederationNetworkName != "" {
		if len(c.FederationPeers) > 0 && c.FederationListener == "" {
			return fmt.Errorf("FEDERATION_LISTENER is required when FEDERATION_PEERS are configured")
		}
		if c.FederationListener != "" {
			fHost, fPort, err := net.SplitHostPort(c.FederationListener)
			if err != nil {
				return fmt.Errorf("invalid federation listener %q: %v", c.FederationListener, err)
			}
			if fHost == "" || fPort == "" {
				return fmt.Errorf("invalid federation listener %q: host and port are required", c.FederationListener)
			}
		}
		if _, err := c.ParseFederationPeers(); err != nil {
			return err
		}
	}

	return nil
}
