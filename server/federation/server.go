package federation

import (
	"bytes"
	"context"
	"crypto/rand"
	"fmt"
	"log/slog"
	"net"
	"sync"

	"github.com/mk6i/open-oscar-server/wire"
)

// Server listens for inbound federation peer connections.
type Server struct {
	listenAddr     string
	manager        *Manager
	logger         *slog.Logger
	listener       net.Listener
	closed         chan struct{}
	shutdownCtx    context.Context
	shutdownCancel context.CancelFunc
	connWg         sync.WaitGroup
}

// NewServer creates a new federation server.
func NewServer(listenAddr string, manager *Manager, logger *slog.Logger) *Server {
	ctx, cancel := context.WithCancel(context.Background())
	return &Server{
		listenAddr:     listenAddr,
		manager:        manager,
		logger:         logger.With("svc", "Federation"),
		closed:         make(chan struct{}),
		shutdownCtx:    ctx,
		shutdownCancel: cancel,
	}
}

// ListenAndServe starts the federation listener and accepts peer connections.
func (s *Server) ListenAndServe() error {
	ln, err := net.Listen("tcp", s.listenAddr)
	if err != nil {
		s.shutdownCancel()
		return fmt.Errorf("federation listen: %w", err)
	}
	s.listener = ln

	s.logger.Info("federation listener started", "addr", s.listenAddr)

	go func() {
		<-s.shutdownCtx.Done()
		ln.Close()
	}()

	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				if s.shutdownCtx.Err() != nil {
					break
				}
				s.logger.Error("federation accept error", "err", err)
				continue
			}
			s.connWg.Add(1)
			go func() {
				defer s.connWg.Done()
				s.handleConnection(conn)
			}()
		}
	}()

	<-s.closed
	return nil
}

// Shutdown gracefully shuts down the federation server.
func (s *Server) Shutdown(_ context.Context) error {
	s.shutdownCancel()
	if s.listener != nil {
		s.listener.Close()
	}
	s.connWg.Wait()
	close(s.closed)
	return nil
}

func (s *Server) handleConnection(conn net.Conn) {
	defer conn.Close()

	flapc := wire.NewFlapClient(200, conn, conn)

	networkName, err := s.performInboundAuth(flapc)
	if err != nil {
		s.logger.Error("federation inbound auth failed",
			"remote", conn.RemoteAddr(),
			"err", err,
		)
		return
	}

	s.logger.Info("federation inbound peer authenticated",
		"peer", networkName,
		"remote", conn.RemoteAddr(),
	)

	if err := s.manager.RegisterInboundPeer(s.shutdownCtx, networkName, conn, flapc); err != nil {
		s.logger.Error("failed to register inbound peer",
			"peer", networkName,
			"err", err,
		)
	}
}

func (s *Server) performInboundAuth(flapc *wire.FlapClient) (string, error) {
	// Generate our challenge
	var challenge [32]byte
	if _, err := rand.Read(challenge[:]); err != nil {
		return "", fmt.Errorf("generating challenge: %w", err)
	}

	// Receive peer's auth request
	flap, err := flapc.ReceiveFLAP()
	if err != nil {
		return "", fmt.Errorf("receiving auth request: %w", err)
	}

	var peerFrame wire.SNACFrame
	var peerAuthReq wire.SNAC_0x0100_0x0001_FedAuthRequest
	buf := bytes.NewBuffer(flap.Payload)
	if err := wire.UnmarshalBE(&peerFrame, buf); err != nil {
		return "", fmt.Errorf("unmarshalling peer frame: %w", err)
	}
	if err := wire.UnmarshalBE(&peerAuthReq, buf); err != nil {
		return "", fmt.Errorf("unmarshalling peer auth request: %w", err)
	}

	peerNetworkName := peerAuthReq.NetworkName

	// Look up peer config
	peerCfg, ok := s.manager.PeerConfigByName(peerNetworkName)
	if !ok {
		flapc.SendSNAC(wire.SNACFrame{
			FoodGroup: wire.Federation,
			SubGroup:  wire.FedAuthResult,
		}, wire.SNAC_0x0100_0x0003_FedAuthResult{
			Code: wire.FedAuthResultUnknown,
		})
		return "", fmt.Errorf("unknown peer: %s", peerNetworkName)
	}

	// Send our auth request (with our challenge)
	if err := flapc.SendSNAC(wire.SNACFrame{
		FoodGroup: wire.Federation,
		SubGroup:  wire.FedAuthRequest,
	}, wire.SNAC_0x0100_0x0001_FedAuthRequest{
		NetworkName: s.manager.LocalNetwork(),
		Challenge:   challenge,
		Version:     federationProtocolVersion,
	}); err != nil {
		return "", fmt.Errorf("sending auth request: %w", err)
	}

	// Compute and send our digest for their challenge
	digest := computeHMAC(peerAuthReq.Challenge[:], s.manager.LocalNetwork(), peerCfg.Secret)
	if err := flapc.SendSNAC(wire.SNACFrame{
		FoodGroup: wire.Federation,
		SubGroup:  wire.FedAuthResponse,
	}, wire.SNAC_0x0100_0x0002_FedAuthResponse{
		NetworkName: s.manager.LocalNetwork(),
		Digest:      digest,
	}); err != nil {
		return "", fmt.Errorf("sending auth response: %w", err)
	}

	// Receive peer's auth response
	flap, err = flapc.ReceiveFLAP()
	if err != nil {
		return "", fmt.Errorf("receiving auth response: %w", err)
	}

	var respFrame wire.SNACFrame
	var peerAuthResp wire.SNAC_0x0100_0x0002_FedAuthResponse
	buf = bytes.NewBuffer(flap.Payload)
	if err := wire.UnmarshalBE(&respFrame, buf); err != nil {
		return "", fmt.Errorf("unmarshalling response frame: %w", err)
	}
	if err := wire.UnmarshalBE(&peerAuthResp, buf); err != nil {
		return "", fmt.Errorf("unmarshalling auth response: %w", err)
	}

	// Verify their digest
	expectedDigest := computeHMAC(challenge[:], peerNetworkName, peerCfg.Secret)
	if peerAuthResp.Digest != expectedDigest {
		flapc.SendSNAC(wire.SNACFrame{
			FoodGroup: wire.Federation,
			SubGroup:  wire.FedAuthResult,
		}, wire.SNAC_0x0100_0x0003_FedAuthResult{
			Code: wire.FedAuthResultFailed,
		})
		return "", fmt.Errorf("auth failed: invalid digest from peer %q", peerNetworkName)
	}

	// Send success
	if err := flapc.SendSNAC(wire.SNACFrame{
		FoodGroup: wire.Federation,
		SubGroup:  wire.FedAuthResult,
	}, wire.SNAC_0x0100_0x0003_FedAuthResult{
		Code: wire.FedAuthResultSuccess,
	}); err != nil {
		return "", fmt.Errorf("sending auth result: %w", err)
	}

	// Receive peer's auth result
	flap, err = flapc.ReceiveFLAP()
	if err != nil {
		return "", fmt.Errorf("receiving auth result: %w", err)
	}

	var resultFrame wire.SNACFrame
	var authResult wire.SNAC_0x0100_0x0003_FedAuthResult
	buf = bytes.NewBuffer(flap.Payload)
	if err := wire.UnmarshalBE(&resultFrame, buf); err != nil {
		return "", fmt.Errorf("unmarshalling result frame: %w", err)
	}
	if err := wire.UnmarshalBE(&authResult, buf); err != nil {
		return "", fmt.Errorf("unmarshalling auth result: %w", err)
	}
	if authResult.Code != wire.FedAuthResultSuccess {
		return "", fmt.Errorf("auth rejected by peer: code %d", authResult.Code)
	}

	return peerNetworkName, nil
}
