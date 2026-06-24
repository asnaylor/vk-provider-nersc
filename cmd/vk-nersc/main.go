package main

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"io"
	"log"
	"math/big"
	"net"
	"net/http"
	"os"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	corev1client "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/record"

	"github.com/virtual-kubelet/virtual-kubelet/node"
	"vk-provider-nersc/pkg/provider"
)

func main() {
	endpoint := os.Getenv("SF_API_ENDPOINT")
	nodeName := os.Getenv("VK_NODE_NAME")
	if nodeName == "" {
		nodeName = "perlmutter-vk"
	}
	nodeAddress := firstNonEmpty(os.Getenv("VK_NODE_IP"), os.Getenv("POD_IP"), "127.0.0.1")
	kubeletListenAddr := firstNonEmpty(os.Getenv("VK_KUBELET_LISTEN_ADDR"), ":10250")

	// Create Kubernetes client
	config, err := rest.InClusterConfig()
	if err != nil {
		// Fallback to kubeconfig for local development
		kubeconfig := os.Getenv("KUBECONFIG")
		if kubeconfig == "" {
			kubeconfig = os.Getenv("HOME") + "/.kube/config"
		}
		config, err = clientcmd.BuildConfigFromFlags("", kubeconfig)
		if err != nil {
			log.Fatalf("Failed to create Kubernetes config: %v", err)
		}
	}

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		log.Fatalf("Failed to create Kubernetes client: %v", err)
	}

	prov, err := provider.NewNerscProvider(endpoint, nodeName, provider.NewSecretTokenResolver(clientset.CoreV1()))
	if err != nil {
		log.Fatalf("Failed to create provider: %v", err)
	}
	prov.SetNodeAddress(nodeAddress)

	// Create the virtual node
	virtualNode := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:   nodeName,
			Labels: provider.VirtualNodeLabels(nodeName),
		},
		Spec: corev1.NodeSpec{
			Taints: []corev1.Taint{
				{
					Key:    "virtual-kubelet.io/provider",
					Value:  "nersc",
					Effect: corev1.TaintEffectNoSchedule,
				},
			},
		},
	}

	ctx := context.Background()
	eventBroadcaster := record.NewBroadcaster()
	defer eventBroadcaster.Shutdown()
	eventBroadcaster.StartLogging(log.Printf)
	eventBroadcaster.StartRecordingToSink(&corev1client.EventSinkImpl{Interface: clientset.CoreV1().Events("")})
	recorder := eventBroadcaster.NewRecorder(scheme.Scheme, corev1.EventSource{Component: "vk-nersc/pod-controller"})

	kubeletServer, err := startKubeletAPI(ctx, kubeletListenAddr, nodeName, nodeAddress, prov)
	if err != nil {
		log.Fatalf("Failed to start kubelet API: %v", err)
	}
	defer kubeletServer.Close()
	log.Printf("Started kubelet-compatible API on %s, advertising node address %s", kubeletListenAddr, nodeAddress)

	podInformerFactory := informers.NewSharedInformerFactoryWithOptions(
		clientset,
		0,
		informers.WithTweakListOptions(func(options *metav1.ListOptions) {
			options.FieldSelector = fields.OneTermEqualSelector("spec.nodeName", nodeName).String()
		}),
	)
	podInformer := podInformerFactory.Core().V1().Pods()

	resourceInformerFactory := informers.NewSharedInformerFactory(clientset, 0)
	secretInformer := resourceInformerFactory.Core().V1().Secrets()
	configMapInformer := resourceInformerFactory.Core().V1().ConfigMaps()
	serviceInformer := resourceInformerFactory.Core().V1().Services()

	// Create and run the virtual kubelet node controller
	nodeController, err := node.NewNodeController(
		prov,
		virtualNode,
		clientset.CoreV1().Nodes(),
	)
	if err != nil {
		log.Fatalf("Failed to create node controller: %v", err)
	}

	podController, err := node.NewPodController(node.PodControllerConfig{
		PodClient:         clientset.CoreV1(),
		EventRecorder:     recorder,
		Provider:          prov,
		PodInformer:       podInformer,
		SecretInformer:    secretInformer,
		ConfigMapInformer: configMapInformer,
		ServiceInformer:   serviceInformer,
		PodEventFilterFunc: func(ctx context.Context, pod *corev1.Pod) bool {
			return pod != nil && pod.Spec.NodeName == nodeName && provider.HasSuperfacilityCredentials(pod)
		},
	})
	if err != nil {
		log.Fatalf("Failed to create pod controller: %v", err)
	}

	podInformerFactory.Start(ctx.Done())
	resourceInformerFactory.Start(ctx.Done())

	errCh := make(chan error, 2)
	go func() {
		errCh <- podController.Run(ctx, 1)
	}()
	select {
	case <-podController.Ready():
		log.Printf("Virtual Kubelet pod controller is ready")
	case <-podController.Done():
		log.Fatalf("Pod controller exited before becoming ready: %v", podController.Err())
	}

	log.Printf("Starting Virtual Kubelet node %s for Perlmutter...", nodeName)
	go func() {
		errCh <- nodeController.Run(ctx)
	}()
	if err := <-errCh; err != nil {
		log.Fatalf("VK exited: %v", err)
	}
	log.Fatalf("VK controller exited")
}

func startKubeletAPI(ctx context.Context, listenAddr, nodeName, nodeAddress string, prov *provider.NerscProvider) (*http.Server, error) {
	mux := http.NewServeMux()
	mux.HandleFunc("/containerLogs/", handleContainerLogs(prov))

	cert, err := selfSignedServingCert(nodeName, nodeAddress)
	if err != nil {
		return nil, err
	}

	listener, err := tls.Listen("tcp", listenAddr, &tls.Config{
		MinVersion:   tls.VersionTLS12,
		Certificates: []tls.Certificate{cert},
		ClientAuth:   tls.RequestClientCert,
	})
	if err != nil {
		return nil, err
	}

	server := &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: 30 * time.Second,
	}
	go func() {
		if err := server.Serve(listener); err != nil && err != http.ErrServerClosed {
			log.Printf("kubelet API server exited: %v", err)
		}
	}()
	go func() {
		<-ctx.Done()
		_ = server.Close()
	}()
	return server, nil
}

func handleContainerLogs(prov *provider.NerscProvider) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/containerLogs/"), "/")
		if len(parts) != 3 || parts[0] == "" || parts[1] == "" || parts[2] == "" {
			http.NotFound(w, r)
			return
		}

		logs, err := prov.GetPodLogs(r.Context(), parts[0], parts[1], parts[2], nil)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		defer logs.Close()

		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		if _, err := io.Copy(w, logs); err != nil {
			log.Printf("Failed to write container logs for %s/%s/%s: %v", parts[0], parts[1], parts[2], err)
		}
	}
}

func selfSignedServingCert(nodeName, nodeAddress string) (tls.Certificate, error) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("generate kubelet serving key: %w", err)
	}

	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("generate kubelet serving cert serial: %w", err)
	}

	template := &x509.Certificate{
		SerialNumber: serial,
		Subject: pkix.Name{
			CommonName: nodeName,
		},
		NotBefore:             time.Now().Add(-time.Minute),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		DNSNames:              []string{nodeName},
	}
	if ip := net.ParseIP(nodeAddress); ip != nil {
		template.IPAddresses = append(template.IPAddresses, ip)
	} else if nodeAddress != "" {
		template.DNSNames = append(template.DNSNames, nodeAddress)
	}

	certDER, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("create kubelet serving cert: %w", err)
	}
	keyDER := x509.MarshalPKCS1PrivateKey(key)

	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: keyDER})
	cert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		return tls.Certificate{}, fmt.Errorf("load kubelet serving key pair: %w", err)
	}
	return cert, nil
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value != "" {
			return value
		}
	}
	return ""
}
