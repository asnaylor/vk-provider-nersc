package main

import (
	"context"
	"log"
	"os"

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
