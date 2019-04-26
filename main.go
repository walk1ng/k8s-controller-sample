package main

import (
	"flag"
	"fmt"
	"time"

	"k8s.io/apimachinery/pkg/util/wait"

	"k8s.io/apimachinery/pkg/util/runtime"

	"k8s.io/apimachinery/pkg/fields"

	"k8s.io/api/core/v1"

	"k8s.io/client-go/tools/clientcmd"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
)

const maxRetries = 5

// Controller type
type Controller struct {
	clientset kubernetes.Interface
	queue     workqueue.RateLimitingInterface
	informer  cache.SharedIndexInformer
}

func newResourceController(client kubernetes.Interface, queue workqueue.RateLimitingInterface, informer cache.SharedIndexInformer) *Controller {

	informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			key, err := cache.MetaNamespaceKeyFunc(obj)
			if err == nil {
				queue.Add(key)
			}
		},
		UpdateFunc: func(old, new interface{}) {
			key, err := cache.MetaNamespaceKeyFunc(new)
			if err == nil {
				queue.Add(key)
			}
		},
		DeleteFunc: func(obj interface{}) {
			key, err := cache.DeletionHandlingMetaNamespaceKeyFunc(obj)
			if err == nil {
				queue.Add(key)
			}
		},
	})

	return &Controller{
		clientset: client,
		queue:     queue,
		informer:  informer,
	}
}

func (c *Controller) processNextItem() bool {
	key, quit := c.queue.Get()
	if quit {
		return false
	}

	defer c.queue.Done(key)

	err := c.processItem(key.(string))
	if err == nil {
		// no error
		c.queue.Forget(key)
	} else if c.queue.NumRequeues(key) < maxRetries {
		fmt.Printf("Error processing %s and will retry: %v\n", key.(string), err)
		c.queue.AddRateLimited(key)
	} else {
		// err != nil and reties exceed
		fmt.Printf("Error processing %s and give up: %v\n", key.(string), err)
		c.queue.Forget(key)
		runtime.HandleError(err)
	}
	return true
}

// the business logic
func (c *Controller) processItem(key string) error {
	obj, exists, err := c.informer.GetIndexer().GetByKey(key)
	if err != nil {
		return fmt.Errorf("Error fetching object with key %s from store: %v", key, err)
	}

	if !exists {
		fmt.Printf("Pod %s doesn't exist anymore\n", key)
	} else {
		fmt.Printf("Sync/Add/Update for Pod %s\n", obj.(*v1.Pod).GetName())
	}
	return nil
}

func (c *Controller) runWorker() {
	for c.processNextItem() {
		// continue looping
	}
}

// Run start the controller
func (c *Controller) Run(workerCount int, stopCh <-chan struct{}) {

	defer runtime.HandleCrash()
	defer c.queue.ShutDown()

	fmt.Println("Starting controller")

	go c.informer.Run(stopCh)

	if !cache.WaitForCacheSync(stopCh, c.hasSynced) {
		runtime.HandleError(fmt.Errorf("Timeout to waiting for caches to sync"))
		return
	}

	fmt.Println("Controller synced and ready")

	for i := 0; i < workerCount; i++ {
		go wait.Until(c.runWorker, time.Second, stopCh)
	}

	// run forever
	<-stopCh

	fmt.Println("Stopping controller")
}

func (c *Controller) hasSynced() bool {
	return c.informer.HasSynced()
}

func main() {

	var kubeconfig string
	flag.StringVar(&kubeconfig, "kubeconfig", "", "path to the kube config file")
	flag.Parse()

	// create the config
	config, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		fmt.Printf("ERROR: %v\n", err)
	}

	// create the clientset
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		fmt.Printf("ERROR: %v\n", err)
	}

	// create the queue
	queue := workqueue.NewRateLimitingQueue(workqueue.DefaultControllerRateLimiter())

	// create the listWatcher
	podListWatcher := cache.NewListWatchFromClient(clientset.CoreV1().RESTClient(), "pods", v1.NamespaceDefault, fields.Everything())
	// create the informer
	informer := cache.NewSharedIndexInformer(
		podListWatcher,
		&v1.Pod{},
		0,
		cache.Indexers{},
	)

	// create the controller
	c := newResourceController(clientset, queue, informer)

	stopCh := make(chan struct{})
	defer close(stopCh)

	// start the controller
	go c.Run(1, stopCh)

	// run forever
	select {}
}
