package state

import (
	"fmt"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
)

// Storer ---
type Storer interface {
	ConfigMaps() []*corev1.ConfigMap
	ConfigMapsWithListOptions(metav1.ListOptions) ([]*corev1.ConfigMap, error)

	Secrets() []*corev1.Secret

	Services() []*corev1.Service
	ServicesWithListOptions(metav1.ListOptions) ([]*corev1.Service, error)

	Pods() []*corev1.Pod
	PodsWithListOptions(metav1.ListOptions) ([]*corev1.Pod, error)

	GetConfigMap(key string) (*corev1.ConfigMap, error)
	GetSecret(key string) (*corev1.Secret, error)
	GetService(key string) (*corev1.Service, error)

	Run(stopCh <-chan struct{})
}

type stateHolder struct {
	namespace string

	informers *sharedInformers
	listers   *listers
}

func NewStateHolder(namespace string, listOptions metav1.ListOptions, resyncPeriod time.Duration, clientset kubernetes.Interface) Storer {
	store := &stateHolder{
		namespace: namespace,
		informers: &sharedInformers{},
		listers:   &listers{},
	}

	informerFactory := informers.NewSharedInformerFactoryWithOptions(clientset, resyncPeriod,
		informers.WithNamespace(namespace),
		informers.WithTweakListOptions(func(options *metav1.ListOptions) {
			options.LabelSelector = listOptions.LabelSelector
			options.FieldSelector = listOptions.FieldSelector
		}),
	)

	store.informers.ConfigMap = informerFactory.Core().V1().ConfigMaps().Informer()
	store.listers.ConfigMap.Store = store.informers.ConfigMap.GetStore()

	store.informers.Secret = informerFactory.Core().V1().Secrets().Informer()
	store.listers.Secret.Store = store.informers.Secret.GetStore()

	store.informers.Service = informerFactory.Core().V1().Services().Informer()
	store.listers.Service.Store = store.informers.Service.GetStore()

	store.informers.Pod = informerFactory.Core().V1().Pods().Informer()
	store.listers.Pod.Store = store.informers.Pod.GetStore()

	eventHandlers := cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
		},
		DeleteFunc: func(obj interface{}) {
		},
	}

	store.informers.ConfigMap.AddEventHandler(eventHandlers)
	store.informers.Secret.AddEventHandler(eventHandlers)
	store.informers.Service.AddEventHandler(eventHandlers)
	store.informers.Pod.AddEventHandler(eventHandlers)

	return store
}

func (sh stateHolder) ConfigMaps() []*corev1.ConfigMap {
	var cmaps []*corev1.ConfigMap
	for _, item := range sh.listers.ConfigMap.List() {
		if cmap, ok := item.(*corev1.ConfigMap); ok {
			cmaps = append(cmaps, cmap)
		}
	}

	return cmaps
}

func (sh stateHolder) Secrets() []*corev1.Secret {
	var secrets []*corev1.Secret
	for _, item := range sh.listers.Secret.List() {
		if secret, ok := item.(*corev1.Secret); ok {
			secrets = append(secrets, secret)
		}
	}

	return secrets
}

func (sh stateHolder) Services() []*corev1.Service {
	var services []*corev1.Service
	for _, item := range sh.listers.Service.List() {
		if service, ok := item.(*corev1.Service); ok {
			services = append(services, service)
		}
	}

	return services
}

func (sh stateHolder) ServicesWithListOptions(listOptions metav1.ListOptions) ([]*corev1.Service, error) {
	selector, _ := labels.Parse(listOptions.LabelSelector)
	filteredObjs, err := filterWithLabels(sh.listers.Service.List(), selector)
	if err != nil {
		return nil, err
	}

	var services []*corev1.Service
	for _, item := range filteredObjs {
		if service, ok := item.(*corev1.Service); ok {
			services = append(services, service)
		}
	}

	return services, nil
}

func (sh stateHolder) GetConfigMap(key string) (*corev1.ConfigMap, error) {
	return sh.listers.ConfigMap.ByKey(getObjectKey(key, sh.namespace))
}

func (sh stateHolder) ConfigMapsWithListOptions(listOptions metav1.ListOptions) ([]*corev1.ConfigMap, error) {
	selector, _ := labels.Parse(listOptions.LabelSelector)
	filteredObjs, err := filterWithLabels(sh.listers.ConfigMap.List(), selector)
	if err != nil {
		return nil, err
	}

	var cmaps []*corev1.ConfigMap
	for _, item := range filteredObjs {
		if cmap, ok := item.(*corev1.ConfigMap); ok {
			cmaps = append(cmaps, cmap)
		}
	}

	return cmaps, nil
}

func (sh stateHolder) GetSecret(key string) (*corev1.Secret, error) {
	return sh.listers.Secret.ByKey(getObjectKey(key, sh.namespace))
}

func (sh stateHolder) GetService(key string) (*corev1.Service, error) {
	return sh.listers.Service.ByKey(getObjectKey(key, sh.namespace))
}

func (sh stateHolder) Pods() []*corev1.Pod {
	var pods []*corev1.Pod
	for _, item := range sh.listers.Pod.List() {
		if pod, ok := item.(*corev1.Pod); ok {
			pods = append(pods, pod)
		}
	}

	return pods
}

func (sh stateHolder) PodsWithListOptions(listOptions metav1.ListOptions) ([]*corev1.Pod, error) {
	selector, _ := labels.Parse(listOptions.LabelSelector)
	filteredObjs, err := filterWithLabels(sh.listers.Pod.List(), selector)
	if err != nil {
		return nil, err
	}

	var pods []*corev1.Pod
	for _, item := range filteredObjs {
		if pod, ok := item.(*corev1.Pod); ok {
			pods = append(pods, pod)
		}
	}

	return pods, nil
}

func (sh stateHolder) Run(stopCh <-chan struct{}) {
	sh.informers.Run(stopCh)
}

func getObjectKey(input, defNs string) string {
	nsName := strings.Split(input, "/")
	if len(nsName) == 0 {
		return fmt.Sprintf("%v/%v", defNs, input)
	}

	return input
}

func filterWithLabels(objs []interface{}, labelSelector labels.Selector) ([]interface{}, error) {
	outItems := make([]interface{}, 0, len(objs))
	for _, obj := range objs {
		meta, err := apimeta.Accessor(obj)
		if err != nil {
			return nil, err
		}

		if labelSelector != nil {
			lbls := labels.Set(meta.GetLabels())
			if !labelSelector.Matches(lbls) {
				continue
			}
		}
		outItems = append(outItems, obj)
	}
	return outItems, nil
}
