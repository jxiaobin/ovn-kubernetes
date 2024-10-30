/*


Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
// Code generated by lister-gen. DO NOT EDIT.

package v1

import (
	v1 "github.com/ovn-org/ovn-kubernetes/go-controller/pkg/crd/networkqos/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/listers"
	"k8s.io/client-go/tools/cache"
)

// NetworkQoSLister helps list NetworkQoSes.
// All objects returned here must be treated as read-only.
type NetworkQoSLister interface {
	// List lists all NetworkQoSes in the indexer.
	// Objects returned here must be treated as read-only.
	List(selector labels.Selector) (ret []*v1.NetworkQoS, err error)
	// NetworkQoSes returns an object that can list and get NetworkQoSes.
	NetworkQoSes(namespace string) NetworkQoSNamespaceLister
	NetworkQoSListerExpansion
}

// networkQoSLister implements the NetworkQoSLister interface.
type networkQoSLister struct {
	listers.ResourceIndexer[*v1.NetworkQoS]
}

// NewNetworkQoSLister returns a new NetworkQoSLister.
func NewNetworkQoSLister(indexer cache.Indexer) NetworkQoSLister {
	return &networkQoSLister{listers.New[*v1.NetworkQoS](indexer, v1.Resource("networkqos"))}
}

// NetworkQoSes returns an object that can list and get NetworkQoSes.
func (s *networkQoSLister) NetworkQoSes(namespace string) NetworkQoSNamespaceLister {
	return networkQoSNamespaceLister{listers.NewNamespaced[*v1.NetworkQoS](s.ResourceIndexer, namespace)}
}

// NetworkQoSNamespaceLister helps list and get NetworkQoSes.
// All objects returned here must be treated as read-only.
type NetworkQoSNamespaceLister interface {
	// List lists all NetworkQoSes in the indexer for a given namespace.
	// Objects returned here must be treated as read-only.
	List(selector labels.Selector) (ret []*v1.NetworkQoS, err error)
	// Get retrieves the NetworkQoS from the indexer for a given namespace and name.
	// Objects returned here must be treated as read-only.
	Get(name string) (*v1.NetworkQoS, error)
	NetworkQoSNamespaceListerExpansion
}

// networkQoSNamespaceLister implements the NetworkQoSNamespaceLister
// interface.
type networkQoSNamespaceLister struct {
	listers.ResourceIndexer[*v1.NetworkQoS]
}