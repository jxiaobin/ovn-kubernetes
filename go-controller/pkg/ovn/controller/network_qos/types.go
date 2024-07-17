package networkqos

// networkQoSState is the cache that keeps the state of a single
// network qos in the cluster with namespace+name being unique
type networkQoSState struct {
	// name of the network qos (prefixed with namespace)
	name string

	// TODO: add other fields here...
}
