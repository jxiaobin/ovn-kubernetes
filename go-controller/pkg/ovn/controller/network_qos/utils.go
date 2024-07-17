package networkqos

func joinMetaNamespaceAndName(namespace, name string) string {
	if namespace == "" {
		return name
	}
	return namespace + "/" + name
}
