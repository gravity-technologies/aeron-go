package cluster

import (
	"fmt"
)

type ClusterError struct {
	Message string
}

func (ce *ClusterError) Error() string {
	return fmt.Sprintf("cluster error: %s", ce.Message)
}

type ClusterTerminationError struct {
	Message string
}

func (cte *ClusterTerminationError) Error() string {
	return fmt.Sprintf("cluster termination error: %s", cte.Message)
}

type AgentTerminationError struct {
	Message string
}

func (ate *AgentTerminationError) Error() string {
	return fmt.Sprintf("agent termination error: %s", ate.Message)
}
