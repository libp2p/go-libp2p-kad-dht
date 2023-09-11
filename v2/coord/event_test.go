package coord

var _ NetworkCommand = (*EventOutboundGetCloserNodes)(nil)

var (
	_ RoutingCommand = (*EventAddNode)(nil)
	_ RoutingCommand = (*EventStartBootstrap)(nil)
)

var (
	_ QueryCommand = (*EventStartQuery)(nil)
	_ QueryCommand = (*EventStopQuery)(nil)
)

var (
	_ RoutingNotification = (*EventRoutingUpdated)(nil)
	_ RoutingNotification = (*EventBootstrapFinished)(nil)
)

var _ NodeHandlerRequest = (*EventOutboundGetCloserNodes)(nil)

var (
	_ NodeHandlerResponse = (*EventGetCloserNodesSuccess)(nil)
	_ NodeHandlerResponse = (*EventGetCloserNodesFailure)(nil)
)
