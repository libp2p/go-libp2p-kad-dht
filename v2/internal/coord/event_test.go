package coord

var _ NetworkCommand = (*EventOutboundGetCloserNodes)(nil)

var (
	_ RoutingCommand = (*EventAddNode)(nil)
	_ RoutingCommand = (*EventStartBootstrap)(nil)
)

var (
	_ QueryCommand = (*EventStartMessageQuery)(nil)
	_ QueryCommand = (*EventStartFindCloserQuery)(nil)
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
