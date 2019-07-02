package datafmts

import (
  "encoding/json"
)

var marshalErr = map[string]interface{}{
  "error": "failed to marshal query runner state",
}

func (r *QueryRunnerState) Loggable() map[string]interface{} {
  // TODO: fix this nasty hack...
  s, err := json.Marshal(r)
  if err != nil {
    return marshalErr
  }
  var m map[string]interface{}
  err = json.Unmarshal(s, &m)
  if err != nil {
    return marshalErr
  }
  return m
}
