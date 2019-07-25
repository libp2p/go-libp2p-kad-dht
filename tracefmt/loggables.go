package datafmts

import (
  "encoding/json"
)

var marshalErr = map[string]interface{}{
  "error": "failed to marshal query runner state",
}

func (r *QueryRunnerState) Loggable() map[string]interface{} {
  m1 := map[string]interface{}{
    "QueryRunner": r,
  }

  // TODO: fix this nasty hack...
  s, err := json.Marshal(m1)
  if err != nil {
    return marshalErr
  }
  var m2 map[string]interface{}
  err = json.Unmarshal(s, &m2)
  if err != nil {
    return marshalErr
  }
  return m2
}
