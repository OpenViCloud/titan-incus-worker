package restore

import "testing"

// A copy is finished only by the restore whose request it carries. An archive of a resource that was itself restored
// brings that restore's request with it, and a config without the key must not match a command without a request.
func TestIsFinishedBy_OnlyTheRequestOfTheRestoreItself_OnTheCopy_SaysFinished(t *testing.T) {
	for name, tc := range map[string]struct {
		config    map[string]string
		requestId string
		want      bool
	}{
		"its own request":                {map[string]string{"user.titan-restore": "r-1", "user.titan-name": "copy"}, "r-1", true},
		"another restore's request":      {map[string]string{"user.titan-restore": "r-0"}, "r-1", false},
		"no marker":                      {map[string]string{"user.titan-name": "copy"}, "r-1", false},
		"no config":                      {nil, "r-1", false},
		"no request and no marker":       {map[string]string{"user.titan-name": "copy"}, "", false},
		"no request and an empty marker": {map[string]string{"user.titan-restore": ""}, "", false},
	} {
		if got := IsFinishedBy(tc.config, tc.requestId); got != tc.want {
			t.Errorf("%s: finished = %v, want %v", name, got, tc.want)
		}
	}
}
