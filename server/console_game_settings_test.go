package server

import (
	"context"
	"encoding/json"
	"github.com/heroiclabs/nakama/v3/console"
	"strings"
	"testing"
)

func TestGameNotificationSettingDefaultsAndValidation(t *testing.T) {
	for _, name := range []string{"game_purchase_correction_message", "game_resource_grant_message"} {
		value, ok := gameNotificationSettingDefault(name)
		var decoded string
		if !ok || json.Unmarshal([]byte(value), &decoded) != nil || decoded == "" {
			t.Fatal("missing JSON string default", name, value)
		}
		for _, value := range []string{`""`, `"   "`, `false`, `{}`, `invalid`, string(mustJSON(strings.Repeat("a", 8001)))} {
			if _, err := (&ConsoleServer{}).UpdateSetting(context.Background(), &console.UpdateSettingRequest{Name: name, Value: value}); err != ErrSettingInvalid {
				t.Fatal("invalid setting reached database", name, value, err)
			}
		}
	}
	if _, ok := gameNotificationSettingDefault("utc_toggle"); ok {
		t.Fatal("native setting changed")
	}
	if _, err := (&ConsoleServer{}).UpdateSetting(context.Background(), &console.UpdateSettingRequest{Name: "unknown", Value: `"a"`}); err != ErrSettingInvalid {
		t.Fatal("unknown setting accepted")
	}
}
func mustJSON(value string) []byte { encoded, _ := json.Marshal(value); return encoded }
