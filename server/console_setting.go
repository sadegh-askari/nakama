// Copyright 2025 The Nakama Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package server

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"slices"
	"strings"

	"go.uber.org/zap"

	"github.com/heroiclabs/nakama/v3/console"
	"github.com/jackc/pgx/v5/pgtype"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	allowedSettings    = []string{"utc_toggle", "game_purchase_correction_message", "game_resource_grant_message"}
	ErrSettingNotFound = status.Error(codes.NotFound, "Setting not found.")
	ErrSettingInvalid  = status.Error(codes.InvalidArgument, "Setting is invalid.")
)

func (s *ConsoleServer) GetSetting(ctx context.Context, in *console.SettingRequest) (*console.Setting, error) {
	if in == nil || in.Name == "" {
		return nil, ErrSettingInvalid
	}

	query := `SELECT name, value, update_time FROM setting WHERE name = $1`

	var name, value string
	var updateTime pgtype.Timestamptz

	if err := s.db.QueryRowContext(ctx, query, in.Name).Scan(&name, &value, &updateTime); err != nil {
		if errors.Is(err, context.Canceled) {
			return nil, status.Error(codes.Canceled, "Request was canceled.")
		}

		if errors.Is(err, sql.ErrNoRows) {
			if value, ok := gameNotificationSettingDefault(in.Name); ok {
				return &console.Setting{Name: in.Name, Value: value}, nil
			}
			return nil, ErrSettingNotFound
		}

		return nil, status.Error(codes.Internal, "An error occurred while trying to get the setting.")
	}

	setting := &console.Setting{
		Name:          name,
		Value:         value,
		UpdateTimeSec: updateTime.Time.Unix(),
	}

	return setting, nil
}

func (s *ConsoleServer) UpdateSetting(ctx context.Context, in *console.UpdateSettingRequest) (*console.Setting, error) {
	if !slices.Contains(allowedSettings, in.Name) {
		return nil, ErrSettingInvalid
	}

	if _, ok := gameNotificationSettingDefault(in.Name); ok {
		var text string
		if json.Unmarshal([]byte(in.Value), &text) != nil || strings.TrimSpace(text) == "" || len(text) > 8000 {
			return nil, ErrSettingInvalid
		}
	}

	var updateTime pgtype.Timestamptz

	query := "UPDATE setting SET value = $2, update_time = now() WHERE name = $1 RETURNING update_time"
	if _, ok := gameNotificationSettingDefault(in.Name); ok {
		query = "INSERT INTO setting (name,value) VALUES ($1,$2::jsonb) ON CONFLICT (name) DO UPDATE SET value=excluded.value,update_time=now() RETURNING update_time"
	}
	params := []any{in.Name, in.Value}

	if err := s.db.QueryRowContext(ctx, query, params...).Scan(&updateTime); err != nil {
		if errors.Is(err, context.Canceled) {
			return nil, status.Error(codes.Canceled, "Request was canceled.")
		}
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrSettingNotFound
		}
		return nil, status.Error(codes.Internal, "An error occurred while trying to update the setting.")
	}

	res := &console.Setting{
		Name:          in.Name,
		Value:         in.Value,
		UpdateTimeSec: updateTime.Time.Unix(),
	}
	return res, nil
}

func (s *ConsoleServer) ListSettings(ctx context.Context, in *console.ListSettingsRequest) (*console.SettingList, error) {
	query := `SELECT name, value, update_time FROM setting`
	params := []any{}

	if len(in.Names) > 0 {
		query += " WHERE name = ANY($1::text[])"

		params = append(params, in.Names)
	}

	rows, err := s.db.QueryContext(ctx, query, params...)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			return nil, status.Error(codes.Canceled, "Request was canceled.")
		}
		s.logger.Error("Error listing settings.", zap.Error(err))
		return nil, status.Error(codes.Internal, "An error occurred while trying to list settings.")
	}
	defer rows.Close()

	settings := make([]*console.Setting, 0, 1)

	for rows.Next() {
		var name, value string

		var updateTime pgtype.Timestamptz
		if err := rows.Scan(&name, &value, &updateTime); err != nil {
			return nil, status.Error(codes.Internal, "An error occurred while trying to read settings.")
		}

		settings = append(settings, &console.Setting{Name: name, Value: value, UpdateTimeSec: updateTime.Time.Unix()})
	}

	if err := rows.Err(); err != nil {
		if errors.Is(err, context.Canceled) {
			return nil, status.Error(codes.Canceled, "Request was canceled.")
		}

		return nil, status.Error(codes.Internal, "An error occurred while trying to read settings.")
	}

	for _, name := range []string{"game_purchase_correction_message", "game_resource_grant_message"} {
		if len(in.Names) > 0 && !slices.Contains(in.Names, name) {
			continue
		}
		if !slices.ContainsFunc(settings, func(setting *console.Setting) bool { return setting.Name == name }) {
			value, _ := gameNotificationSettingDefault(name)
			settings = append(settings, &console.Setting{Name: name, Value: value})
		}
	}
	return &console.SettingList{Settings: settings}, nil
}

func gameNotificationSettingDefault(name string) (string, bool) {
	var message string
	switch name {
	case "game_purchase_correction_message":
		message = "A purchase correction has been issued to you."
	case "game_resource_grant_message":
		message = "Resources have been added to your account."
	default:
		return "", false
	}
	value, _ := json.Marshal(message)
	return string(value), true
}
