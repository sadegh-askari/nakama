// Copyright 2020 The Nakama Authors
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
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/gofrs/uuid"
	"github.com/heroiclabs/nakama-common/api"
	"github.com/heroiclabs/nakama-common/runtime"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func processMultiUpdateInput(accountUpdates []*runtime.AccountUpdate, storageWrites []*runtime.StorageWrite, walletUpdates []*runtime.WalletUpdate) ([]*accountUpdate, StorageOpWrites, []*walletUpdate, error) {
	// Process account update inputs.
	accountUpdateOps := make([]*accountUpdate, 0, len(accountUpdates))
	for _, update := range accountUpdates {
		u, err := uuid.FromString(update.UserID)
		if err != nil {
			return nil, nil, nil, errors.New("expects user ID to be a valid identifier")
		}

		var metadataWrapper *wrapperspb.StringValue
		if update.Metadata != nil {
			metadataBytes, err := json.Marshal(update.Metadata)
			if err != nil {
				return nil, nil, nil, fmt.Errorf("error encoding metadata: %v", err.Error())
			}
			metadataWrapper = &wrapperspb.StringValue{Value: string(metadataBytes)}
		}

		var displayNameWrapper *wrapperspb.StringValue
		if update.DisplayName != "" {
			displayNameWrapper = &wrapperspb.StringValue{Value: update.DisplayName}
		}
		var timezoneWrapper *wrapperspb.StringValue
		if update.Timezone != "" {
			timezoneWrapper = &wrapperspb.StringValue{Value: update.Timezone}
		}
		var locationWrapper *wrapperspb.StringValue
		if update.Location != "" {
			locationWrapper = &wrapperspb.StringValue{Value: update.Location}
		}
		var langWrapper *wrapperspb.StringValue
		if update.LangTag != "" {
			langWrapper = &wrapperspb.StringValue{Value: update.LangTag}
		}
		var avatarWrapper *wrapperspb.StringValue
		if update.AvatarUrl != "" {
			avatarWrapper = &wrapperspb.StringValue{Value: update.AvatarUrl}
		}

		accountUpdateOps = append(accountUpdateOps, &accountUpdate{
			userID:      u,
			username:    update.Username,
			displayName: displayNameWrapper,
			timezone:    timezoneWrapper,
			location:    locationWrapper,
			langTag:     langWrapper,
			avatarURL:   avatarWrapper,
			metadata:    metadataWrapper,
		})
	}

	// Process storage write inputs.
	storageWriteOps := make(StorageOpWrites, 0, len(storageWrites))
	for _, write := range storageWrites {
		if write.Collection == "" {
			return nil, nil, nil, errors.New("expects collection to be a non-empty string")
		}
		if write.Key == "" {
			return nil, nil, nil, errors.New("expects key to be a non-empty string")
		}
		if write.UserID != "" {
			if _, err := uuid.FromString(write.UserID); err != nil {
				return nil, nil, nil, errors.New("expects an empty or valid user id")
			}
		}
		if maybeJSON := []byte(write.Value); !json.Valid(maybeJSON) || bytes.TrimSpace(maybeJSON)[0] != byteBracket {
			return nil, nil, nil, errors.New("value must be a JSON-encoded object")
		}

		op := &StorageOpWrite{
			Object: &api.WriteStorageObject{
				Collection:      write.Collection,
				Key:             write.Key,
				Value:           write.Value,
				Version:         write.Version,
				PermissionRead:  &wrapperspb.Int32Value{Value: int32(write.PermissionRead)},
				PermissionWrite: &wrapperspb.Int32Value{Value: int32(write.PermissionWrite)},
			},
		}
		if write.UserID == "" {
			op.OwnerID = uuid.Nil.String()
		} else {
			op.OwnerID = write.UserID
		}

		storageWriteOps = append(storageWriteOps, op)
	}

	// Process wallet update inputs.
	walletUpdateOps := make([]*walletUpdate, len(walletUpdates))
	for i, update := range walletUpdates {
		uid, err := uuid.FromString(update.UserID)
		if err != nil {
			return nil, nil, nil, errors.New("expects a valid user id")
		}

		metadataBytes := []byte("{}")
		if update.Metadata != nil {
			metadataBytes, err = json.Marshal(update.Metadata)
			if err != nil {
				return nil, nil, nil, fmt.Errorf("failed to convert metadata: %s", err.Error())
			}
		}

		walletUpdateOps[i] = &walletUpdate{
			UserID:    uid,
			Changeset: update.Changeset,
			Metadata:  string(metadataBytes),
		}
	}
	return accountUpdateOps, storageWriteOps, walletUpdateOps, nil
}

func MultiUpdate(ctx context.Context, logger *zap.Logger, db *sql.DB, accountUpdates []*accountUpdate, storageWrites StorageOpWrites, walletUpdates []*walletUpdate, updateLedger bool) ([]*api.StorageObjectAck, []*runtime.WalletUpdateResult, error) {
	if len(accountUpdates) == 0 && len(storageWrites) == 0 && len(walletUpdates) == 0 {
		return nil, nil, nil
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		logger.Error("Could not begin database transaction.", zap.Error(err))
		return nil, nil, err
	}

	var storageWriteAcks []*api.StorageObjectAck
	var walletUpdateResults []*runtime.WalletUpdateResult

	if err = ExecuteInTx(ctx, tx, func() error {
		storageWriteAcks = nil
		walletUpdateResults = nil

		// Execute any account updates.
		updateErr := updateAccounts(ctx, logger, tx, accountUpdates)
		if updateErr != nil {
			return updateErr
		}

		// Execute any storage updates.
		storageWriteAcks, updateErr = storageWriteObjects(ctx, logger, tx, true, storageWrites)
		if updateErr != nil {
			return updateErr
		}

		// Execute any wallet updates.
		walletUpdateResults, updateErr = updateWallets(ctx, logger, tx, walletUpdates, updateLedger)
		if updateErr != nil {
			return updateErr
		}

		return nil
	}); err != nil {
		if e, ok := err.(*statusError); ok {
			return nil, walletUpdateResults, e.Cause()
		}
		logger.Error("Error running multi update.", zap.Error(err))
		return nil, walletUpdateResults, err
	}

	return storageWriteAcks, walletUpdateResults, nil
}

func MultiUpdateTx(ctx context.Context, logger *zap.Logger, tx *sql.Tx, accountUpdates []*accountUpdate, storageWrites StorageOpWrites, walletUpdates []*walletUpdate, updateLedger bool) ([]*api.StorageObjectAck, []*runtime.WalletUpdateResult, error) {
	if len(accountUpdates) == 0 && len(storageWrites) == 0 && len(walletUpdates) == 0 {
		return nil, nil, nil
	}

	var storageWriteAcks []*api.StorageObjectAck
	var walletUpdateResults []*runtime.WalletUpdateResult

	// Execute any account updates.
	updateErr := updateAccounts(ctx, logger, tx, accountUpdates)
	if updateErr != nil {
		return nil, nil, updateErr
	}

	// Execute any storage updates.
	storageWriteAcks, updateErr = storageWriteObjects(ctx, logger, tx, true, storageWrites)
	if updateErr != nil {
		return nil, nil, updateErr
	}

	// Execute any wallet updates.
	walletUpdateResults, updateErr = updateWallets(ctx, logger, tx, walletUpdates, updateLedger)
	if updateErr != nil {
		return nil, nil, updateErr
	}

	return storageWriteAcks, walletUpdateResults, nil
}
