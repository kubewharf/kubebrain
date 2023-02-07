// Copyright 2023 ByteDance and/or its affiliates
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package leader

// Stub is an implement of LeaderElection for test
type Stub struct {
	ElectionInfo
}

// Campaign implements LeaderElection interface
func (s *Stub) Campaign() {
}

// GetLeaderInfo implements LeaderElection interface
func (s *Stub) GetLeaderInfo() string {
	return s.ElectionInfo.LeaderAddress
}

// IsLeader implements LeaderElection interface
func (s *Stub) IsLeader() bool {
	return s.ElectionInfo.IsLeader
}

// GetElectionInfo implements LeaderElection interface
func (s *Stub) GetElectionInfo() (ElectionInfo, error) {
	return s.ElectionInfo, nil
}
