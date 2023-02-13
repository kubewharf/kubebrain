#!/bin/bash
# Copyright 2022 ByteDance and/or its affiliates
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


openssl ecparam -genkey -name prime256v1 -out ca.key

openssl req -new -x509 -days 10000 -key ca.key -out ca.crt -subj "/CN=KubeWharf"

openssl ecparam -genkey -name prime256v1 -out server.key

openssl req -new -key server.key -out server.csr -subj "/CN=KubeWharfServer"

# ! must set the md algorithm as a secure one except sha1, otherwise it may rise error if go version >= 1.17
openssl x509 -req -sha256 -in server.csr -CA ca.crt -CAkey ca.key \
-CAcreateserial -out server.crt -days 10000 \
-extensions v3_ext -extfile csr.conf